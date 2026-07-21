"""
tdcs_daily_fetch DAG
====================
職責範圍：每日驅動 dtadm 上的四段式流程 —
  (1) tdcs_daily_fetch.py      只抓「未壓縮每日 CSV」到本機 staging（不 put）。
  (2) 03_shared/put_to_hdfs.sh 把本機 staging 的 csv 整批 put 上 HDFS /raw/…。
  (3) tdcs_backfill_missing.py --auto  自我修復：以 HDFS 為準檔級補漏，
      掃 D-5~D-24 窗口、精確補缺的檔（dtadm 重啟漏抓的日子自動補回，
      不再依賴人工）。單次補檔有上限，巨大缺口靠窗口滾動幾天內收斂。
  (4) pair2/convert_batch.py --auto   把 raw CSV 轉成 /dataset/ 的 Parquet
      （M06A、M03A 各一次 spark-submit）。同樣掃 D-5~D-24，只轉「raw 已完整、
      dataset 還沒轉」的日子，冪等自我修復——(3) 補回來的舊日子這裡會一起補轉。
每天抓「今天 - 5 天（D-5）」單日的 M06A + M03A。窗口外的深層歷史缺口
仍用手動 tdcs_backfill_missing.py（不帶 --auto，預設掃 30 天）、
手動 convert_batch.py --start/--end 補轉。

為什麼補漏可以放進 DAG（原本刻意手動的理由已被結構消解）：
  補漏會問高公局來源，原本怕跟主 DAG 的 fetch「同時」跑搶流量、壞了 >40s
  規則。現在補漏是同一條 DAG 的下游 task —— fetch 抓完才輪到它，且
  max_active_runs=1 保證整條 DAG 不並發 → 永遠不會同時對來源發請求。

輸入來源：高公局 TISVCloud（由 fetch.py 解析 autoindex 抓取）。
輸出位置：先落本機 staging（fetch），再落 HDFS /raw/<type>/year=/month=（put）。
銜接對象：
  - 呼叫 02_daily_automation/tdcs_daily_fetch.py（fetch 段）
  - 呼叫 03_shared/put_to_hdfs.sh（put 段，逐 year/month 分區呼叫）

執行模式（關鍵）：
  Airflow pod 本身沒有 hdfs client,不能直接跑落地。
  兩個 task 都用 kubectl exec 鑽進 dtadm,在 dtadm 裡跑（python3 / bash）。

兩個踩過的坑（已解,寫在 wrap_remote 裡）：
  1. pod 名帶 hash 後綴會隨重啟改變 → 每次執行動態抓 dtadm-<hash>,不寫死。
     （用 pod 前綴比對,不需要 get deployments 權限,避開 airflow-sa 的 RBAC 限制。）
  2. kubectl exec 不經過 PAM,不會讀 /etc/environment → 進去後 hdfs 不在 PATH、
     JAVA_HOME/HADOOP_CONF_DIR 全空。故先 `set -a; . /etc/environment; set +a`
     把整份環境 export 給子行程,hdfs 才能用且連得到 NameNode。
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ============================================================
# CONFIG（會變的東西集中在此;不寫死）
# ============================================================
DTADM_NS     = "dt"                                          # dtadm 所在 namespace
FETCH_SCRIPT = "/home/bigred/wulin/automation/tdcs_daily_fetch.py"  # (對應 02_daily_automation/tdcs_daily_fetch.py)
PUT_SCRIPT   = "/home/bigred/wulin/shared/put_to_hdfs.sh"           # (對應 03_shared/put_to_hdfs.sh)
HEAL_SCRIPT  = "/home/bigred/wulin/automation/tdcs_backfill_missing.py"  # (對應 02_daily_automation/tdcs_backfill_missing.py)
CONVERT_SCRIPT = "/home/bigred/wulin/automation/convert_batch.py"       # (對應 pair2/convert_batch.py)
STAGING_ROOT = "/home/bigred/wulin/staging/extract"         # fetch 落地 / put 讀取的本機 staging 根
OFFSET_DAYS  = 5                                             # D-5:回掃窗口最新端 = 今天 - N 天

# Spark 提交參數（在 dtadm 內 spark-submit，master=yarn、client 模式）
SPARK_SUBMIT = (
    "spark-submit --master yarn --deploy-mode client "
)
# ============================================================


def wrap_remote(remote_cmd: str) -> str:
    """
    把「要在 dtadm 內跑的指令」包成 Airflow pod 端可執行的 bash：
      動態抓 dtadm pod 名 → kubectl exec 進去 → 載入 hadoop 環境 → 跑 remote_cmd。
    兩個 task（fetch / put）共用同一套包裝,只換 remote_cmd。
    """
    inner = (
        "export HADOOP_USER_NAME=bigred; "      # 讓 Airflow(root)以 bigred 身分操作 HDFS
        "set -a; . /etc/environment; set +a; "  # 補 exec 不經 PAM 的環境缺口
        f"{remote_cmd}"
    )
    return (
        "set -euo pipefail; "
        f"POD=$(kubectl get pod -n {DTADM_NS} -o name | grep '/dtadm-' | head -1 | cut -d/ -f2); "
        'test -n "$POD" || { echo "找不到 dtadm pod"; exit 1; }; '
        'echo "使用 pod: $POD"; '
        f"kubectl exec -n {DTADM_NS} \"$POD\" -- bash -c '{inner}'"
    )


# --- (1) fetch：只抓「今天 - OFFSET_DAYS」單日 CSV 到本機 staging ---
#   （補漏不在這裡：由 task (3) verify_and_heal 以 HDFS 為準檔級補。）
FETCH_REMOTE = f"python3 -u {FETCH_SCRIPT} --offset-days {OFFSET_DAYS}"

# --- (2) put：掃本機 staging 有哪些 year/month 分區,逐一呼叫 put_to_hdfs.sh 上傳 ---
#   find 找出 STAGING_ROOT/<type>/year=YYYY/month=MM 目錄,從路徑解出 type/year/month。
PUT_REMOTE = (
    f'find {STAGING_ROOT} -mindepth 3 -maxdepth 3 -type d -name "month=*" | while read p; do '
    '  typ=$(echo "$p" | awk -F/ "{print \\$(NF-2)}"); '
    '  yr=$(echo "$p"  | sed -n "s/.*year=\\([0-9]\\{4\\}\\).*/\\1/p"); '
    '  mo=$(echo "$p"  | sed -n "s/.*month=\\([0-9]\\{2\\}\\).*/\\1/p"); '
    f'  echo "put $typ $yr $mo"; bash {PUT_SCRIPT} "$typ" "$yr" "$mo"; '
    'done'
)

# --- (3) heal：以 HDFS 為準的自我修復（檔級補漏,掃 D-5~D-24）---
#   快照 HDFS 檔名 → 差集算精確缺檔 → 只直抓缺的檔 → 增量 put。
#   來源本身缺檔不算失敗（exit 0）;下載/put 失敗才讓 task 紅、觸發 retry。
HEAL_REMOTE = f"python3 -u {HEAL_SCRIPT} --auto"

# --- (4) convert：raw CSV → /dataset/ Parquet（M06A、M03A 各一次 spark-submit）---
#   convert_batch.py --auto 掃 D-5~D-24,只轉「raw 已完整、dataset 還沒轉」的日子。
#   兩個 dtype 以 && 串接:前者失敗即中止並讓 task 紅(retry 會重跑,冪等安全)。
CONVERT_REMOTE = (
    f"{SPARK_SUBMIT} {CONVERT_SCRIPT} M06A --auto && "
    f"{SPARK_SUBMIT} {CONVERT_SCRIPT} M03A --auto"
)

default_args = {
    "retries": 1,                          # 整支掛掉時自動重試一次(網路抖動常見)
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="tdcs_daily_fetch",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",          # 每天 00:00(UTC)觸發 → Scheduler 看這個
    catchup=False,              # 不補跑歷史;每次只跑當下的「今天 - 5 天」
    max_active_runs=1,          # 同時只准一個 run → 避免並發寫同分區撞 LeaseExpiredException
    default_args=default_args,
    tags=["tdcs", "pair1"],
) as dag:

    # 先抓（fetch.py → 本機 staging）
    fetch_csv = BashOperator(
        task_id="fetch_csv",
        bash_command=wrap_remote(FETCH_REMOTE),
    )

    # 再上傳（put_to_hdfs.sh 逐分區 → HDFS /raw）
    put_to_hdfs = BashOperator(
        task_id="put_to_hdfs",
        bash_command=wrap_remote(PUT_REMOTE),
    )

    # 自我修復（以 HDFS 為準,精確補窗口內缺的檔;不並發、不搶流量）
    verify_and_heal = BashOperator(
        task_id="verify_and_heal",
        bash_command=wrap_remote(HEAL_REMOTE),
    )

    # 轉 Parquet（raw → /dataset/;冪等,只轉 raw 已完整而 dataset 還沒有的日子）
    convert_parquet = BashOperator(
        task_id="convert_parquet",
        bash_command=wrap_remote(CONVERT_REMOTE),
    )

    fetch_csv >> put_to_hdfs >> verify_and_heal >> convert_parquet
