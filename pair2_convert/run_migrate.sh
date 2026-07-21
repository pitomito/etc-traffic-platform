#!/bin/bash
# 用法：bash run_migrate.sh M03A [每輪月數，預設3]
#
# 每輪起一個「全新的」spark-submit，只跑 N 個月就讓整個 JVM 結束，再起下一輪。
# 這是本環境驗證過的模式（同 run_batch.sh 的每日一 JVM）：dtadm pod 有 cgroup
# pids 上限（thread 也算 pid），單一長壽 driver JVM 逐月累積 thread 會撞
# pthread_create EAGAIN；每輪換新 JVM 就徹底沒這個問題。
# 進度在 state file（migrate_<dtype>_state.jsonl），中斷／失敗後重跑本腳本即可接續。
set -euo pipefail

DTYPE=$1
CHUNK=${2:-2}
DIR="$(cd "$(dirname "$0")" && pwd)"

# state file 在執行目錄下（跟 migrate 腳本的預設一致）
STATE="migrate_$(echo "$DTYPE" | tr 'A-Z' 'a-z')_state.jsonl"

ok_count() { grep -c '"status": "OK"' "$STATE" 2>/dev/null || echo 0; }

ROUND=0
FAILS=0   # 連續「毫無進展」的失敗輪數；暫時性錯誤（pids 上限偶發）重試就會過
while true; do
  ROUND=$((ROUND + 1))
  OK_BEFORE=$(ok_count)
  echo "=== ${DTYPE} 第 ${ROUND} 輪（每輪 ${CHUNK} 個月）==="
  set +e
  spark-submit --master yarn --deploy-mode client \
    "$DIR/migrate_dataset_to_iceberg.py" "$DTYPE" --max-months "$CHUNK"
  rc=$?
  set -e
  case $rc in
    0) echo "=== ${DTYPE} 全部月份完成 ==="; exit 0 ;;
    3) FAILS=0; echo "--- 本輪完成，換新 JVM 續跑 ---"; sleep 3 ;;
    *)
      # 有進展（本輪至少完成一個月）就不算連續失敗
      if [ "$(ok_count)" -gt "$OK_BEFORE" ]; then FAILS=0; else FAILS=$((FAILS + 1)); fi
      if [ "$FAILS" -ge 3 ]; then
        echo "!!! 連續 ${FAILS} 輪無進展（exit $rc），停止。看上方錯誤，修正後重跑即可接續"
        exit "$rc"
      fi
      echo "--- 本輪失敗（exit $rc，多為暫時性資源錯誤），換新 JVM 重試（第 ${FAILS}/3 次）---"
      sleep 10 ;;
  esac
done
