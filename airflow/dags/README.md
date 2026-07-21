# airflow/dags — 每日 ETL 排程定義

`tdcs_daily_fetch_dag.py` 是整條資料管線的排程骨架，串接四個環環相扣的任務，
`@daily` 觸發、`max_active_runs=1` 保證不並發：

```
fetch_csv  →  put_to_hdfs  →  verify_and_heal  →  convert_parquet
 抓當日CSV     上傳HDFS/raw      自我修復補漏         轉Parquet入湖
```

| Task | 對應程式 | 職責 |
|---|---|---|
| `fetch_csv` | `pair1_pipeline/02_daily_automation/tdcs_daily_fetch.py` | 只抓「今天 - 5 天」的單日 CSV 到本機 staging |
| `put_to_hdfs` | `pair1_pipeline/03_shared/put_to_hdfs.sh` | 掃 staging 有哪些 `year=/month=` 分區，逐一上傳 |
| `verify_and_heal` | `tdcs_backfill_missing.py --auto` | 以 HDFS 現況為準，檔級比對近期窗口、補回缺檔 |
| `convert_parquet` | [`pair2_convert/convert_batch.py`](../../pair2_convert/) | raw CSV → 分區 Parquet，冪等自我修復 |

## 設計重點

- **執行環境**：Airflow pod 本身沒有 HDFS/Spark client，各 task 用
  `kubectl exec` 動態找到管理節點 pod（不寫死 pod 名，因為帶 hash 後綴
  的 pod 名會隨重啟改變），再在裡面執行實際指令。
- **為什麼補漏可以放進同一條 DAG**：補漏原本因為怕跟主抓取流程「同時」
  對資料來源發請求（避免踩到來源網站的節流規則）而刻意手動執行。
  現在補漏是同一條 DAG 的下游 task，`max_active_runs=1` 保證整條 DAG
  不會並發，自然不會同時發請求，因此可以安全排程化。
- **窗口設計**：每天只抓「今天 - 5 天」單日（對齊來源資料的處理延遲），
  但補漏與轉檔會回掃一個更大的窗口（D-5 ~ D-24），讓中間任何一天的
  暫時性失敗都能在後續幾天內自動收斂，不需要人工重跑。
- **重試策略**：整支 DAG 失敗自動重試一次（`retry_delay=10min`），
  對應常見的網路抖動；各 task 本身冪等，重試不會造成重複資料。
