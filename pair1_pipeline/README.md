# pair1_pipeline — 資料抓取與回填管線

負責把政府開放資料的 TDCS CSV（M03A 車流量 / M06A 旅次明細）從來源網站
落地到 HDFS 的 `/raw/`，是整條資料平台的最上游。分成「歷史回填」與
「每日自動化」兩條流程，共用同一套上傳工具。

```
01_historical_backfill/   一次性歷史回填：下載 tar.gz → 解壓攤平 → 交給共用 put
02_daily_automation/      每日排程：抓當日 CSV → 自我修復補漏 → Airflow DAG 定義
03_shared/                兩條流程共用：本機 staging → HDFS /raw 的上傳邏輯
config/                   集中設定（路徑、日期範圍、節流間隔），換環境只改這裡
docs/                     操作手冊 / 設計紀錄
```

## 兩條流程怎麼分工

| | 歷史回填 (01) | 每日自動化 (02) |
|---|---|---|
| 觸發方式 | 手動執行 | Airflow DAG，`@daily` |
| 來源格式 | 整月壓縮的 `tar.gz` | 未壓縮的單日 CSV |
| 用途 | 補齊上線前的歷史資料 | 每天抓「今天 - 5 天」，並自我修復近期缺檔 |
| 落地路徑 | 本機 staging（與 02 共用同一套路徑規則）|

兩條流程的本機 staging 輸出路徑一致（`staging/extract/<type>/year=/month=/`），
所以 `03_shared/put_to_hdfs.sh` 可以通吃兩邊產出，不用重複寫上傳邏輯。

## 設計重點

- **冪等**：`put_to_hdfs.sh` 以「HDFS 現有檔數 vs 應有檔數」判斷完整度，
  不足才覆蓋重傳，重跑安全。
- **自我修復**：`tdcs_backfill_missing.py`（02）以 HDFS 現況為準，逐檔比對
  來源目錄，只補真正缺的檔案，不用人工介入。
- **設定與程式分離**：`config/` 集中管理路徑、日期窗口等易變參數，
  部署到新環境時不用改程式本體。

下游接續：`03_shared/put_to_hdfs.sh` 寫入的 `/raw/` 是
[`pair2_convert/`](../pair2_convert/) 的輸入來源。
