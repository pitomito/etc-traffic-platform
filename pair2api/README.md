# pair2api — 查詢 API 服務

FastAPI + PySpark 組成的查詢服務，直接對 HDFS 上的 Parquet 資料湖下查詢，
不經過中間資料庫；同時掛載靜態前端（`webapp/` 子目錄），對外提供
「網頁 + API」一體的服務。

## 核心檔案

- **`api_server_3.py`** — 服務主體，關鍵設計：
  - **門架新舊編號自動展開**：真實路網中門架會改編號（例如 2024-04-03
    `01F0155N` 改編為 `01F0153N`）。查詢時間若跨過切換日，只用單一編號
    過濾會漏抓另一時期的資料，導致車流量／旅次數被系統性低估。用
    union-find 從對照表建立「別名群組」，查詢時自動展開成該群組所有
    歷史編號一起查，並把結果歸戶回使用者查詢的編號，確保圖表是連續的
    一條序列。細節與驗證方式見 `CHANGES_2026-07-17_gantry_alias.md`。
  - **M06A 旅次比對用 regex、不用 UDF**：判斷一趟旅次是否「依序經過
    A 再到 B」，是在 JVM 端用 `rlike`/`regexp_extract` 對
    `TripInformation` 欄位比對，而不是寫 Python UDF——UDF 會把資料序列化
    丟進 Python worker，掃整月大 Parquet 時容易把 Spark JVM 記憶體吃爆。
  - **時區陷阱**：容器內部時區是 UTC，但資料裡的時間字串是台灣當地時間；
    刻意維持字串比對（而非轉成 timestamp 再比較），避開隱性時區偏移。
  - **併發控制**：Spark 查詢丟進 threadpool 執行、外加 Semaphore 限制
    同時執行的查詢數，避免多個重查詢同時把 driver heap 撐爆。
  - **分區結構相容**：M03A 資料湖正從日分區遷移到月分區，路徑產生邏輯
    同時嘗試兩種分區深度、交由「實際存在的路徑」決定查哪一種，遷移
    前後都能查，不需要配合切換時間點改版。
- **`Dockerfile`** / **`pair2api-deploy.yaml`** / **`pair2web-svc.yaml`** —
  容器化與 K8s 部署設定（Service、Deployment）。
- **`DEPLOY_GUIDE.md`** — 完整部署教學，說明整個系統的三層網路架構
  （實體主機／容器化 K8s 節點／Pod）與檔案如何跨層級同步。
- **`CHANGES_*.md`** — 各次重要修改的變更記錄：問題現象、根因、解法、
  驗證方式，取代「commit message 寫一行」的做法，讓每個決策可追溯。
- **`webapp/`** — 這裡掛載的是「教學／展示用的獨立站台」靜態前端副本
  （由本服務的 FastAPI 直接 mount 提供，與正式站的
  [`webapp/`](../webapp/)（Flask 代理版）是兩條並存的服務線，共用同一套
  查詢 API 後端，但各自持有一份前端靜態檔）。

## 與其他目錄的關係

```
pair2/convert_parquet/  →  HDFS /dataset（資料湖）
                                  │
                                  ▼
                         pair2api/api_server_3.py（本目錄，直查資料湖）
                                  │  /api/*
                    ┌─────────────┴─────────────┐
                    ▼                             ▼
        webapp/（Flask 代理 + 正式站前端）   pair2api/webapp/（教學站前端，FastAPI 直接掛載）
```
