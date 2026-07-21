# ETC 國道流量分析平台

> 台灣高速公路 ETC（電子收費）交通資料的端到端資料平台：從每日自動抓取政府開放資料、
> 清整入湖、到即時查詢 API 與互動式視覺化網頁。以 Spark on Kubernetes 承載
> 十億級交通明細，支援任意路段、任意時段的車流量與旅行時間查詢。

## 專案簡介

台灣交通部高速公路局每日發布 ETC 門架的交通資料集：

- **M03A**（車流量）：每個門架、每 5 分鐘、各車種的通過車輛數。
- **M06A**（旅次明細）：每一趟旅程的起訖門架、完整路徑與時間戳，可還原任意兩門架間的旅行時間。

原始資料以每日 CSV / tar.gz 發布、量級龐大（單月 M06A 近一億筆），
直接查詢既慢又難用。本專案解決的問題是把這些零散的原始檔，
**自動化地轉成可即時查詢的資料湖，並提供給非技術使用者的查詢介面**：

- 資料工程面：每日自動抓取 → 落地 HDFS → Spark 轉 Parquet 分區資料湖，全程冪等、可自我修復補漏。
- 查詢服務面：FastAPI + Spark 直查資料湖，前端只需選路段與時段即可得到車流量趨勢、
  旅行時間、路段排行與地圖路徑。
- 資料治理面：處理真實世界的髒資料問題——例如**門架改編號**造成的跨期查詢遺漏、
  歷史資料的 schema 演進、分區結構遷移等。

[![觀看示範影片](https://drive.google.com/file/d/18ekFHl13S3O2lyegig7SOtWneqMjiGIc/view?usp=drive_link)](https://drive.google.com/file/d/18ekFHl13S3O2lyegig7SOtWneqMjiGIc/view?usp=drive_link)

## 技術棧

| 層次 | 技術 |
|---|---|
| **資料處理** | Apache Spark 3.4 (PySpark)、HDFS、Hive 目錄式分區 Parquet |
| **排程 / 自動化** | Apache Airflow（DAG 每日觸發、`max_active_runs` 防並發）|
| **查詢 API** | FastAPI、Uvicorn、`run_in_threadpool` + Semaphore 併發閘門 |
| **前端代理 / 服務** | Flask + Gunicorn（靜態檔 + `/api/*` 反向代理）|
| **前端** | 原生 JavaScript（模組化）、Leaflet 地圖、Chart.js 圖表 |
| **基礎設施** | Kubernetes（多節點）、haproxy（對外入口）、容器化部署 |
| **語言** | Python 3、Bash、JavaScript |

## 功能特色

**資料管線（`pair1_pipeline/`、`airflow/dags/`）**
- 每日 Airflow DAG 串接四段式流程：抓取 CSV → 上傳 HDFS → 檔級補漏自我修復 → Spark 轉 Parquet。
- **冪等設計**：每個環節都可安全重跑；以「HDFS 現況」為準比對缺檔，斷點續跑不重工。
- 歷史回填工具（tar.gz 解壓攤平 → 批次入湖），與每日流程共用同一套上傳邏輯。

**ETL / 資料湖（`pair2_convert/`）**
- CSV → 年月（/日）分區 Parquet，**寫入採「暫存目錄 → 驗筆數 → 原子 rename 換入」**，
  確保線上查詢永遠讀不到寫一半的資料。
- 分區結構遷移工具（日分區 → 月分區）、schema 型別統一工具，皆含逐月筆數校驗與斷點續跑。
- 針對容器化 Spark 的資源限制（cgroup PID 上限）設計「每輪全新 JVM」的分批執行策略，
  避免長任務累積執行緒耗盡資源。

**查詢 API（`pair2_api/api_server_3.py`）**
- 直查 HDFS Parquet，無中間資料庫；以字串比對避開容器 UTC 與資料在地時間的時區陷阱。
- **門架新舊編號自動展開**：真實世界中門架會改編號，跨切換日的查詢若只用單一編號會漏抓、
  導致數據被低估。以 union-find 從對照表建立「別名群組」，查詢時自動展開新舊編號並將結果歸戶，
  確保同一實體路段的資料完整且連續（詳見 `pair2_api/CHANGES_2026-07-17_gantry_alias.md`）。
- M06A 旅次查詢用 regex 在 JVM 端比對 `TripInformation` 路徑序列（避開 Python UDF 的記憶體開銷），
  可查「經過 A 再到 B」的旅次並計算分段旅行時間。
- 效能與穩定性：單趟聚合取代多次全量掃描、執行緒池 + 併發閘門避免 Spark heap 爆掉。

**前端（`pair3_webapp/`）**
- 車流量查詢、旅行時間查詢、統計儀表板三頁；模組化原生 JS。
- Leaflet 地圖標示查詢路段的實際門架路徑、Chart.js 呈現趨勢與車種佔比。

## 架構總覽

```
 政府開放資料 (每日 CSV / tar.gz)
        │  Airflow DAG（每日、冪等、自我修復）
        ▼
   HDFS /raw  ──Spark 轉檔──▶  HDFS /dataset（年月分區 Parquet 資料湖）
                                        │
              ┌─────────────────────────┘
              ▼
   FastAPI + Spark（pair2_api）  ◀── 直查資料湖，無中間 DB
              │  /api/*
              ▼
   Flask 代理 + 靜態前端（pair3_webapp）  ◀── 使用者：選路段、選時段
              │
              ▼
        瀏覽器（Leaflet 地圖 + Chart.js 圖表）
```

## 安裝與執行

> 本 repo 為作品集用途，已將所有叢集位址、主機名、憑證抽換為佔位值或環境變數。
> 實際部署需自備 HDFS / Spark / Kubernetes 環境。以下為本機試跑前端與 API 的方式。

### 環境需求
- Python 3.10+
- （完整資料查詢）可存取的 HDFS 上有 `/dataset/M03A`、`/dataset/M06A` 分區 Parquet
- Java 11 + Spark 3.4（PySpark 隨查詢 API 使用）

### 1. 前端畫面本機預覽（免 Spark / HDFS）
```bash
cd pair3_webapp
pip install flask
python app_query_demo.py       # 以合成 / 樣本資料驅動前端，純看畫面
```

### 2. 查詢 API（需 Spark + HDFS）
```bash
pip install fastapi "uvicorn[standard]" pyspark
export HADOOP_USER_NAME=<your_hdfs_user>
python pair2_api/api_server_3.py        # 監聽 :8000，直查 HDFS 資料湖
```

### 3. 前端 + API 代理（整條鏈）
```bash
cd pair3_webapp
pip install flask gunicorn
export PAIR2_API_URL="http://<your-api-host>:8000"   # 指向上面的查詢 API
gunicorn -b 0.0.0.0:8000 -w 4 --timeout 120 app:app
```

### 設定（全部走環境變數，無寫死憑證）
| 變數 | 用途 | 預設 |
|---|---|---|
| `PAIR2_API_URL` | 前端代理的上游查詢 API 位址 | `http://<api-host>:8000` |
| `PAIR2_API_TIMEOUT` | 代理上游逾時（秒）| `110` |
| `HADOOP_USER_NAME` | 存取 HDFS 的使用者 | — |
| `MAX_CONCURRENT_SPARK_QUERIES` | Spark 查詢併發上限 | `4` |

參考 `.env.example`。**本專案不在程式碼中寫死任何憑證或連線密碼**，
所有敏感設定均透過環境變數注入。

## 專案結構

```
pair1_pipeline/     資料抓取與回填管線（歷史回填 / 每日自動化 / 共用工具）
airflow/dags/       Airflow DAG：每日四段式 ETL 流程定義
pair2_convert/      CSV→Parquet 轉檔、分區遷移、schema 統一等 Spark 工具
pair2_api/          FastAPI + Spark 查詢服務（含門架別名展開等資料治理邏輯）
pair3_webapp/       Flask 代理 + 原生 JS 前端（查詢頁 / 統計頁 / 地圖）
docs/               系統建置文件：K8s 架構、Hadoop/Spark 版本與調校、整體網路拓樸
```

## 系統建置與基礎設施

想了解整套平台怎麼建的（Kubernetes 叢集組成、HDFS/Spark/Hive 版本與參數、
網路拓樸、部署機制、實際踩過的坑與對策），見 [`docs/`](docs/)：

| 文件 | 內容 |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | 三層網路結構、對外入口、資料流、檔案投遞機制 |
| [`docs/kubernetes.md`](docs/kubernetes.md) | 叢集組成、namespace/工作負載、Service、儲存、部署機制 |
| [`docs/hadoop-spark.md`](docs/hadoop-spark.md) | HDFS/YARN/Hive/Spark 版本、關鍵設定、調校筆記 |

## 技術亮點（面試向）

- **真實資料治理**：門架改編號的跨期查詢遺漏，是實際營運才會遇到的坑；以別名群組 + 結果歸戶解決。
- **正確性優先的寫入**：暫存 → 驗筆數 → 原子 rename，線上服務零讀到半成品。
- **對執行環境的理解**：時區字串比對、容器 cgroup PID 上限下的 Spark 分批策略，都是踩過坑後的設計。
- **冪等與自我修復**：管線任一環節可安全重跑，以現況為準補漏，無需人工介入。
- **安全意識**：憑證全走環境變數、機密不進版控。
