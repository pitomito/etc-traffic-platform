# 系統建置與基礎設施文件

本目錄說明整個 ETC 國道流量平台的底層建置：Kubernetes 叢集、Hadoop/Spark
資料層、各元件的版本、設定與調校參數。應用程式碼的說明請看各目錄自己的
README（[`pair1_pipeline`](../pair1_pipeline/)、[`pair2_convert`](../pair2_convert/)、
[`pair2_api`](../pair2_api/)、[`pair3_webapp`](../pair3_webapp/)）。

> ⚠️ 本文件為作品集用途，所有 IP、主機名、叢集網域、憑證均已抽換為
> **佔位值**（例如 `192.0.2.x`、`registry.internal`、`cluster.local`、
> `portfolio-host`）。實際部署數值不對外。

## 文件索引

| 文件 | 內容 |
|---|---|
| [`architecture.md`](architecture.md) | 系統整體架構：三層結構、網路拓樸、資料流、檔案投遞機制 |
| [`kubernetes.md`](kubernetes.md) | K8s 叢集組成、namespace / 工作負載、Service 與對外入口、儲存、部署機制 |
| [`hadoop-spark.md`](hadoop-spark.md) | HDFS / YARN / Hive / Spark 版本、設定、關鍵參數與調校筆記 |

## 工具與版本矩陣

| 類別 | 元件 | 版本 | 備註 |
|---|---|---|---|
| **容器編排** | Kubernetes | v1.34 | 5 節點，podman-in-host（節點本身是主機上的容器）|
| | CNI | Canal | Calico + Flannel 組合 |
| | 容器 runtime | podman | 節點即 podman 容器；worker Pod 另加 gVisor sandbox |
| | Pod 沙箱 | gVisor（runsc）| DataNode 等 worker 工作負載用 `runtimeClassName: gvisor` |
| **資料儲存 / 運算** | Hadoop（HDFS + YARN）| 3.4.3 | NameNode / DataNode / ResourceManager / NodeManager |
| | Apache Spark | 3.4.4（主）| 另安裝 3.3.4、3.5.8 並存，依需求切換 |
| | Apache Hive | 4.0.1 | HiveServer2 + Metastore |
| | Metastore 後端 | MySQL | Hive metadata 儲存 |
| **排程** | Apache Airflow | standalone | SequentialExecutor + SQLite（單機排程，非高併發場景）|
| **執行環境** | JDK | OpenJDK 11（另備 17）| Hadoop/Spark 以 JDK 11 執行 |
| | 基底映像 | Ubuntu 22.04（節點）/ 24.04（主機）| |
| **服務層** | FastAPI + Uvicorn | — | 查詢 API（`pair2_api`）|
| | Flask + Gunicorn | — | 前端代理（`pair3_webapp`）|
| | haproxy | — | 主機層對外入口（TCP 模式）|
| | 容器映像 registry | registry:3 | 叢集內私有 registry |

## 為什麼記錄這些

資料平台的行為往往由「底層怎麼建的」決定——例如 NameNode 的 heap 大小
限制了小檔數量上限、容器 cgroup 的 PID 上限影響 Spark 長任務的執行策略、
節點的 gVisor sandbox 影響某些系統呼叫。這份文件把這些「看不見但會咬人」
的基礎設施決策寫下來，讓維運與除錯有據可循，也是這個專案在
「應用程式碼之外」對整體系統的掌握證明。
