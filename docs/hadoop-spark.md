# Hadoop / Spark / Hive 資料層

> 位址與網域為佔位值。本文件著重版本、設定與**踩過的坑**——這些調校
> 決策大多是實際運行後才發現必要，不是憑空選的預設值。

## 版本矩陣

| 元件 | 版本 | 備註 |
|---|---|---|
| Hadoop（HDFS + YARN）| 3.4.3 | |
| Apache Spark | 3.4.4（主要使用）| 另安裝 3.3.4、3.5.8 供相容性測試 |
| Apache Hive | 4.0.1 | HiveServer2 + Metastore |
| Hive Metastore 後端 | MySQL | |
| JDK | OpenJDK 11 | Hadoop/Spark 執行環境；另備 17 |

多版本 Spark 並存（安裝在 `/opt/zfs/sys/spark-<ver>-bin-hadoop3/`）是為了
在不換底層環境的前提下，測試不同 Spark 版本對特定 job 的相容性
（例如評估升級到 3.5 對既有 pipeline 的影響），而不需要另建整套叢集。

## HDFS 配置

### 節點角色

| 角色 | 副本 | 說明 |
|---|---|---|
| NameNode | 1 主 + 1 SecondaryNameNode | 跑在標記 `dt: master` 的節點 |
| DataNode | 3 | 跑在標記 `dt: worker` 的節點，`runtimeClassName: gvisor` |

### 已知限制：NameNode heap 偏小

```
HADOOP_HEAPSIZE_MAX=384   # MB
HADOOP_HEAPSIZE_MIN=256
```

這是這套環境**刻意壓低**的資源設定（單機展示環境，非生產規模），
但直接影響了資料湖的分區設計決策：**NameNode 記憶體與檔案/目錄數量
成正比**，小檔案/目錄數太多會逐漸吃緊。這也是為什麼
[`pair2_convert`](../pair2_convert/) 要把 M03A 從「日分區」（每天一個
目錄，5 年下來近 2,000 個目錄）重整成「月分區」——用大約 60 個月目錄
取代近 2,000 個日目錄，同樣的資料量對 NameNode 的 metadata 壓力小
非常多。**調小的 heap 不是筆誤或疏忽，是理解此限制後主動做的分區優化**。

### PVC 容量

見 [`kubernetes.md`](kubernetes.md#儲存)：NameNode/SecondaryNN 各 60Gi、
ZooKeeper 30Gi、每個 DataNode 200Gi（×3）。

## YARN / Spark 執行模式

兩種執行模式並存，依用途選擆：

| 模式 | 用於 | 說明 |
|---|---|---|
| `--master yarn --deploy-mode client` | ETL 批次工具（`pair2_convert/*.py`）| 走 YARN 排資源，Airflow 呼叫的日常轉檔即此模式 |
| `--master local[*]` | 查詢 API（`pair2_api`）| 常駐服務直接用本機模式跑 Spark，`--driver-memory 8g`，省掉每次查詢都要向 YARN 申請資源的延遲 |

查詢 API 選 local 模式的取捨：換取查詢延遲降低，代價是資源隔離較弱
（查詢 API 自己就要控制併發，見下方「查詢服務併發控制」）。

### YARN 資源分配範例（Airflow 呼叫的 spark-submit）

```
--master yarn --deploy-mode client
--driver-memory 2g --driver-cores 1
--num-executors 2 --executor-memory 3g --executor-cores 1
```

## 已知地雷與對策

### 1. 容器 cgroup 的 PID/執行緒數上限

**現象**：長時間執行的單一 Spark driver JVM，逐步累積執行緒（Spark 內部
執行緒池、HDFS 檔案列舉的 ForkJoinPool 等），最終撞到容器 cgroup 的
`pids.max` 限制，丟出 `java.lang.OutOfMemoryError: unable to create
native thread`（本質是 PID 額度耗盡，不是真的記憶體不足，訊息容易誤導）。

**對策**：批次工具（如分區重整、資料遷移）刻意設計成**每處理完一小批
就讓整個 JVM 程序結束、下一批重新啟動**，而不是在同一個長壽 JVM 裡跑完
全部工作。外部 shell 迴圈搭配 state file 記錄進度，重啟後自動從斷點
接續，任一批失敗也不影響已完成的部分。

```bash
while :; do
  spark-submit ... --max-batch 4
  rc=$?
  [ $rc -eq 3 ] && continue   # 3 = 這批做完了，還有剩，換新 JVM 續跑
  break                        # 0 = 全部完成；其他 = 真的失敗
done
```

### 2. spark-defaults.conf 的設定飄移

**現象**：叢集的 `spark-defaults.conf` 曾被手動改過（例如加上
`spark.task.cpus=4`），但 `spark-submit` 指令的 `--executor-cores` 卻是
沿用舊值（例如 1）。兩者不一致時，`SparkContext` 初始化直接拒絕啟動：

```
The number of cores per executor (=1) has to be >= spark.task.cpus (=4)
```

**對策**：不假設節點上的 `spark-defaults.conf` 內容跟版控裡的一致，
關鍵 job 的 `spark-submit` 一律用 `--conf spark.task.cpus=1` 明確蓋掉，
不依賴節點本地可能已經飄移的設定檔。

### 3. UDF 記憶體開銷

**現象**：Spark 的 Python UDF 需要把資料序列化、丟進獨立的 Python
worker 處理，掃描整月大型 Parquet（單月旅次資料近億筆）時，這個
序列化開銷會把 Spark JVM 記憶體吃到見底。

**對策**：字串比對邏輯（例如判斷一趟旅次是否依序經過門架 A 再到 B）
改用 Spark SQL 內建的 `rlike` / `regexp_extract`，全程留在 JVM 端
用原生 Catalyst 引擎執行，不跨進程序列化。

### 4. 時區陷阱

容器內部系統時區是 UTC，但資料裡的時間欄位是**字串型別**、內容是
台灣當地時間。若把這欄轉成 `timestamp` 型別再比較，Spark 會依系統時區
做隱性轉換，產生 8 小時的偏移誤差。**對策是刻意維持字串型別、用字串
比對**（字串格式恰好與時間順序一致，字典序比較等同時間序比較）—— 這是
權衡過的正確選擇，遇到「優化成 timestamp」的建議應該拒絕。

## Hive Metastore

HiveServer2 搭配獨立 MySQL 存放 metadata，`hive-site.xml` 設定
`ConnectionURL` 指向 Metastore 的 Service DNS 名（不寫死 IP）。目前
`/dataset` 底下的 Parquet 資料湖是**直接用 Spark 讀寫檔案路徑**，
未透過 Hive 外部表註冊——這代表查詢層（`pair2_api`）跟 ETL 層
（`pair2_convert`）對分區結構的認知必須手動保持一致，是後續可以優化的
方向（改用 Hive/Iceberg catalog 統一 schema 與分區定義）。

## 查詢服務併發控制

`pair2_api` 用 local 模式常駐執行 Spark，多個查詢同時進來時容易把
driver heap（8g）撐爆。對策：

- 查詢邏輯丟進 threadpool 執行，避免長查詢卡住 FastAPI 的 event loop
- 用 `Semaphore` 限制同時執行的 Spark 查詢數，超過上限的請求在
  threadpool 裡排隊等，而不是直接一起衝進 Spark
- 統計類查詢採**單趟聚合**（一次 Spark job 把明細壓縮到「時間桶 × 維度」
  的中間粒度後收回 Python 端運算），取代舊版「cache 全量明細後跑多個
  聚合 job」的作法——後者在查詢跨度變大時容易讓 cache 的資料量超過
  heap 上限而觸發 OOM。

## Airflow

- **Executor**：`SequentialExecutor` + SQLite 後端（standalone 模式，
  單機序列執行，非高併發生產配置，符合此環境的資料量與排程頻率）
- **`max_active_runs=1`**：確保同一條 DAG 不會並發執行多個 run，這也是
  「補漏檢查可以安全放進同一條 DAG」的前提——避免補漏與正常抓取同時
  對資料來源發出請求
