# Kubernetes 叢集架構

> IP / 主機名 / registry 網域為佔位值，實際部署數值不對外。

## 叢集組成

- **版本**：Kubernetes v1.34
- **節點數**：5（1 control-plane + 1 master + 3 worker），全部是主機上的
  **podman 容器**（podman-in-host，不是真實體機或雲端 VM）
- **CNI**：Canal（Calico 網路策略 + Flannel 覆蓋網路）
- **容器 runtime**：節點本身跑在 podman 裡；部分工作負載額外指定
  `runtimeClassName: gvisor`，多一層使用者空間核心（runsc）沙箱

| 節點 | 角色 | 內網位址（佔位） | 主要工作負載 |
|---|---|---|---|
| control-plane | K8s control-plane | `172.31.0.1` | API server、管理類 Pod |
| master | 標記 `dt: master` | `172.31.0.2` | HDFS NameNode/SecondaryNN + ZooKeeper |
| worker1 | 標記 `dt: worker` | `172.31.0.3` | HDFS DataNode |
| worker2 | 標記 `dt: worker` | `172.31.0.4` | HDFS DataNode |
| worker3 | 標記 `dt: worker` | `172.31.0.5` | HDFS DataNode |

節點角色靠 **`nodeSelector`** 決定（例如 `dt: master`、`dt: worker`、
`dt: admin`），不是用 K8s 內建的 `node-role` label——這是因為同一批節點
上要跑多種不相關的工作負載（HDFS、管理箱、Airflow），用自訂 key 更精確。

## Namespace 與工作負載

| Namespace | 工作負載 | 副本數 | 角色 |
|---|---|---|---|
| `dt`（資料層） | `dtm`（StatefulSet）| 2 | HDFS NameNode + SecondaryNameNode + ZooKeeper |
| | `dtw`（StatefulSet）| 3 | HDFS DataNode（`runtimeClassName: gvisor`）|
| | `dep-dths2`（Deployment）| 1 | HiveServer2（hostPort 直通）|
| | `dep-metadb`（Deployment）| 1 | MySQL，Hive Metastore 後端 |
| | `dtadm`（Deployment）| 1 | 特權管理/客戶端 Pod（ssh、spark-submit 入口）|
| | `pair2-api`（Deployment）| 1 | 查詢 API（FastAPI + Spark local 模式）|
| `web` | `pair3-webapp`（Deployment）| 3 | 前端代理（Flask + Gunicorn）|
| `airflow` | `airflow`（Deployment）| 1 | Airflow standalone（排程）|
| `registry-ns` | `registry-mgmt`（Deployment，2 容器）| 1 | 管理箱 + 私有映像 registry |
| `local-path-storage` | provisioner | — | 動態配置 StatefulSet 的節點本機儲存 |
| `kube-system` | canal、kube-proxy、coredns、metrics-server | — | 叢集基礎設施 |

**StatefulSet vs Deployment 的選用原則**：需要穩定身分 + 持久儲存的
（HDFS NameNode/DataNode）用 StatefulSet，配 headless Service 讓每個
replica 有固定 DNS 名（例如 `dtm-0.svc-dt.dt.svc.cluster.local`）；
其餘無狀態服務用 Deployment。

## Service 與對外入口

| Namespace | Service | Type | Port | NodePort | 用途 |
|---|---|---|---|---|---|
| `web` | `pair3-webapp-svc` | NodePort | 8000→8000 | 30080 | 正式站對外入口 |
| `dt` | `pair2-api-web`（教學站）| NodePort | 8000→8000 | 30081 | 教學站對外入口 |
| `dt` | `pair2-api`（內部）| ClusterIP | 8000→8000 | — | 給 `pair3-webapp` 內部呼叫 |
| `dt` | `dths2` | ClusterIP | 10000、22 | — | HiveServer2 |
| `dt` | `metadb` | ClusterIP | 3306→3306 | — | Hive Metastore MySQL |
| `dt` | `svc-dt`（Headless）| None | — | — | `dtm`/`dtw` 的 StatefulSet DNS |
| `registry-ns` | `registry`（ClusterIP）| ClusterIP | 5000→5000 | — | 私有映像 registry |

**只有走 haproxy 的服務才對外可見**：NodePort 只綁在節點內網 IP
（`172.31.0.x`），LAN/外網完全看不到；沒走 haproxy 的服務（如 Airflow UI）
只能從主機直連節點 IP。詳見 [`architecture.md`](architecture.md) 的
haproxy 一節。

## 儲存

StatefulSet 用 `local-path` storageClass 動態配置**節點本機磁碟**當
PV（不是網路儲存）：

| PVC | 容量 | 掛載對象 |
|---|---|---|
| `nn`（NameNode）| 60Gi | `dtm` |
| `sn`（SecondaryNameNode）| 60Gi | `dtm` |
| `zk`（ZooKeeper）| 30Gi | `dtm` |
| `dn`（DataNode）| 200Gi × 3 | `dtw` × 3 |

**取捨**：用 local-path 而非分散式儲存（Ceph/NFS）簡化了單機開發/展示環境
的建置，代價是 Pod 沒辦法自由排到別的節點（PV 綁定在建立時所在的節點）。

## 部署機制：程式碼即掛載，非映像內建

應用程式碼（`api_server_3.py`、`app.py`、前端靜態檔）**不是 build 進容器
映像裡**，而是用 `hostPath` 從節點掛進 Pod：

```yaml
volumes:
  - name: appcode
    hostPath: {path: /opt/wulin/pair2_api}
volumeMounts:
  - name: appcode
    mountPath: /app
```

搭配主機到節點的另一層 podman bind mount（見
[`architecture.md`](architecture.md#檔案投遞三層-bind-mount)），改動主機
上的程式碼會立即反映到所有節點、所有 Pod。這個設計讓「改程式碼」與
「建映像、推 registry、滾動更新」兩件事解耦：

| 改動類型 | 生效方式 |
|---|---|
| Python 程式碼（`.py`）| `kubectl rollout restart deploy/<name>`（行程已載入記憶體，不會自己重讀）|
| 靜態檔（HTML/JS/CSS）| 什麼都不用做，瀏覽器重新整理即可（每次請求都重新讀檔）|
| 依賴套件變更 / 系統層變更 | 需要重建映像、推私有 registry、更新 Deployment 的 `image:` tag |

映像本身只裝**執行環境**（Python + 必要套件），程式碼與環境分離。

## DNS 與 Pod 網路

部分 Pod（如 `dtm`/`dtw`/`dtadm`/`pair2-api`）用 `dnsPolicy: None` +
手動 `dnsConfig` 指定 nameserver 與 search domain，確保能解析同 namespace
的 headless Service 名（連 HDFS 時靠這個解析 NameNode 主機名）：

```yaml
dnsPolicy: None
dnsConfig:
  nameservers: [10.96.0.10]
  searches: [svc-dt.dt.svc.cluster.local, dt.svc.cluster.local, svc.cluster.local]
```

## 憑證與私有映像

私有 registry 拉取憑證以 `imagePullSecrets`（`dockerconfigjson` 類型）
掛在需要拉私有映像的 namespace，憑證本身存在 K8s Secret，不進版控、
不寫死在 Deployment yaml 裡。
