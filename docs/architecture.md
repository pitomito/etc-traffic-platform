# 系統整體架構

> 所有 IP / 主機名 / 網域為佔位值（`192.0.2.x` 對外、`172.31.0.x` 節點內網、
> `10.96.0.x` Service 網段、`cluster.local` 叢集網域、`registry.internal`
> 私有映像庫）。

## 三層結構總覽

整個平台跑在一台實體主機上，主機用 podman 起了 5 個容器當作 Kubernetes
節點（podman-in-host），實際的應用 Pod 再跑在這些「容器節點」裡。所以有
三層由外而內的網路：

```
┌─────────────────────────────────────────────────────────────────┐
│ 實體主機 portfolio-host（Ubuntu 24.04）                            │
│                                                                    │
│   對外網路：LAN 192.0.2.10  /  遠端 192.0.2.20（同 port）           │
│        │                                                           │
│        ▼                                                           │
│   haproxy（TCP 模式，開機自啟）── :80 / :28000 / :28001            │
│        │  轉發到節點 NodePort                                       │
│        ▼                                                           │
│ ┌────────────────────────────────────────────────────────────┐   │
│ │ podman 內網 172.31.0.0/24（主機 = .254，LAN 看不到此網段）    │   │
│ │                                                              │   │
│ │  5 個 podman 容器節點（= K8s 節點）：                          │   │
│ │   control-plane .1 / master .2 / worker .3 / .4 / .5          │   │
│ │                                                              │   │
│ │  ┌────────────────────────────────────────────────────┐     │   │
│ │  │ Kubernetes（Pod 網段 10.244/16、Service 10.96.0/24）  │     │   │
│ │  │   應用 Pod：查詢 API、HDFS、Hive、Airflow…            │     │   │
│ │  └────────────────────────────────────────────────────┘     │   │
│ └────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

**為什麼要 haproxy 這一層**：NodePort 只綁在節點的 `172.31.0.x` 內網 IP 上，
LAN / 外網完全看不到那個網段。所以對外服務一律靠主機的 haproxy 把
`:80/:28000/:28001` 轉進節點 NodePort，haproxy 是唯一的對外入口。

## 網路分層一覽

| 層 | 網段 / 位址 | 誰看得到 | 用途 |
|---|---|---|---|
| 對外 | `192.0.2.10`（LAN）/ `192.0.2.20`（遠端）| 使用者瀏覽器 | 進 haproxy |
| podman 內網 | `172.31.0.0/24`（主機 `.254`）| 只有主機與節點 | 節點互連、NodePort |
| K8s Pod | `10.244.0.0/16` | 叢集內 | Pod 間通訊（IP 每次重建都變，不寫死）|
| K8s Service | `10.96.0.0/24`，網域 `cluster.local` | 叢集內 | 穩定 DNS 名（`<svc>.<ns>.svc.cluster.local`）|
| 叢集 DNS | `10.96.0.10:53` | 叢集內 | CoreDNS / kube-dns |

**設計原則**：程式與設定一律用 Service 的 DNS 名（`<svc>.<ns>.svc.cluster.local`），
不寫死 ClusterIP——Service 重建 ClusterIP 就會變，DNS 名才穩定。

## 對外入口（haproxy）

主機 `/etc/haproxy/haproxy.cfg`，systemd 開機自啟，TCP 模式：

| 前端 bind | 後端 | 指向 | 逾時 |
|---|---|---|---|
| `:80`、`:28000` | 五節點 `172.31.0.1..5:30080` | 正式站 `pair3_webapp`（roundrobin）| client/server 300s |
| `:28001` | 五節點 `172.31.0.1..5:30081` | 教學站 `pair2_api`（roundrobin）| client/server 300s |

逾時特意拉到 **300s**：Spark 重查詢可能跑很久，逾時鏈必須由外而內遞減，
避免外層先斷、內層卻還在跑造成「前端顯示失敗但後端其實查得到」的不一致：

```
haproxy 300s  ＞  gunicorn 120s  ＞  app.py 代理上游 110s
```

## 兩條網站服務線

同一套查詢 API 後端（`pair2_api`），前面掛了兩條並存的前端線：

| 入口 | 路徑 | 說明 |
|---|---|---|
| `http://192.0.2.10/`（或 :28000）| haproxy → NodePort 30080 → `pair3_webapp` Pod ×3 | **正式站**：Flask 供應靜態檔，`/api/*` 全代理給 `pair2_api` |
| `http://192.0.2.10:28001/` | haproxy → NodePort 30081 → `pair2_api` Pod | 教學站：FastAPI 直接掛靜態檔 + 同源 API |

```
瀏覽器 → haproxy → NodePort 30080 → pair3_webapp（Flask）
   └ /api/* 轉送 PAIR2_API_URL=http://pair2-api.<ns>.svc.cluster.local:8000
       → pair2_api（FastAPI + Spark local[*], driver 8g）
           → HDFS /dataset/M03A（日分區 Parquet）
           → HDFS /dataset/M06A（月分區 Parquet）
```

前端一律打**同源** `/api/*`，由代理層轉送，因此無 CORS 問題，前端也不需
知道查詢服務實際部署在哪。

## 資料流（端到端）

```
政府開放資料（每日 CSV / 歷史 tar.gz）
      │  Airflow DAG（@daily，冪等、自我修復）
      ▼
 HDFS /raw/<type>/year=/month=/…（原始 CSV 中繼區）
      │  Spark 轉檔（pair2_convert）
      ▼
 HDFS /dataset/<type>/…（分區 Parquet 資料湖 ← 網站唯一資料來源）
      │
      ▼
 pair2_api（FastAPI + Spark 直查，無中間 DB） → pair3_webapp → 瀏覽器
```

**重要界線**：`/raw` 與本機 staging 都是**資料管線的中繼站**，不是服務用
資料；網站查詢只認最終的 `/dataset`。這條界線避免了「把中繼半成品誤接給
線上服務」的常見坑。

## 檔案投遞：三層 bind mount

程式碼只有主機一份，靠兩層 mount 讓所有節點與 Pod 看到同一份：

```
主機 /home/<user>/…/wulin   ─(podman -v)→   節點 /opt/wulin   ─(hostPath)→   Pod /app
主機 /opt/zfs               ─(podman -v)→   節點 /opt/zfs     ─(hostPath)→   Pod /opt/zfs
                                                （Spark/Hadoop 安裝檔）
```

五個節點都掛同一份，Pod 排到哪個節點都一樣。**改主機檔案 = Pod 內立即
可見**（但已載入記憶體的行程要重啟才會重讀——所以改 Python 要
`rollout restart`，改靜態檔不用）。

## 關鍵設計取捨

- **無中間資料庫**：查詢 API 直接對 HDFS Parquet 下 Spark 查詢，省掉一層
  OLAP DB 的維運與同步成本；代價是查詢延遲較高，用併發閘門 + 單趟聚合
  控制資源（詳見 [`hadoop-spark.md`](hadoop-spark.md)）。
- **Service DNS 而非 IP**：所有跨元件連線走 `cluster.local` DNS，重建不受影響。
- **逾時由外而內遞減**：避免分散式逾時造成的狀態不一致。
- **中繼區與服務區分離**：`/raw`（中繼）vs `/dataset`（服務），界線清楚。
