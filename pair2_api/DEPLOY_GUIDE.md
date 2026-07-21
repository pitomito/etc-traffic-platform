# ETC 國道流量平台 — K8s 部署完全指南

> 適用環境：`/home/bigred/tk/wulin` 專案、五節點 podman-in-host K8s 叢集（v2-*）
> 最後更新：2026-07-16

---

# Part 1｜開始之前：你必須先懂的架構與互通原理

部署要順利，關鍵不是指令，而是先搞懂「**一個東西放在哪裡、另一個東西怎麼看得到它**」。
這套系統有三層，每一層的檔案和網路互通方式都不一樣。

## 1.1 三層結構總覽

```
┌─ 第 1 層：實體主機（portfolio-host, Ubuntu 24.04）───────────────┐
│   LAN IP: 192.0.2.10（enp4s0）      podman 內網: 172.31.0.254/24        │
│   跑著：haproxy（對外入口）、podman（下面五個節點容器）                        │
│                                                                              │
│  ┌─ 第 2 層：五個 podman 容器 = K8s「節點」(v1.34.8, CNI=calico) ─────────┐  │
│  │   v2-control-plane 172.31.0.1   （etcd、apiserver 在這裡）           │  │
│  │   v2-master1       172.31.0.2                                       │  │
│  │   v2-worker1       172.31.0.3                                       │  │
│  │   v2-worker2       172.31.0.4                                       │  │
│  │   v2-worker3       172.31.0.5                                       │  │
│  │                                                                       │  │
│  │  ┌─ 第 3 層：K8s Pods ────────────────────────────────────────────┐   │  │
│  │  │  Pod 網段 10.244.0.0/16    Service 網段 10.96.0.0/24         │   │  │
│  │  │  namespace dt：pair2_api（本平台）、dtm/dtw（HDFS）、metadb…    │   │  │
│  │  │  namespace web：前端負責人的 webapp proxy（另一條線）           │   │  │
│  │  └────────────────────────────────────────────────────────────────┘   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

重要事實：**172.31.0.x 是主機上的 podman 內網**，LAN 上其他電腦「看不到」它。
這就是為什麼需要 haproxy 當跳板（見 1.3）。

## 1.2 檔案怎麼互通：三層 bind mount 鏈

程式碼只有**一份**，放在主機硬碟上，透過兩段掛載讓 pod 看得到：

```
主機  /home/bigred/tk/wulin
        │  （podman run -v 掛進節點容器）
節點  /opt/wulin
        │  （k8s hostPath volume 掛進 pod）
Pod   /app
```

所以：

| 你在主機做的事 | Pod 裡看到什麼 | 需要做什麼才生效 |
|---|---|---|
| 改 `~/tk/wulin/pair2_api/webapp/` 裡的 JS/HTML/CSS | `/app/webapp/` 同步變 | **不用動作**，瀏覽器 Ctrl+F5 即可（FastAPI 每次請求都重新讀檔） |
| 改 `~/tk/wulin/pair2_api/api_server_3.py` | `/app/api_server_3.py` 同步變 | `kubectl rollout restart deploy/pair2_api -n dt`（Python 程式載入記憶體後不會自己重讀） |
| 改 `pair2_api-deploy.yaml` | 不會自動生效 | `kubectl apply -f` 該檔 |

同理，Spark 與 Hadoop 的安裝檔在 `/opt/zfs/v2`（主機）→ `/opt/zfs`（節點與 pod），
pod 啟動指令裡的 `PYTHONPATH` 就是指到這裡的 pyspark。

**注意**：pair2_api 的 Deployment 把 pod 排到哪個節點都沒關係，因為**五個節點都掛了同一個
`/home/bigred/tk/wulin`**，hostPath 在每個節點上看到的內容一樣。

## 1.3 網路怎麼互通：從瀏覽器到 Spark 的完整一條線

```
使用者瀏覽器（LAN 任何機器）
   │  http://192.0.2.10/          ← 80 port，不用打 port
   │  http://192.0.2.10:28001/    ← 備用入口
   ▼
主機 haproxy（/etc/haproxy/haproxy.cfg，systemd 服務、開機自啟）
   │  frontend pair2_web: bind *:80 + *:28001, mode tcp, timeout 300s
   │  backend  pair2_web_nodes: 五個節點 172.31.0.1~5 的 30081, round-robin
   ▼
K8s NodePort 30081（Service: pair2-web, namespace dt, selector app=pair2_api）
   │  kube-proxy 把任一節點收到的 30081 轉到 Service → 後端 Pod
   ▼
pair2_api Pod（containerPort 8000）
   │  FastAPI（uvicorn）：
   │    /、/query.html、/stats.html、/css、/js、/data  ← 供應 webapp 靜態檔
   │    /api/query、/api/stats、/api/stats/meta、/api/query/meta、/api/health
   ▼
Spark（local[*] 模式，在同一個 pod 的 JVM 裡，driver-memory 8g）
   │  HADOOP_USER_NAME=bigred
   ▼
HDFS（namespace dt 裡的 dtm/dtw pods）
     /dataset/M03A/year=YYYY/month=MM/day=DD/*.parquet   （日分割）
     /dataset/M06A/year=YYYY/month=MM/*.parquet          （月分割，每月一檔 ~3.7GB）
```

其他你必須知道的網路細節：

1. **Pod 的 DNS 是手動指定的**（deploy yaml 裡 `dnsConfig`）：nameserver `10.96.0.10`、
   search `svc-dt.dt.svc.cluster.local` 等。pod 內連 HDFS 用的主機名靠這個解析。
2. **前端 JS 一律打同源 API**（`fetch("/api/query")`），所以「誰供應網頁、誰就要供應 API」
   ——這就是 api_server_3.py 兼做靜態檔伺服器的原因，也因此**不會有 CORS 問題**。
3. 網頁會從 CDN 載入 Leaflet / Chart.js / OpenStreetMap 圖磚，**使用者的瀏覽器要能上網**，
   地圖和圖表才會顯示（API 資料不受影響）。
4. **時區陷阱**：容器是 UTC，資料裡的時間字串是台灣時間。api_server_3.py 刻意用
   「字串比較」而不是轉 timestamp，避免 8 小時偏移。改程式時不要「順手優化」成 timestamp。

## 1.4 元件清單：什麼東西放在哪裡

| 元件 | 位置 | 說明 |
|---|---|---|
| API + 網站程式 | `~/tk/wulin/pair2_api/api_server_3.py` | FastAPI + PySpark，單檔 |
| 前端網站 | `~/tk/wulin/pair2_api/webapp/` | index/query/stats.html + js/ + css/ + data/gantry_groups_frontend.json |
| 容器映像 | `registry.internal:5000/pair2_api:v2` | base=usdt.hdp34，只多裝 fastapi+uvicorn（見 Dockerfile） |
| Deployment 定義 | `~/tk/wulin/pair2_api/pair2_api-deploy.yaml` | 含 8g driver memory、hostPath 掛載、DNS 設定 |
| 對外 Service | `~/tk/wulin/pair2_api/pair2web-svc.yaml` | NodePort 30081 |
| haproxy 設定 | 主機 `/etc/haproxy/haproxy.cfg` | 80/28001 → 30081 |
| 拉映像的憑證 | Secret `dkreg-cred`（namespace dt） | imagePullSecrets 用 |
| K8s 物件本體 | etcd（control-plane 的 podman named volume） | 存在主機 `/var/lib/containers/storage/volumes/` |
| 資料 | HDFS `/dataset/M03A`、`/dataset/M06A` | M03A: 2022-01~2026-05；M06A: 2024-01~2025-12（會隨管線更新） |

## 1.5 持久性：重啟後什麼會活著

- **主機硬碟上的東西**（程式碼、webapp、yaml、haproxy 設定）：永遠都在。
- **K8s 物件**（Deployment、Service）：存在 etcd，etcd 資料在 podman named volume =
  主機硬碟 → pod 重啟、節點重啟、主機重開機都在。
- **Pod 本身**：無狀態，砍掉重建完全沒差（這是設計目標）。
- **唯一要手動的**：五個節點容器沒設 restart policy，**主機重開機後要手動把叢集拉起來**
  （見 Part 3 步驟 0），haproxy 則會自己起來。

---

# Part 2｜部署前置條件檢查（Checklist）

部署前逐項確認，任何一項不成立，後面都會卡：

- [ ] **叢集活著**：`kubectl get nodes` 五個節點都是 `Ready`
- [ ] **HDFS 資料在**：`kubectl exec -n dt deploy/dtadm -- hdfs dfs -ls /dataset` 看得到 M03A、M06A
      （或用任何一個掛了 hadoop client 的 pod）
- [ ] **registry 可用**：`curl -s http://registry.internal:5000/v2/_catalog` 有回應，
      裡面有 `pair2_api`（或 base image `usdt.hdp34`）
- [ ] **Secret 存在**：`kubectl get secret dkreg-cred -n dt` 存在（拉映像用）
- [ ] **檔案就位**：`~/tk/wulin/pair2_api/` 下有 `api_server_3.py`、`webapp/`（含 index/query/stats.html、js/、css/、data/）、
      `pair2_api-deploy.yaml`、`pair2web-svc.yaml`、`Dockerfile`
- [ ] **haproxy 已安裝**：`systemctl status haproxy` 是 active，且 `systemctl is-enabled haproxy` 是 enabled
- [ ] **主機 80 / 28001 port 沒被別人占用**：`sudo ss -tlnp | grep -E ":80 |:28001"` 只能是 haproxy（或空）

---

# Part 3｜部署流程（超詳細版）

## 步驟 0：把叢集拉起來（只有主機剛重開機才需要）

```bash
# 0-1 看節點容器是否在跑
sudo podman ps --format "{{.Names}} {{.Status}}"

# 0-2 沒在跑就依序啟動（control-plane 要先起）
sudo podman start v2-control-plane
sleep 20
sudo podman start v2-master1 v2-worker1 v2-worker2 v2-worker3

# 0-3 等所有節點 Ready（可能要 1~3 分鐘）
watch kubectl get nodes
# 五個都 Ready 後 Ctrl+C

# 0-4 確認基礎服務 pod 都恢復（HDFS 等）
kubectl get pods -n dt
```

## 步驟 1：確認程式碼與網站檔案就位

```bash
ls ~/tk/wulin/pair2_api/
# 必須看到：api_server_3.py  webapp/  Dockerfile  pair2_api-deploy.yaml  pair2web-svc.yaml

ls ~/tk/wulin/pair2_api/webapp/
# 必須看到：index.html  query.html  stats.html  css/  js/  data/

# 語法快速檢查（改過 python 的話）
python3 -m py_compile ~/tk/wulin/pair2_api/api_server_3.py && echo OK
```

**如果前端負責人給了新版 webapp**：把它覆蓋到 `~/tk/wulin/pair2_api/webapp/`，
但記得我們在這份上有自己的修改（M06A 階梯式選單、日期防呆等，詳見 CHANGES_2026-07-16.md 第五、六節），
覆蓋前先 diff，把我們的修改合回去。

## 步驟 2：建置映像（只有 Dockerfile 變了才需要，平常跳過）

映像刻意做得很薄：程式碼**不進映像**（用掛載的），所以改 code 不用重 build。

```bash
cd ~/tk/wulin/pair2_api

# 2-1 build（tag 版本號往上加，不要覆蓋舊 tag）
sudo podman build -t registry.internal:5000/pair2_api:v2 .

# 2-2 push 到內部 registry
sudo podman push registry.internal:5000/pair2_api:v2

# 2-3 如果改了 tag（例如 v3），記得同步改 pair2_api-deploy.yaml 裡的 image: 欄位
```

## 步驟 3：部署 API（Deployment + 內部 Service）

```bash
kubectl apply -f ~/tk/wulin/pair2_api/pair2_api-deploy.yaml
```

這份 yaml 的關鍵欄位（改動前先理解）：

```yaml
spec.template.spec:
  dnsPolicy: None                    # 手動 DNS，讓 pod 解析得到 HDFS 主機名
  dnsConfig:
    nameservers: [10.96.0.10]
    searches: [svc-dt.dt.svc.cluster.local, dt.svc.cluster.local, svc.cluster.local]
  containers:
  - image: registry.internal:5000/pair2_api:v2
    args:                            # 啟動指令重點：
    - ... PYTHONPATH=<指到 /opt/zfs 的 pyspark> ...
      export HADOOP_USER_NAME=bigred;                      # HDFS 身分
      export PYSPARK_SUBMIT_ARGS="--master local[*] --driver-memory 8g pyspark-shell";
      exec python3 /app/api_server_3.py                    # ← 8g 是 M06A 聚合查詢的命脈，不能拿掉
    ports: [{containerPort: 8000}]
    volumeMounts:
    - {name: appcode, mountPath: /app}      # ← 程式碼從節點 /opt/wulin/pair2_api 掛進來
    - {name: zfs,     mountPath: /opt/zfs}  # ← Spark/Hadoop 安裝
  volumes:
  - {name: appcode, hostPath: {path: /opt/wulin/pair2_api}}
  - {name: zfs,     hostPath: {path: /opt/zfs}}
```

等 pod 起來：

```bash
kubectl rollout status deploy/pair2_api -n dt --timeout=180s
kubectl get pods -n dt -l app=pair2_api
# STATUS 必須是 Running；如果 CrashLoopBackOff 見 Part 4 排錯
```

## 步驟 4：部署對外 NodePort Service

```bash
kubectl apply -f ~/tk/wulin/pair2_api/pair2web-svc.yaml
kubectl get svc -n dt pair2-web
# 應顯示：NodePort  8000:30081/TCP
```

## 步驟 5：Pod 內驗證（先確定 API 自己是好的，再往外接）

```bash
kubectl exec -n dt deploy/pair2_api -- bash -c '
curl -s -o /dev/null -w "首頁 %{http_code}\n"      http://localhost:8000/
curl -s -o /dev/null -w "查詢頁 %{http_code}\n"    http://localhost:8000/query.html
curl -s http://localhost:8000/api/health; echo
curl -s http://localhost:8000/api/query/meta; echo'
```

預期：兩個 200、health 回 `{"ok":true,...}`、meta 回兩個資料集的日期範圍。
**meta 有正確日期 = Spark 連 HDFS 成功**，這步過了後面就只剩網路層。

## 步驟 6：設定主機 haproxy（對外入口）

`/etc/haproxy/haproxy.cfg` 需要有這兩段（已存在就確認內容）：

```
frontend pair2_web
    bind *:80
    bind *:28001
    mode tcp
    timeout client 300000
    default_backend pair2_web_nodes

backend pair2_web_nodes
    mode tcp
    balance roundrobin
    timeout server 300000
    server cp  172.31.0.1:30081 check
    server m1  172.31.0.2:30081 check
    server w1  172.31.0.3:30081 check
    server w2  172.31.0.4:30081 check
    server w3  172.31.0.5:30081 check
```

兩個 timeout 是 **300 秒**（預設 50 秒會砍掉跑比較久的 Spark 查詢，一定要加）。

```bash
# 6-1 驗證設定檔語法（有錯不會過，不怕改壞）
sudo haproxy -c -f /etc/haproxy/haproxy.cfg

# 6-2 平滑重載（不斷線）
sudo systemctl reload haproxy

# 6-3 確認在聽
sudo ss -tlnp | grep -E ":80 |:28001"
```

## 步驟 7：端到端驗證（照這份清單跑完才算部署完成）

```bash
# 7-1 網頁層
curl -s -o /dev/null -w "%{http_code}\n" http://192.0.2.10/            # 200
curl -s -o /dev/null -w "%{http_code}\n" http://192.0.2.10/query.html  # 200
curl -s -o /dev/null -w "%{http_code}\n" http://192.0.2.10/js/query.js # 200

# 7-2 API 層
curl -s http://192.0.2.10/api/health         # {"ok":true,...}
curl -s http://192.0.2.10/api/query/meta     # 兩個資料集的日期範圍
curl -s http://192.0.2.10/api/stats/meta     # M03A 統計範圍

# 7-3 真槍實彈：一筆 M03A（秒級）
curl -s -X POST http://192.0.2.10/api/query -H "Content-Type: application/json" \
  -d '{"dataset":"M03A","start_time":"2026-05-13 08:00","end_time":"2026-05-13 09:00","gantry_ids":["01F1465N"]}' \
  | head -c 200

# 7-4 真槍實彈：一筆 M06A（約 5~15 秒，注意日期要在 M06A 資料範圍內！）
curl -s -X POST http://192.0.2.10/api/query -H "Content-Type: application/json" \
  -d '{"dataset":"M06A","start_time":"2025-05-13 08:00","end_time":"2025-05-13 09:00","start_gantry_ids":["01F0099N"],"end_gantry_ids":["01F0005N"],"start_gantry":"01F0099N","end_gantry":"01F0005N"}' \
  | head -c 200
```

7-3、7-4 都回 `"ok":true` 且 `row_count > 0` → **部署完成**。
最後用另一台電腦的瀏覽器開 `http://192.0.2.10/` 實際查一次。

## 步驟 8：日常更新流程（部署完之後的維運）

| 改了什麼 | 要做什麼 |
|---|---|
| webapp 的 JS/HTML/CSS | 什麼都不用做，使用者 Ctrl+F5 |
| api_server_3.py | `kubectl rollout restart deploy/pair2_api -n dt` |
| pair2_api-deploy.yaml | `kubectl apply -f pair2_api-deploy.yaml` |
| haproxy.cfg | `sudo haproxy -c -f /etc/haproxy/haproxy.cfg && sudo systemctl reload haproxy` |
| Dockerfile | 重 build + push + 改 yaml 的 image tag + apply |

---

# Part 4｜排錯指南（照症狀查）

| 症狀 | 先查 | 常見原因與解法 |
|---|---|---|
| 瀏覽器完全連不上 | `curl http://192.0.2.10/api/health`（在主機上） | 主機上通=使用者端問題（網段/打錯網址）；不通往下查 |
| 主機上也不通 | `systemctl status haproxy`、`ss -tlnp \| grep :80` | haproxy 掛了→ restart；port 被占→ 查誰占的 |
| haproxy 通、NodePort 不通 | `curl http://172.31.0.5:30081/` | Service/pod 掛了→ `kubectl get pods,svc -n dt` |
| Pod CrashLoopBackOff | `kubectl get events -n dt --sort-by=.lastTimestamp \| tail` | 常見是 calico 網路暫時故障（等它自己好）或 python 語法錯（看 `kubectl logs`） |
| 網頁 404 | pod 內 `ls /app/webapp/` | webapp 目錄沒放到 `~/tk/wulin/pair2_api/webapp/` |
| 查詢送出後永遠轉圈→失敗 | `kubectl logs -n dt deploy/pair2_api --tail=50` 找 GC/RPC timeout | driver memory 不夠：確認 yaml 裡有 `--driver-memory 8g` |
| M06A 一直 0 筆 | log 裡的 `[api/query]` 那行（有完整參數） | ① 日期超出 M06A 資料範圍 ② 起訖門架方向/順序不對（0.0s 回空=①） |
| meta 回 null 日期 | pod 內 `hdfs dfs -ls /dataset/M06A` | HDFS 沒起來或資料目錄被管線搬走 |
| 查詢變超慢 | `kubectl top pod -n dt`（或 free） | 節點記憶體被別的作業吃光；M06A 正在重整（`.retype_tmp` 存在） |

每一筆 `/api/query` 都會在 pod log 留一行：
`[api/query] M06A 2025-05-13 08:00~... o=[...] d=[...] vt='' rows=16 7.6s`
——排「使用者說查不到」這類問題，**先看這行**，參數、筆數、耗時一目了然。

---

# 附錄：本平台 API 一覽

| 端點 | 方法 | 用途 |
|---|---|---|
| `/api/health` | GET | 健康檢查 |
| `/api/query/meta` | GET | M03A/M06A 資料涵蓋範圍（前端日期防呆用） |
| `/api/query` | POST | 查詢頁主端點（dataset=M03A/M06A；跨度上限 366/92 天） |
| `/api/stats/meta` | GET | 統計頁資料範圍 |
| `/api/stats` | POST | 統計儀表板（日/週/月） |
| `/api/m03a`、`/api/m06a` | GET | 舊版端點（向下相容，web 代理仍在用） |
| `/`、`/query.html`、`/stats.html`、`/css/*`、`/js/*`、`/data/*` | GET | 網站靜態檔 |
