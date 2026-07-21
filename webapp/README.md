# webapp — 正式站前端（Flask 代理 + 原生 JS）

使用者實際瀏覽的網站。Flask（`app.py`）扮演「靜態檔伺服器 + API 反向
代理」的角色，本身不碰資料，所有 `/api/*` 請求原樣轉送給
[`pair2api`](../pair2api/) 的查詢服務。

## 檔案說明

- **`app.py`** — 唯一會被部署到生產環境的後端進來。用
  `PAIR2_API_URL` 環境變數決定上游查詢服務位址，把逾時時間刻意設在
  上游 gunicorn timeout 之內，避免代理層先逾時而查詢服務還在跑，
  造成前端顯示錯誤但後端其實查得到資料的不一致狀態。
- **`app_query_demo.py`** — **僅供本機開發預覽**，用合成 / 樣本資料
  驅動前端畫面，不連 HDFS，方便純看畫面或前端開發時不用架資料環境。
  不會被部署上線。
- **`index.html` / `query.html` / `stats.html`** — 三個頁面：首頁、
  查詢頁（車流量 / 旅行時間）、統計儀表板。
- **`css/`** — 各頁面樣式。
- **`js/query/`** — 查詢頁模組化邏輯：`api.js`（打 API）、
  `gantries.js`（門架選單/搜尋）、`m03a.js` / `m06a.js`（兩種資料集
  各自的查詢表單與結果處理）、`map.js`（Leaflet 地圖畫路徑）、
  `state.js`（頁面狀態管理）。
- **`js/stats/`** — 統計儀表板模組：`api.js`、`date-utils.js`、
  `render.js`（Chart.js 圖表渲染）。
- **`data/gantry_groups_frontend.json`** — 門架分組對照表（公開路網
  資訊，無個資），供地圖與路段排行使用；不隨每次查詢重新產生，是
  靜態資產。

## 查詢一次的完整路徑

```
瀏覽器 → webapp/app.py（Flask 代理）
            │  /api/*（同源，無 CORS 問題）
            ▼
      pair2api/api_server_3.py（FastAPI + Spark，直查 HDFS 資料湖）
```

前端一律打同源的 `/api/*`，代理層再轉送到實際的查詢服務，
前端程式碼完全不需要知道查詢服務部署在哪裡。
