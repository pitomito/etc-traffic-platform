ETC 國道流量平台：query.js 模組化版本

一、請把本壓縮檔中的 js 資料夾內容放入專案的 js 資料夾：

js/
├─ query.js
└─ query/
   ├─ api.js
   ├─ config.js
   ├─ forms.js
   ├─ gantries.js
   ├─ m03a.js
   ├─ m06a.js
   ├─ map.js
   ├─ state.js
   ├─ ui.js
   └─ utils.js

二、query.html 最下方請保持 Leaflet、Chart.js 在前，並把 query.js 改為 module：

<script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script type="module" src="js/query.js"></script>

三、請透過 Flask 或 Live Server 開啟網站，不要直接雙擊 HTML 使用 file://。

四、Flask 目前的 /js/<path:filename> 路由可以載入 js/query/*.js，不必因模組化修改 app.py。

五、替換前請保留原本 query.js；backup 資料夾也附上本次拆分前的版本。

模組責任：
- config.js：固定設定
- state.js：共用狀態
- utils.js：純工具函式
- ui.js：共用結果畫面
- gantries.js：門架資料與下拉選單
- map.js：Leaflet 地圖
- m03a.js：M03A 資料解析、指標、圖表、表格
- m06a.js：M06A 資料解析、指標、圖表、表格
- api.js：API 請求與驗證
- forms.js：表單、查詢條件、事件綁定
- query.js：頁面初始化入口
