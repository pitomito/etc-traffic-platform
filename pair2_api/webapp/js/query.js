// 查詢頁入口：只負責初始化與模組協調。

// 模組責任：
// - config.js：固定設定
// - state.js：共用狀態
// - utils.js：純工具函式
// - ui.js：共用結果畫面
// - gantries.js：門架資料與下拉選單
// - map.js：Leaflet 地圖
// - m03a.js：M03A 資料解析、指標、圖表、表格
// - m06a.js：M06A 資料解析、指標、圖表、表格
// - api.js：API 請求與驗證
// - forms.js：表單、查詢條件、事件綁定
// - query.js：頁面初始化入口


import { state } from "./query/state.js";
import { applyDataCoverage, fillHourSelects, fillMinuteSelects, switchDataset, updateGantryLabels, bindQueryEvents } from "./query/forms.js";
import { loadGantries, populateAllGantrySelects } from "./query/gantries.js";
import { initializeMap, renderGantryMarkers } from "./query/map.js";
import { loadQueryMeta } from "./query/api.js";
import { showResultMessage } from "./query/ui.js";

async function initializeQueryPage() {
  try {
    initializeMap();
    updateGantryLabels();
    fillHourSelects();
    fillMinuteSelects();
    bindQueryEvents();
    switchDataset(document.getElementById("datasetType").value);

    await loadGantries();
    populateAllGantrySelects();
    renderGantryMarkers();

    await loadQueryMeta();
    applyDataCoverage();

    console.log("查詢頁模組初始化完成。", {
      gantryCount: state.gantryGroups.length
    });
  } catch (error) {
    console.error("查詢頁初始化失敗：", error);
    showResultMessage(
      "error",
      `查詢頁初始化失敗：${error.message}`
    );
  }
}

initializeQueryPage();
