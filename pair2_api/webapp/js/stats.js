// 統計儀表板入口：只負責初始化與協調各模組。

import { fetchStats, fetchStatsMeta } from "./stats/api.js";
import {
  bindStatsFormEvents,
  getCurrentStatsQuery,
  getStatsElements,
  setAvailableRange,
  switchStatsType
} from "./stats/forms.js";
import { validateStatsQuery } from "./stats/date-utils.js";
import {
  renderStatsResult,
  setDashboardError,
  setDashboardLoading,
  setDashboardWaiting,
  setSearchCompleted
} from "./stats/render.js";

const elements = getStatsElements();
let currentStatsType = "daily";

async function handleSearch() {
  const query = getCurrentStatsQuery(elements, currentStatsType);
  const errorMessage = validateStatsQuery(query);

  if (errorMessage) {
    alert(errorMessage);
    return;
  }

  setDashboardLoading();

  try {
    const data = await fetchStats(query);
    renderStatsResult(data);
  } catch (error) {
    console.error(error);
    setDashboardError(error.message);
  } finally {
    setSearchCompleted();
  }
}

function handleTypeChange(statsType) {
  currentStatsType = statsType;
  switchStatsType(elements, statsType);
  setDashboardWaiting();
}

async function initializeStatsPage() {
  bindStatsFormEvents(elements, {
    onTypeChange: handleTypeChange,
    onSearch: handleSearch
  });

  try {
    const meta = await fetchStatsMeta();
    setAvailableRange(elements, meta);
    setDashboardWaiting(
      meta.max_date
        ? `目前可查詢資料範圍：${meta.min_date} ～ ${meta.max_date}。`
        : undefined
    );
  } catch (error) {
    console.warn("無法取得統計資料範圍，改用預設日期。", error);
    setAvailableRange(elements);
    setDashboardWaiting("無法取得資料涵蓋範圍，仍可手動選擇日期後查詢。");
  }

  switchStatsType(elements, currentStatsType);
}

initializeStatsPage();
