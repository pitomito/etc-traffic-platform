// API 請求與驗證

import { MAX_SPAN_DAYS, QUERY_API_URL, QUERY_META_URL } from "./config.js";
import { state } from "./state.js";
import { renderM03AResult } from "./m03a.js";
import { renderM06AResult } from "./m06a.js";
import {
  destroyM06ACharts,
  openResultDashboard,
  renderQuerySummary,
  setSearchButtonsDisabled,
  showRawResult,
  showResultMessage
} from "./ui.js";

export function validateQuery(query) {
  if (!query.start_time || !query.end_time) {
    return "請先選擇起始日期與終點日期。";
  }

  const startDate = new Date(query.start_time.replace(" ", "T"));
  const endDate = new Date(query.end_time.replace(" ", "T"));

  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
    return "時間格式無法辨識，請重新選擇日期與時間。";
  }

  if (endDate < startDate) {
    return "終點時間不能早於起始時間。";
  }

  // 查詢跨度上限：範圍太大 Spark 掃不完會逾時，直接在送出前擋下
  const spanDays = (endDate - startDate) / 86400000;
  const maxSpanDays = MAX_SPAN_DAYS[query.dataset];
  if (maxSpanDays && spanDays > maxSpanDays) {
    return `${query.dataset} 單次查詢最長 ${maxSpanDays} 天（約 ${Math.round(maxSpanDays / 30)} 個月），請縮短時間範圍、分次查詢。`;
  }

  // 資料涵蓋範圍檢查（範圍由 /api/query/meta 提供）
  const coverage = query.dataset === "M06A" ? state.queryMeta?.m06a : state.queryMeta?.m03a;
  if (coverage?.min_date && coverage?.max_date) {
    const queryStartDate = query.start_time.slice(0, 10);
    const queryEndDate = query.end_time.slice(0, 10);
    if (queryEndDate < coverage.min_date || queryStartDate > coverage.max_date) {
      return `${query.dataset} 資料目前僅涵蓋 ${coverage.min_date} ～ ${coverage.max_date}，你選的日期沒有資料，請調整查詢日期。`;
    }
  }

  if (query.dataset === "M06A" && (!query.start_gantry || !query.end_gantry)) {
    return "M06A 請選擇起始門架與終點門架。";
  }

  if (
    query.dataset === "M06A" &&
    query.start_gantry_group_id &&
    query.start_gantry_group_id === query.end_gantry_group_id
  ) {
    return "M06A 的起始門架與終點門架不可相同。";
  }

  if (
    query.dataset === "M06A" &&
    query.start_gantry &&
    query.end_gantry &&
    query.start_gantry.slice(-1) !== query.end_gantry.slice(-1)
  ) {
    return "M06A 的起始與終點門架方向必須相同（北上配北上、南下配南下），反向的旅次不存在。";
  }

  if (query.dataset === "M06A" && query.start_gantry && query.end_gantry) {
    const startMileage = Number((query.start_gantry.match(/^\d{2}F(\d{4})[NS]$/) || [])[1]);
    const endMileage = Number((query.end_gantry.match(/^\d{2}F(\d{4})[NS]$/) || [])[1]);
    const sameRoad = query.start_gantry.slice(0, 3) === query.end_gantry.slice(0, 3);
    const direction = query.start_gantry.slice(-1);

    if (sameRoad && Number.isFinite(startMileage) && Number.isFinite(endMileage)) {
      const wrongOrder = direction === "N"
        ? startMileage <= endMileage
        : startMileage >= endMileage;

      if (wrongOrder) {
        return "起訖順序與行車方向相反：北上是從里程大的門架開往里程小的門架（南下相反），請對調起點與終點。";
      }
    }
  }

  return "";
}

export async function loadQueryMeta() {
  try {
    const response = await fetch(QUERY_META_URL);
    const data = await parseJsonResponse(response);
    if (response.ok && data?.ok) {
      state.queryMeta = data;
    }
  } catch (error) {
    console.warn("無法取得資料涵蓋範圍，仍可查詢，但不會提前擋下超出範圍的日期。", error);
  }
  return state.queryMeta;
}

export async function parseJsonResponse(response) {
  const text = await response.text();

  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`回傳內容不是合法 JSON：${text.slice(0, 500)}`);
  }
}

export function getApiErrorMessage(data, responseStatus) {
  return data?.error || data?.message || data?.detail || `HTTP ${responseStatus}`;
}

export async function sendQueryToFlask(query) {
  const errorMessage = validateQuery(query);

  if (errorMessage) {
    alert(errorMessage);
    return;
  }

  setSearchButtonsDisabled(true);
  openResultDashboard();
  renderQuerySummary(query);

  [
    "m03aMetricsSection",
    "m03aChartSection",
    "m03aTableSection",
    "m06aMetricsSection",
    "m06aTrendSection",
    "m06aVehicleSection",
    "m06aTableSection",
    "rawResultDetails"
  ].forEach(id => document.getElementById(id).classList.add("hidden"));

  destroyM06ACharts();
  showResultMessage("loading", "查詢中，請稍後等待。");

  try {
    const response = await fetch(QUERY_API_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(query)
    });

    const data = await parseJsonResponse(response);

    if (!response.ok) {
      throw new Error(getApiErrorMessage(data, response.status));
    }

    console.log("回傳資料：", data);

    if (query.dataset === "M03A") {
      renderM03AResult(data, query);
    } else {
      renderM06AResult(data, query);
    }
  } catch (error) {
    console.error(error);
    showRawResult({
      ok: false,
      error: error.message,
      query
    });
    showResultMessage(
      "error",
      `查詢失敗：${error.message}`
    );
  } finally {
    setSearchButtonsDisabled(false);
  }
}
