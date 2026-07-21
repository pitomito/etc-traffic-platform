// M03A 資料解析、指標、圖表、表格

import { NUMBER_FORMATTER, VEHICLE_LABELS } from "./config.js";
import { state } from "./state.js";
import { findGantryGroupById, getGantryDirection, getGantryName } from "./gantries.js";
import {
  focusRankingMarker,
  renderGantryMarkers,
  renderRankingGantryMarkers,
  renderSelectedGantryMarker
} from "./map.js";
import {
  firstValue,
  formatDisplayTimestamp,
  getDatePart,
  getDirectionLabel,
  getQuerySpanDays,
  getVehicleLabel,
  parseNumericValue,
  sumBy
} from "./utils.js";
import {
  hideResultMessage,
  renderQuerySummary,
  showRawResult,
  showResultMessage
} from "./ui.js";

export function looksLikeM03ARow(row) {
  if (!row || typeof row !== "object" || Array.isArray(row)) {
    return false;
  }

  const keys = Object.keys(row).map(key => key.toLowerCase());
  return keys.some(key =>
    key === "volume" ||
    key === "flow" ||
    key === "trafficvolume" ||
    key === "traffic_volume" ||
    key === "traffic_count" ||
    key === "vehicle_count" ||
    (key.includes("traffic") && key.includes("volume"))
  );
}

export function findRowsInResponse(value, depth = 0) {
  if (depth > 4 || value === null || value === undefined) {
    return [];
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return [];
    }
    if (value.some(looksLikeM03ARow)) {
      return value;
    }
    return [];
  }

  if (typeof value !== "object") {
    return [];
  }

  const preferredKeys = ["rows", "data", "items", "results", "records", "result"];

  for (const key of preferredKeys) {
    if (Object.prototype.hasOwnProperty.call(value, key)) {
      const found = findRowsInResponse(value[key], depth + 1);
      if (found.length > 0 || Array.isArray(value[key])) {
        return found;
      }
    }
  }

  for (const nestedValue of Object.values(value)) {
    const found = findRowsInResponse(nestedValue, depth + 1);
    if (found.length > 0) {
      return found;
    }
  }

  return looksLikeM03ARow(value) ? [value] : [];
}

export function getM03AGantryRanking(rows, limit = 12) {
  const totals = new Map();

  rows.forEach(row => {
    const group = findGantryGroupById(row.gantryId);
    const key = group?.group_id || `${row.gantryId || row.gantryName}|||${row.direction}`;
    const current = totals.get(key) || {
      key,
      group,
      gantryId: group?.current_gantry_id || row.gantryId,
      name: group ? getGantryName(group) : row.gantryName,
      direction: group ? getGantryDirection(group) : row.direction,
      volume: 0
    };

    current.volume += row.volume;
    totals.set(key, current);
  });

  return Array.from(totals.values())
    .sort((a, b) => b.volume - a.volume)
    .slice(0, limit);
}

export function normalizeM03ARow(row) {
  const time = String(firstValue(row, [
    "time", "timestamp", "datetime", "date_time", "time_interval",
    "TimeInterval", "DataCollectTime", "start_time", "StartTime"
  ])).trim();

  const gantryId = String(firstValue(row, [
    "gantry_id", "gantry", "GantryID", "gantryId", "etc_gantry_id"
  ])).trim();

  const directionValue = String(firstValue(row, [
    "direction", "Direction", "dir"
  ]) || gantryId.slice(-1)).toUpperCase();

  const vehicleType = String(firstValue(row, [
    "vehicle_type", "vehicleType", "VehicleType", "vehicle", "car_type"
  ])).trim();

  const volume = parseNumericValue(firstValue(row, [
    "traffic_volume", "trafficVolume", "TrafficVolume", "volume",
    "count", "vehicle_count", "flow", "traffic_count"
  ]));

  const group = findGantryGroupById(gantryId);
  const gantryName = String(firstValue(row, [
    "gantry_name", "gantryName", "GantryName", "location", "road_section"
  ]) || getGantryName(group) || gantryId || "未標示門架");

  return {
    raw: row,
    time,
    gantryId,
    gantryName,
    direction: directionValue === "S" ? "S" : directionValue === "N" ? "N" : "",
    vehicleType,
    volume
  };
}

export function extractM03ARows(data) {
  return findRowsInResponse(data)
    .map(normalizeM03ARow)
    .filter(row => row.time || row.gantryId || row.vehicleType || row.volume !== 0);
}

export function getTrendBucket(timeValue, spanDays) {
  const normalized = String(timeValue).replace("T", " ");

  if (spanDays > 14) {
    return normalized.slice(0, 10);
  }

  if (spanDays > 2) {
    return `${normalized.slice(0, 13)}:00`;
  }

  return normalized.slice(0, 16);
}

export function getPeakTime(rows) {
  const byTime = sumBy(rows, row => row.time || "未標示時間");
  let peakTime = "";
  let peakVolume = 0;

  byTime.forEach((volume, time) => {
    if (volume > peakVolume) {
      peakTime = time;
      peakVolume = volume;
    }
  });

  return { peakTime, peakVolume };
}

export function renderM03AMetrics(rows) {
  const totalVolume = rows.reduce((sum, row) => sum + row.volume, 0);
  const uniqueDays = new Set(rows.map(row => getDatePart(row.time)).filter(Boolean));
  const averageDaily = uniqueDays.size > 0 ? totalVolume / uniqueDays.size : totalVolume;
  const uniqueGantries = new Set(rows.map(row => row.gantryId || row.gantryName).filter(Boolean));
  const { peakTime, peakVolume } = getPeakTime(rows);

  document.getElementById("metricTotalVolume").textContent = NUMBER_FORMATTER.format(totalVolume);
  document.getElementById("metricAverageDaily").textContent = NUMBER_FORMATTER.format(averageDaily);
  document.getElementById("metricGantryCount").textContent = NUMBER_FORMATTER.format(uniqueGantries.size);
  document.getElementById("metricPeakTime").textContent = formatDisplayTimestamp(peakTime);
  document.getElementById("metricPeakVolume").textContent = peakTime
    ? `${NUMBER_FORMATTER.format(peakVolume)} 輛`
    : "無法判斷";
  document.getElementById("metricDataNote").textContent = `${NUMBER_FORMATTER.format(rows.length)} 筆 API 資料`;

  document.getElementById("m03aMetricsSection").classList.remove("hidden");
}

export function destroyM03AChart() {
  if (state.m03aChartInstance) {
    state.m03aChartInstance.destroy();
    state.m03aChartInstance = null;
  }
}

export function buildChart(canvas, config) {
  destroyM03AChart();

  if (typeof Chart === "undefined") {
    document.getElementById("m03aChartDescription").textContent = "Chart.js 載入失敗，表格資料仍可正常使用。";
    return;
  }

  state.m03aChartInstance = new Chart(canvas, config);
}

export function renderM03AChart(rows, query) {
  const chartSection = document.getElementById("m03aChartSection");
  const canvas = document.getElementById("m03aChart");
  const chartTitle = document.getElementById("m03aChartTitle");
  const chartDescription = document.getElementById("m03aChartDescription");

  chartSection.classList.remove("hidden");

  if (!query.gantry_group_id) {
    const ranking = getM03AGantryRanking(rows, 12);

    chartTitle.textContent = "門架車流量排行";
    chartDescription.textContent = "未指定門架時，自動比較查詢範圍內流量最高的前 12 處門架。";

    buildChart(canvas, {
      type: "bar",
      data: {
        labels: ranking.map(item => item.name),
        datasets: [{
          label: "總車流量",
          data: ranking.map(item => item.volume),
          backgroundColor: "rgba(15, 118, 110, 0.72)",
          borderColor: "rgba(15, 118, 110, 1)",
          borderWidth: 1,
          borderRadius: 7
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: "y",
        onClick: (event, elements) => {
          if (elements.length > 0) {
            focusRankingMarker(ranking[elements[0].index]);
          }
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: context => `${NUMBER_FORMATTER.format(context.raw)} 輛`
            }
          }
        },
        scales: {
          x: {
            beginAtZero: true,
            ticks: {
              callback: value => NUMBER_FORMATTER.format(value)
            }
          }
        }
      }
    });
    return;
  }

  if (!query.vehicle_type) {
    const totals = sumBy(rows, row => row.vehicleType || "未分類");
    const entries = Array.from(totals.entries())
      .map(([vehicleType, volume]) => ({ vehicleType, volume }))
      .sort((a, b) => b.volume - a.volume);

    chartTitle.textContent = "各車種流量比較";
    chartDescription.textContent = "指定單一門架、未指定車種時，自動比較各車種在查詢期間的累積流量。";

    buildChart(canvas, {
      type: "bar",
      data: {
        labels: entries.map(item => VEHICLE_LABELS[item.vehicleType] || item.vehicleType || "未分類"),
        datasets: [{
          label: "總車流量",
          data: entries.map(item => item.volume),
          backgroundColor: [
            "rgba(15, 118, 110, 0.78)",
            "rgba(37, 99, 235, 0.72)",
            "rgba(245, 158, 11, 0.72)",
            "rgba(249, 115, 22, 0.72)",
            "rgba(100, 116, 139, 0.72)"
          ],
          borderWidth: 0,
          borderRadius: 8
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: context => `${NUMBER_FORMATTER.format(context.raw)} 輛`
            }
          }
        },
        scales: {
          y: {
            beginAtZero: true,
            ticks: {
              callback: value => NUMBER_FORMATTER.format(value)
            }
          }
        }
      }
    });
    return;
  }

  const spanDays = getQuerySpanDays(query);
  const totals = sumBy(rows, row => getTrendBucket(row.time, spanDays));
  const entries = Array.from(totals.entries())
    .map(([time, volume]) => ({ time, volume }))
    .sort((a, b) => a.time.localeCompare(b.time));

  const granularityLabel = spanDays > 14 ? "每日" : spanDays > 2 ? "每小時" : "每 5 分鐘";
  chartTitle.textContent = `${getVehicleLabel(query.vehicle_type)}流量趨勢`;
  chartDescription.textContent = `依查詢期間自動以${granularityLabel}彙整，避免時間跨度過長造成圖表過度擁擠。`;

  buildChart(canvas, {
    type: "line",
    data: {
      labels: entries.map(item => item.time.replaceAll("-", "/")),
      datasets: [{
        label: "車流量",
        data: entries.map(item => item.volume),
        borderColor: "rgba(15, 118, 110, 1)",
        backgroundColor: "rgba(15, 118, 110, 0.14)",
        borderWidth: 2,
        pointRadius: entries.length > 80 ? 0 : 2,
        pointHoverRadius: 5,
        tension: 0.22,
        fill: true
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: context => `${NUMBER_FORMATTER.format(context.raw)} 輛`
          }
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            callback: value => NUMBER_FORMATTER.format(value)
          }
        }
      }
    }
  });
}

export function clearTable() {
  document.getElementById("m03aTableHead").innerHTML = "";
  document.getElementById("m03aTableBody").innerHTML = "";
}

export function appendTableHeader(labels, centeredIndexes = []) {
  const head = document.getElementById("m03aTableHead");
  const row = document.createElement("tr");
  const centered = new Set(centeredIndexes);

  labels.forEach((label, index) => {
    const cell = document.createElement("th");
    cell.scope = "col";
    cell.textContent = label;

    if (centered.has(index)) {
      cell.classList.add("is-centered");
    }

    row.appendChild(cell);
  });

  head.appendChild(row);
}

export function appendTableRow(values, centeredIndexes = []) {
  const body = document.getElementById("m03aTableBody");
  const row = document.createElement("tr");
  const centered = new Set(centeredIndexes);

  values.forEach((value, index) => {
    const cell = document.createElement("td");
    cell.textContent = value;

    if (index > 0 && /^[-\d,%.]+(?: 輛)?$/.test(String(value))) {
      cell.classList.add("is-number");
    }

    if (centered.has(index)) {
      cell.classList.add("is-centered");
    }

    row.appendChild(cell);
  });

  body.appendChild(row);
}

export function renderM03ATable(rows, query) {
  const section = document.getElementById("m03aTableSection");
  const title = document.getElementById("m03aTableTitle");
  const description = document.getElementById("m03aTableDescription");
  const countBadge = document.getElementById("m03aTableCount");
  const totalVolume = rows.reduce((sum, row) => sum + row.volume, 0);

  clearTable();
  section.classList.remove("hidden");

  if (!query.gantry_group_id) {
    const entries = getM03AGantryRanking(rows, 50);

    title.textContent = "門架流量排行表";
    description.textContent = "未指定門架時，以累積流量由高至低排列，最多顯示前 50 名。";
    appendTableHeader(["排名", "門架位置", "方向", "總流量", "占查詢總量"], [0, 2, 3, 4]);

    entries.forEach((entry, index) => {
      const percentage = totalVolume > 0 ? `${(entry.volume / totalVolume * 100).toFixed(1)}%` : "0.0%";
      appendTableRow([
        String(index + 1),
        entry.name,
        getDirectionLabel(entry.direction).replace(/（.*）/, ""),
        NUMBER_FORMATTER.format(entry.volume),
        percentage
      ], [0, 2, 3, 4]);
    });

    countBadge.textContent = `${entries.length} 筆`;
    return;
  }

  if (!query.vehicle_type) {
    const uniqueDays = Math.max(1, new Set(rows.map(row => getDatePart(row.time)).filter(Boolean)).size);
    const totals = sumBy(rows, row => row.vehicleType || "未分類");
    const entries = Array.from(totals.entries())
      .map(([vehicleType, volume]) => ({ vehicleType, volume }))
      .sort((a, b) => b.volume - a.volume);

    title.textContent = "車種流量統計表";
    description.textContent = "指定門架、未指定車種時，呈現各車種總量、占比與平均每日流量。";
    appendTableHeader(["車種", "總流量", "占比", "平均每日流量"], [1, 2, 3]);

    entries.forEach(entry => {
      appendTableRow([
        getVehicleLabel(entry.vehicleType),
        NUMBER_FORMATTER.format(entry.volume),
        totalVolume > 0 ? `${(entry.volume / totalVolume * 100).toFixed(1)}%` : "0.0%",
        NUMBER_FORMATTER.format(entry.volume / uniqueDays)
      ], [1, 2, 3]);
    });

    countBadge.textContent = `${entries.length} 筆`;
    return;
  }

  const spanDays = getQuerySpanDays(query);
  const totals = sumBy(rows, row => getTrendBucket(row.time, spanDays));
  const entries = Array.from(totals.entries())
    .map(([time, volume]) => ({ time, volume }))
    .sort((a, b) => a.time.localeCompare(b.time))
    .slice(0, 200);

  title.textContent = "時間流量明細表";
  description.textContent = "指定單一門架與車種時，依圖表所採用的時間粒度彙整；最多顯示 200 筆。";
  appendTableHeader(["時間", "車流量"], [1]);

  entries.forEach(entry => {
    appendTableRow([
      entry.time.replace("T", " ").replaceAll("-", "/"),
      NUMBER_FORMATTER.format(entry.volume)
    ], [1]);
  });

  countBadge.textContent = `${entries.length} 筆`;
}

export function renderM03AResult(data, query) {
  renderQuerySummary(query);
  showRawResult(data);

  const rows = extractM03ARows(data);

  if (query.gantry_group_id) {
    renderSelectedGantryMarker(query, rows);
  } else if (rows.length > 0) {
    renderRankingGantryMarkers(getM03AGantryRanking(rows, 12));
  } else {
    renderGantryMarkers();
  }

  if (rows.length === 0) {
    document.getElementById("m03aMetricsSection").classList.add("hidden");
    document.getElementById("m03aChartSection").classList.add("hidden");
    document.getElementById("m03aTableSection").classList.add("hidden");
    showResultMessage("empty", "查詢成功，但 API 沒有回傳可辨識的 M03A 資料。請展開下方原始回傳內容確認欄位名稱或查詢條件。");
    return;
  }

  hideResultMessage();
  renderM03AMetrics(rows);
  renderM03AChart(rows, query);
  renderM03ATable(rows, query);
}
