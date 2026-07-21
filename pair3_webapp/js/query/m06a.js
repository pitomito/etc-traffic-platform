// M06A 資料解析、指標、圖表、表格

import { NUMBER_FORMATTER, VEHICLE_LABELS } from "./config.js";
import { state } from "./state.js";
import { renderM06ARouteMap } from "./map.js";
import {
  firstValue,
  getQuerySpanDays,
  getQuerySpanHours,
  getVehicleLabel,
  parseDateTime,
  parseDurationMinutes,
  parseNumericValue
} from "./utils.js";
import {
  hideResultMessage,
  renderQuerySummary,
  showRawResult,
  showResultMessage
} from "./ui.js";

export function looksLikeM06ARow(row) {
  if (!row || typeof row !== "object" || Array.isArray(row)) {
    return false;
  }

  const keys = Object.keys(row).map(key => key.toLowerCase());

  return keys.some(key =>
    key.includes("trip") ||
    key.includes("gantry_o") ||
    key.includes("gantry_d") ||
    key.includes("origin") ||
    key.includes("destination") ||
    key.includes("detectiontime_o") ||
    key.includes("detectiontime_d") ||
    key === "start_gantry" ||
    key === "end_gantry"
  );
}

export function findM06ARowsInResponse(value, depth = 0) {
  if (depth > 5 || value === null || value === undefined) {
    return [];
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return [];
    }

    if (value.some(looksLikeM06ARow)) {
      return value.filter(item => item && typeof item === "object");
    }

    return [];
  }

  if (typeof value !== "object") {
    return [];
  }

  const preferredKeys = ["rows", "data", "items", "results", "records", "result", "trips"];

  for (const key of preferredKeys) {
    if (Object.prototype.hasOwnProperty.call(value, key)) {
      const found = findM06ARowsInResponse(value[key], depth + 1);
      if (found.length > 0 || Array.isArray(value[key])) {
        return found;
      }
    }
  }

  for (const nestedValue of Object.values(value)) {
    const found = findM06ARowsInResponse(nestedValue, depth + 1);
    if (found.length > 0) {
      return found;
    }
  }

  return looksLikeM06ARow(value) ? [value] : [];
}

export function normalizeM06ARow(row, query) {
  const originTime = String(firstValue(row, [
    "origin_time", "start_time", "entry_time", "time_o", "Time_O",
    "DetectionTime_O", "detection_time_o", "trip_start_time",
    "timestamp", "time"
  ])).trim();

  const destinationTime = String(firstValue(row, [
    "destination_time", "end_time", "exit_time", "time_d", "Time_D",
    "DetectionTime_D", "detection_time_d", "trip_end_time"
  ])).trim();

  const startGantryId = String(firstValue(row, [
    "start_gantry", "gantry_o", "GantryID_O", "origin_gantry",
    "gantry_from", "start_gantry_id"
  ]) || query.start_gantry || "").trim();

  const endGantryId = String(firstValue(row, [
    "end_gantry", "gantry_d", "GantryID_D", "destination_gantry",
    "gantry_to", "end_gantry_id"
  ]) || query.end_gantry || "").trim();

  const vehicleType = String(firstValue(row, [
    "vehicle_type", "vehicleType", "VehicleType", "car_type", "vehicle"
  ]) || query.vehicle_type || "").trim();

  const rawTripCount = firstValue(row, [
    "trip_count", "tripCount", "count", "vehicle_count", "total_trips", "volume"
  ]);
  const tripCount = rawTripCount === "" ? 1 : Math.max(0, parseNumericValue(rawTripCount));

  let travelTimeMinutes = parseDurationMinutes(firstValue(row, [
    "avg_travel_time_minutes", "travel_time_minutes", "travel_time",
    "trip_time_minutes", "duration_minutes", "avg_duration_minutes"
  ]));

  const durationSeconds = firstValue(row, [
    "travel_time_seconds", "trip_time_seconds", "duration_seconds"
  ]);
  if (travelTimeMinutes === null && durationSeconds !== "") {
    travelTimeMinutes = parseNumericValue(durationSeconds) / 60;
  }

  if (travelTimeMinutes === null) {
    const startDate = parseDateTime(originTime);
    const endDate = parseDateTime(destinationTime);
    if (startDate && endDate && endDate >= startDate) {
      travelTimeMinutes = (endDate - startDate) / 60000;
    }
  }

  return {
    raw: row,
    originTime,
    destinationTime,
    startGantryId,
    endGantryId,
    vehicleType,
    tripCount,
    travelTimeMinutes
  };
}

export function extractM06ARows(data, query) {
  return findM06ARowsInResponse(data)
    .map(row => normalizeM06ARow(row, query))
    .filter(row => row.tripCount > 0);
}

export function getM06AGranularity(query) {
  const spanDays = getQuerySpanDays(query);

  if (spanDays <= .5) {
    return { type: "quarterHour", label: "每 15 分鐘" };
  }

  if (spanDays <= 7) {
    return { type: "hour", label: "每小時" };
  }

  if (spanDays <= 90) {
    return { type: "day", label: "每日" };
  }

  return { type: "month", label: "每月" };
}

export function getM06ATimeBucket(timeValue, granularity) {
  const date = parseDateTime(timeValue);
  if (!date) {
    return String(timeValue || "未標示時間");
  }

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hour = String(date.getHours()).padStart(2, "0");
  const minute = date.getMinutes();

  if (granularity.type === "quarterHour") {
    const bucketMinute = String(Math.floor(minute / 15) * 15).padStart(2, "0");
    return `${year}-${month}-${day} ${hour}:${bucketMinute}`;
  }

  if (granularity.type === "hour") {
    return `${year}-${month}-${day} ${hour}:00`;
  }

  if (granularity.type === "day") {
    return `${year}-${month}-${day}`;
  }

  return `${year}-${month}`;
}

export function getM06ATotalsByVehicle(rows) {
  const totals = new Map();

  rows.forEach(row => {
    const key = row.vehicleType || "未分類";
    totals.set(key, (totals.get(key) || 0) + row.tripCount);
  });

  return totals;
}

export function getM06ATotalsByTime(rows, query) {
  const granularity = getM06AGranularity(query);
  const totals = new Map();

  rows.forEach(row => {
    const bucket = getM06ATimeBucket(row.originTime || query.start_time, granularity);
    const vehicleType = row.vehicleType || "未分類";

    if (!totals.has(bucket)) {
      totals.set(bucket, new Map());
    }

    const bucketMap = totals.get(bucket);
    bucketMap.set(vehicleType, (bucketMap.get(vehicleType) || 0) + row.tripCount);
  });

  return {
    granularity,
    entries: Array.from(totals.entries())
      .map(([time, vehicleMap]) => ({
        time,
        vehicleMap,
        total: Array.from(vehicleMap.values()).reduce((sum, value) => sum + value, 0)
      }))
      .sort((a, b) => a.time.localeCompare(b.time))
  };
}

export function getWeightedAverageTravelTime(rows) {
  let weightedTotal = 0;
  let totalCount = 0;

  rows.forEach(row => {
    if (Number.isFinite(row.travelTimeMinutes) && row.travelTimeMinutes >= 0) {
      weightedTotal += row.travelTimeMinutes * row.tripCount;
      totalCount += row.tripCount;
    }
  });

  return totalCount > 0 ? weightedTotal / totalCount : null;
}

export function renderM06AMetrics(rows, query) {
  const totalTrips = rows.reduce((sum, row) => sum + row.tripCount, 0);
  const averageHourly = totalTrips / getQuerySpanHours(query);
  const { entries, granularity } = getM06ATotalsByTime(rows, query);
  const peak = entries.reduce(
    (best, entry) => entry.total > best.total ? entry : best,
    { time: "", total: 0 }
  );

  const vehicleTotals = Array.from(getM06ATotalsByVehicle(rows).entries())
    .map(([vehicleType, count]) => ({ vehicleType, count }))
    .sort((a, b) => b.count - a.count);
  const mainVehicle = vehicleTotals[0] || { vehicleType: "", count: 0 };
  const mainShare = totalTrips > 0 ? mainVehicle.count / totalTrips * 100 : 0;
  const averageTravelTime = getWeightedAverageTravelTime(rows);

  document.getElementById("m06aMetricTotalTrips").textContent =
    NUMBER_FORMATTER.format(totalTrips);
  document.getElementById("m06aMetricAverageHourly").textContent =
    NUMBER_FORMATTER.format(averageHourly);
  document.getElementById("m06aMetricPeakTime").textContent =
    peak.time ? peak.time.replaceAll("-", "/") : "--";
  document.getElementById("m06aMetricPeakTrips").textContent =
    peak.time ? `${NUMBER_FORMATTER.format(peak.total)} 輛（${granularity.label}）` : "無法判斷";
  document.getElementById("m06aMetricMainVehicle").textContent =
    getVehicleLabel(mainVehicle.vehicleType).replace(/（.*）/, "");
  document.getElementById("m06aMetricMainVehicleShare").textContent =
    `${mainShare.toFixed(1)}%｜${NUMBER_FORMATTER.format(mainVehicle.count)} 輛`;

  const travelTimeText = averageTravelTime === null
    ? ""
    : `｜平均旅行時間 ${averageTravelTime.toFixed(1)} 分鐘`;

  document.getElementById("m06aMetricDataNote").textContent =
    `${NUMBER_FORMATTER.format(rows.length)} 筆 API 資料${travelTimeText}`;
  document.getElementById("m06aMetricsSection").classList.remove("hidden");
}

export function buildM06ATrendChart(canvas, config) {
  if (state.m06aTrendChartInstance) {
    state.m06aTrendChartInstance.destroy();
  }

  if (typeof Chart === "undefined") {
    document.getElementById("m06aTrendDescription").textContent =
      "Chart.js 載入失敗，統計表格仍可正常使用。";
    return;
  }

  state.m06aTrendChartInstance = new Chart(canvas, config);
}

export function buildM06AVehicleChart(canvas, config) {
  if (state.m06aVehicleChartInstance) {
    state.m06aVehicleChartInstance.destroy();
  }

  if (typeof Chart === "undefined") {
    return;
  }

  state.m06aVehicleChartInstance = new Chart(canvas, config);
}

export function renderM06ATrendChart(rows, query) {
  const section = document.getElementById("m06aTrendSection");
  const title = document.getElementById("m06aTrendTitle");
  const description = document.getElementById("m06aTrendDescription");
  const canvas = document.getElementById("m06aTrendChart");
  const { entries, granularity } = getM06ATotalsByTime(rows, query);

  section.classList.remove("hidden");
  title.textContent = query.vehicle_type
    ? `${getVehicleLabel(query.vehicle_type)}區間旅次趨勢`
    : "區間旅次時間趨勢";
  description.textContent =
    `依查詢期間自動以${granularity.label}彙整，呈現通過起訖門架區間的旅次變化。`;

  const labels = entries.map(entry => entry.time.replaceAll("-", "/"));

  if (query.vehicle_type) {
    buildM06ATrendChart(canvas, {
      type: "line",
      data: {
        labels,
        datasets: [{
          label: "旅次數",
          data: entries.map(entry => entry.total),
          borderColor: "rgba(15, 118, 110, 1)",
          backgroundColor: "rgba(15, 118, 110, .14)",
          borderWidth: 2,
          pointRadius: entries.length > 80 ? 0 : 2,
          pointHoverRadius: 5,
          tension: .22,
          fill: true
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
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
            ticks: { callback: value => NUMBER_FORMATTER.format(value) }
          }
        }
      }
    });
    return;
  }

  const vehicleTypes = Array.from(getM06ATotalsByVehicle(rows).keys())
    .sort((a, b) => {
      const order = ["31", "32", "41", "42", "5"];
      return order.indexOf(a) - order.indexOf(b);
    });

  const colors = [
    "rgba(15, 118, 110, .80)",
    "rgba(37, 99, 235, .72)",
    "rgba(245, 158, 11, .75)",
    "rgba(249, 115, 22, .75)",
    "rgba(100, 116, 139, .75)"
  ];

  buildM06ATrendChart(canvas, {
    type: "bar",
    data: {
      labels,
      datasets: vehicleTypes.map((vehicleType, index) => ({
        label: VEHICLE_LABELS[vehicleType] || vehicleType || "未分類",
        data: entries.map(entry => entry.vehicleMap.get(vehicleType) || 0),
        backgroundColor: colors[index % colors.length],
        borderWidth: 0,
        borderRadius: 4
      }))
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        tooltip: {
          callbacks: {
            label: context =>
              `${context.dataset.label}：${NUMBER_FORMATTER.format(context.raw)} 輛`
          }
        }
      },
      scales: {
        x: { stacked: true },
        y: {
          stacked: true,
          beginAtZero: true,
          ticks: { callback: value => NUMBER_FORMATTER.format(value) }
        }
      }
    }
  });
}

export function renderM06AVehicleChart(rows, query) {
  const section = document.getElementById("m06aVehicleSection");

  if (query.vehicle_type) {
    section.classList.add("hidden");
    if (state.m06aVehicleChartInstance) {
      state.m06aVehicleChartInstance.destroy();
      state.m06aVehicleChartInstance = null;
    }
    return;
  }

  const entries = Array.from(getM06ATotalsByVehicle(rows).entries())
    .map(([vehicleType, count]) => ({ vehicleType, count }))
    .sort((a, b) => b.count - a.count);

  section.classList.remove("hidden");

  buildM06AVehicleChart(document.getElementById("m06aVehicleChart"), {
    type: "bar",
    data: {
      labels: entries.map(entry =>
        VEHICLE_LABELS[entry.vehicleType] || entry.vehicleType || "未分類"
      ),
      datasets: [{
        label: "旅次數",
        data: entries.map(entry => entry.count),
        backgroundColor: [
          "rgba(15, 118, 110, .80)",
          "rgba(37, 99, 235, .72)",
          "rgba(245, 158, 11, .75)",
          "rgba(249, 115, 22, .75)",
          "rgba(100, 116, 139, .75)"
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
          ticks: { callback: value => NUMBER_FORMATTER.format(value) }
        }
      }
    }
  });
}

export function clearM06ATable() {
  document.getElementById("m06aTableHead").innerHTML = "";
  document.getElementById("m06aTableBody").innerHTML = "";
}

export function appendM06ATableHeader(labels) {
  const row = document.createElement("tr");

  labels.forEach(label => {
    const cell = document.createElement("th");
    cell.scope = "col";
    cell.textContent = label;
    row.appendChild(cell);
  });

  document.getElementById("m06aTableHead").appendChild(row);
}

export function appendM06ATableRow(values) {
  const row = document.createElement("tr");

  values.forEach((value, index) => {
    const cell = document.createElement("td");
    cell.textContent = value;
    if (index > 0 && /^[-\d,.%]+(?: 輛| 分鐘)?$/.test(String(value))) {
      cell.classList.add("is-number");
    }
    row.appendChild(cell);
  });

  document.getElementById("m06aTableBody").appendChild(row);
}

export function renderM06ATable(rows, query) {
  const section = document.getElementById("m06aTableSection");
  const title = document.getElementById("m06aTableTitle");
  const description = document.getElementById("m06aTableDescription");
  const countBadge = document.getElementById("m06aTableCount");
  const { entries, granularity } = getM06ATotalsByTime(rows, query);

  clearM06ATable();
  section.classList.remove("hidden");

  if (query.vehicle_type) {
    title.textContent = "時間旅次統計表";
    description.textContent =
      `指定單一車種時，依${granularity.label}呈現旅次數；最多顯示 200 筆。`;
    appendM06ATableHeader(["時間區間", "旅次數"]);

    entries.slice(0, 200).forEach(entry => {
      appendM06ATableRow([
        entry.time.replaceAll("-", "/"),
        NUMBER_FORMATTER.format(entry.total)
      ]);
    });

    countBadge.textContent = `${Math.min(entries.length, 200)} 筆`;
    return;
  }

  const vehicleTypes = Array.from(getM06ATotalsByVehicle(rows).keys())
    .sort((a, b) => {
      const order = ["31", "32", "41", "42", "5"];
      return order.indexOf(a) - order.indexOf(b);
    });

  title.textContent = "各時段車種旅次統計表";
  description.textContent =
    `依${granularity.label}彙整各車種通過此起訖區間的旅次數；最多顯示 200 筆。`;
  appendM06ATableHeader([
    "時間區間",
    ...vehicleTypes.map(type => VEHICLE_LABELS[type] || type || "未分類"),
    "合計"
  ]);

  entries.slice(0, 200).forEach(entry => {
    appendM06ATableRow([
      entry.time.replaceAll("-", "/"),
      ...vehicleTypes.map(type => NUMBER_FORMATTER.format(entry.vehicleMap.get(type) || 0)),
      NUMBER_FORMATTER.format(entry.total)
    ]);
  });

  countBadge.textContent = `${Math.min(entries.length, 200)} 筆`;
}

export function renderM06AResult(data, query) {
  renderQuerySummary(query);
  showRawResult(data);
  renderM06ARouteMap(query, data);

  const rows = extractM06ARows(data, query);

  [
    "m03aMetricsSection",
    "m03aChartSection",
    "m03aTableSection"
  ].forEach(id => document.getElementById(id).classList.add("hidden"));

  if (rows.length === 0) {
    [
      "m06aMetricsSection",
      "m06aTrendSection",
      "m06aVehicleSection",
      "m06aTableSection"
    ].forEach(id => document.getElementById(id).classList.add("hidden"));

    showResultMessage(
      "empty",
      "查詢成功，但 API 沒有回傳可辨識的 M06A 旅次資料。請展開原始回傳內容，確認欄位是否包含起始時間、車種與旅次數。"
    );
    return;
  }

  hideResultMessage();
  renderM06AMetrics(rows, query);
  renderM06ATrendChart(rows, query);
  renderM06AVehicleChart(rows, query);
  renderM06ATable(rows, query);
}
