import {
  NUMBER_FORMATTER,
  PERCENT_FORMATTER,
  STATS_CONFIG,
  VEHICLE_LABELS
} from "./config.js";

let trendChart = null;
let rankingChart = null;
let vehicleChart = null;

function destroyChart(chart) {
  if (chart) {
    chart.destroy();
  }
  return null;
}

function setText(id, value) {
  const element = document.getElementById(id);
  if (element) {
    element.textContent = value;
  }
}

function formatVolume(value) {
  return NUMBER_FORMATTER.format(Number(value) || 0);
}

function formatPercent(value) {
  return `${PERCENT_FORMATTER.format(Number(value) || 0)}%`;
}

function getVehicleLabel(vehicleType) {
  return VEHICLE_LABELS[String(vehicleType)] || String(vehicleType || "未分類");
}

export function setDashboardWaiting(message = "請選擇統計週期與時間條件後送出查詢。") {
  setText("totalFlow", "--");

  document.getElementById("statsStatus").className = "stats-status is-empty";
  setText("statsStatus", message);
  document.getElementById("statsResultContent").classList.add("hidden");

  trendChart = destroyChart(trendChart);
  rankingChart = destroyChart(rankingChart);
  vehicleChart = destroyChart(vehicleChart);
}

export function setDashboardLoading() {
  const status = document.getElementById("statsStatus");
  status.className = "stats-status is-loading";
  status.textContent = "統計中，正在彙整全部門架與全部車種資料……";
  document.getElementById("statsResultContent").classList.add("hidden");
  document.getElementById("statsSearchBtn").disabled = true;
  document.getElementById("statsSearchBtn").classList.add("is-loading");
}

export function setDashboardError(message) {
  const status = document.getElementById("statsStatus");
  status.className = "stats-status is-error";
  status.textContent = `查詢失敗：${message}`;
  document.getElementById("statsResultContent").classList.add("hidden");
}

export function setSearchCompleted() {
  document.getElementById("statsSearchBtn").disabled = false;
  document.getElementById("statsSearchBtn").classList.remove("is-loading");
}

function buildChart(canvasId, config, existingChart) {
  existingChart = destroyChart(existingChart);

  if (typeof Chart === "undefined") {
    throw new Error("Chart.js 載入失敗。請確認網路連線或 CDN 設定。");
  }

  return new Chart(document.getElementById(canvasId), config);
}

function renderTrendChart(data) {
  const periodType = data.period_type || "daily";
  const useHourly = periodType === "daily";
  const entries = useHourly ? data.hourly_series || [] : data.daily_series || [];

  setText("trendChartTitle", STATS_CONFIG[periodType]?.trendTitle || "車流量趨勢");
  setText("trendChartDescription", STATS_CONFIG[periodType]?.trendDescription || "");

  trendChart = buildChart("trendChart", {
    type: "line",
    data: {
      labels: entries.map(entry => useHourly
        ? String(entry.time).replaceAll("-", "/").slice(5, 16)
        : String(entry.date).replaceAll("-", "/").slice(5)
      ),
      datasets: [{
        label: "通行量",
        data: entries.map(entry => entry.volume),
        borderColor: "rgba(15, 118, 110, 1)",
        backgroundColor: "rgba(15, 118, 110, .12)",
        borderWidth: 2,
        pointRadius: entries.length > 40 ? 0 : 3,
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
            label: context => `${formatVolume(context.raw)} 輛`
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
  }, trendChart);
}

function renderRankingChart(data) {
  const entries = data.road_ranking || [];

  rankingChart = buildChart("rankingChart", {
    type: "bar",
    data: {
      labels: entries.map(entry => `${entry.name}｜${entry.direction_label || ""}`),
      datasets: [{
        label: "累積通行量",
        data: entries.map(entry => entry.volume),
        backgroundColor: "rgba(15, 118, 110, .74)",
        borderColor: "rgba(15, 118, 110, 1)",
        borderWidth: 1,
        borderRadius: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: "y",
      plugins: {
        legend: { display: false },
        tooltip: {
          titleFont: { size: 14 },
          bodyFont: { size: 14 },
          callbacks: {
            label: context => `${formatVolume(context.raw)} 輛`
          }
        }
      },
      scales: {
        x: {
          beginAtZero: true,
          ticks: {
            font: { size: 14 },
            callback: value => NUMBER_FORMATTER.format(value)
          }
        },
        y: {
          ticks: {
            autoSkip: false,
            font: { size: 14, weight: "600" }
          }
        }
      }
    }
  }, rankingChart);
}

function renderVehicleChart(data) {
  const entries = [...(data.vehicle_share || [])].sort((a, b) => {
    const volumeDifference = Number(b.volume || 0) - Number(a.volume || 0);
    if (volumeDifference !== 0) {
      return volumeDifference;
    }

    return String(a.vehicle_type || "").localeCompare(String(b.vehicle_type || ""));
  });

  const rankingColors = [
    "#0f3376",
    "#2a9d8f",
    "#e9c46a",
    "#f4a261",
    "#e76f51"
  ];

  const rankingHoverColors = [
    "#0f3376",
    "#21867c",
    "#d4aa3b",
    "#dc8741",
    "#cf573d"
  ];

  vehicleChart = buildChart("vehicleChart", {
    type: "doughnut",
    data: {
      labels: entries.map(entry => getVehicleLabel(entry.vehicle_type)),
      datasets: [{
        data: entries.map(entry => entry.volume),
        backgroundColor: entries.map((_, index) => rankingColors[index % rankingColors.length]),
        hoverBackgroundColor: entries.map((_, index) => rankingHoverColors[index % rankingHoverColors.length]),
        borderWidth: 2,
        borderColor: "#ffffff",
        hoverOffset: 8
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: "58%",
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            boxWidth: 14,
            padding: 18,
            usePointStyle: true,
            pointStyle: "circle",
            font: { size: 14, weight: "600" }
          }
        },
        tooltip: {
          titleFont: { size: 14 },
          bodyFont: { size: 14 },
          callbacks: {
            label: context => {
              const item = entries[context.dataIndex];
              const ranking = context.dataIndex + 1;
              return `第 ${ranking} 名｜${context.label}：${formatVolume(item.volume)} 輛（${formatPercent(item.share)}）`;
            }
          }
        }
      }
    }
  }, vehicleChart);
}

export function renderStatsResult(data) {
  const status = document.getElementById("statsStatus");
  const rowsAvailable = Number(data.row_count || 0) > 0;

  if (!rowsAvailable) {
    status.className = "stats-status is-empty";
    status.textContent = "查詢成功，但所選期間沒有可用的 M03A 資料。";
    document.getElementById("statsResultContent").classList.add("hidden");
    return;
  }

  const range = data.range || {};
  const totalVolume = data.summary?.total_volume ?? data.total_volume ?? 0;

  status.className = "stats-status is-success";
  status.textContent = `${range.label || "統計完成"}｜共彙整 ${formatVolume(data.row_count)} 筆 M03A 記錄`;

  setText("statsRangeLabel", range.label || "--");
  setText("totalFlow", formatVolume(totalVolume));

  renderTrendChart(data);
  renderRankingChart(data);
  renderVehicleChart(data);

  document.getElementById("statsResultContent").classList.remove("hidden");
}
