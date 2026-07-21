import { state } from "./state.js";
import { formatQueryTime, getDirectionLabel, getVehicleLabel } from "./utils.js";

export function setSearchButtonsDisabled(isDisabled) {
  ["m03aSearchBtn", "m06aSearchBtn"].forEach(buttonId => {
    const button = document.getElementById(buttonId);
    button.disabled = isDisabled;
    button.classList.toggle("is-loading", isDisabled);
  });
}

export function destroyM06ACharts() {
  if (state.m06aTrendChartInstance) {
    state.m06aTrendChartInstance.destroy();
    state.m06aTrendChartInstance = null;
  }

  if (state.m06aVehicleChartInstance) {
    state.m06aVehicleChartInstance.destroy();
    state.m06aVehicleChartInstance = null;
  }
}

export function resetResultDashboard() {
  document.getElementById("resultEmptyState").classList.remove("hidden");
  document.getElementById("resultDashboard").classList.add("hidden");
  document.getElementById("resultStatus").className = "result-status hidden";

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

  if (state.m03aChartInstance) {
    state.m03aChartInstance.destroy();
    state.m03aChartInstance = null;
  }

  destroyM06ACharts();
}

export function openResultDashboard() {
  document.getElementById("resultEmptyState").classList.add("hidden");
  document.getElementById("resultDashboard").classList.remove("hidden");
}

export function showResultMessage(type, message) {
  openResultDashboard();
  const status = document.getElementById("resultStatus");
  status.className = `result-status is-${type}`;
  status.textContent = message;
}

export function hideResultMessage() {
  const status = document.getElementById("resultStatus");
  status.className = "result-status hidden";
  status.textContent = "";
}

export function showRawResult(data) {
  const details = document.getElementById("rawResultDetails");
  const pre = document.getElementById("rawResultText");
  pre.textContent = JSON.stringify(data, null, 2);
  details.classList.remove("hidden");
}

export function renderQuerySummary(query) {
  openResultDashboard();
  document.getElementById("querySummarySection").classList.remove("hidden");
  document.getElementById("summaryTime").textContent =
    `${formatQueryTime(query.start_time)} ～ ${formatQueryTime(query.end_time)}`;

  const directionLabel = document.getElementById("summaryDirectionLabel");
  const gantryLabel = document.getElementById("summaryGantryLabel");

  if (query.dataset === "M06A") {
    directionLabel.textContent = "起始門架";
    gantryLabel.textContent = "終點門架";
    document.getElementById("summaryDirection").textContent =
      query.start_gantry_name || query.start_gantry || "--";
    document.getElementById("summaryGantry").textContent =
      query.end_gantry_name || query.end_gantry || "--";
  } else {
    directionLabel.textContent = "方向";
    gantryLabel.textContent = "門架位置";
    const effectiveDirection = query.direction || query.gantry_direction || "";
    document.getElementById("summaryDirection").textContent = getDirectionLabel(effectiveDirection);
    document.getElementById("summaryGantry").textContent = query.gantry_name || "全部門架位置";
  }

  document.getElementById("summaryVehicle").textContent = getVehicleLabel(query.vehicle_type);
}
