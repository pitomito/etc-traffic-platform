import { STATS_CONFIG } from "./config.js";
import {
  buildStatsQuery,
  formatDateRange,
  getWeekRange,
  parseDateInput,
  pad2,
  toDateInputValue
} from "./date-utils.js";

export function getStatsElements() {
  return {
    tabButtons: document.querySelectorAll(".tab-btn"),
    filterGroups: document.querySelectorAll("[data-filter]"),
    filterTitle: document.getElementById("filterTitle"),
    filterHint: document.getElementById("filterHint"),
    statsSearchBtn: document.getElementById("statsSearchBtn"),
    dailyDate: document.getElementById("dailyDate"),
    weeklyBaseDate: document.getElementById("weeklyBaseDate"),
    weeklyRangeText: document.getElementById("weeklyRangeText"),
    monthlyYear: document.getElementById("monthlyYear"),
    monthlyMonth: document.getElementById("monthlyMonth")
  };
}

function fillMonthOptions(select) {
  select.innerHTML = "";

  for (let month = 1; month <= 12; month += 1) {
    const option = document.createElement("option");
    option.value = pad2(month);
    option.textContent = `${month} 月`;
    select.appendChild(option);
  }
}

function fillYearOptions(select, minYear, maxYear) {
  select.innerHTML = "";

  for (let year = maxYear; year >= minYear; year -= 1) {
    const option = document.createElement("option");
    option.value = String(year);
    option.textContent = `${year} 年`;
    select.appendChild(option);
  }
}

function updateWeeklyRange(elements) {
  const baseDate = parseDateInput(elements.weeklyBaseDate.value);
  elements.weeklyRangeText.value = baseDate
    ? formatDateRange(...Object.values(getWeekRange(baseDate)))
    : "";
}

export function setAvailableRange(elements, meta = {}) {
  const fallbackDate = new Date();
  fallbackDate.setDate(fallbackDate.getDate() - 3);

  const minDateValue = meta.min_date || "2016-01-01";
  const maxDateValue = meta.max_date || toDateInputValue(fallbackDate);
  const defaultDateValue = meta.default_date || maxDateValue;
  const minYear = Number(minDateValue.slice(0, 4)) || 2016;
  const maxYear = Number(maxDateValue.slice(0, 4)) || new Date().getFullYear();

  [elements.dailyDate, elements.weeklyBaseDate].forEach(input => {
    input.min = minDateValue;
    input.max = maxDateValue;
    input.value = defaultDateValue;
  });

  fillMonthOptions(elements.monthlyMonth);
  fillYearOptions(elements.monthlyYear, minYear, maxYear);
  elements.monthlyYear.value = defaultDateValue.slice(0, 4);
  elements.monthlyMonth.value = defaultDateValue.slice(5, 7);

  updateWeeklyRange(elements);
}

export function switchStatsType(elements, statsType) {
  elements.tabButtons.forEach(button => {
    button.classList.toggle("active", button.dataset.type === statsType);
    button.setAttribute("aria-selected", button.dataset.type === statsType ? "true" : "false");
  });

  elements.filterGroups.forEach(group => {
    group.classList.toggle("hidden", group.dataset.filter !== statsType);
  });

  elements.filterTitle.textContent = STATS_CONFIG[statsType].title;
  elements.filterHint.textContent = STATS_CONFIG[statsType].hint;
}

export function getCurrentStatsQuery(elements, statsType) {
  return buildStatsQuery(statsType, elements);
}

export function bindStatsFormEvents(elements, callbacks) {
  elements.tabButtons.forEach(button => {
    button.addEventListener("click", () => callbacks.onTypeChange(button.dataset.type));
  });

  elements.weeklyBaseDate.addEventListener("change", () => updateWeeklyRange(elements));

  document.getElementById("statsFilterForm").addEventListener("submit", event => {
    event.preventDefault();
    callbacks.onSearch();
  });
}
