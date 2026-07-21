// 統計期間與日期格式工具。

export function pad2(value) {
  return String(value).padStart(2, "0");
}

export function toDateInputValue(date) {
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`;
}

export function formatDisplayDate(date) {
  return `${date.getFullYear()}/${pad2(date.getMonth() + 1)}/${pad2(date.getDate())}`;
}

export function parseDateInput(value) {
  if (!value) {
    return null;
  }

  const [year, month, day] = value.split("-").map(Number);
  const date = new Date(year, month - 1, day);

  if (
    Number.isNaN(date.getTime()) ||
    date.getFullYear() !== year ||
    date.getMonth() !== month - 1 ||
    date.getDate() !== day
  ) {
    return null;
  }

  return date;
}

export function addDays(date, days) {
  const copiedDate = new Date(date);
  copiedDate.setDate(copiedDate.getDate() + days);
  return copiedDate;
}

export function getWeekRange(baseDate) {
  const dayOfWeek = baseDate.getDay();
  const startDate = addDays(baseDate, -dayOfWeek);
  const endDate = addDays(startDate, 6);
  return { startDate, endDate };
}

export function getMonthRange(yearValue, monthValue) {
  const year = Number(yearValue);
  const month = Number(monthValue);

  if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
    return null;
  }

  return {
    startDate: new Date(year, month - 1, 1),
    endDate: new Date(year, month, 0)
  };
}

export function formatDateRange(startDate, endDate) {
  return `${formatDisplayDate(startDate)} ～ ${formatDisplayDate(endDate)}`;
}

export function buildStatsQuery(statsType, elements) {
  if (statsType === "daily") {
    return {
      period_type: "daily",
      start_date: elements.dailyDate.value,
      end_date: elements.dailyDate.value
    };
  }

  if (statsType === "weekly") {
    const baseDate = parseDateInput(elements.weeklyBaseDate.value);
    const range = baseDate ? getWeekRange(baseDate) : null;

    return {
      period_type: "weekly",
      start_date: range ? toDateInputValue(range.startDate) : "",
      end_date: range ? toDateInputValue(range.endDate) : ""
    };
  }

  const range = getMonthRange(elements.monthlyYear.value, elements.monthlyMonth.value);

  return {
    period_type: "monthly",
    start_date: range ? toDateInputValue(range.startDate) : "",
    end_date: range ? toDateInputValue(range.endDate) : ""
  };
}

export function validateStatsQuery(query) {
  if (!query.start_date || !query.end_date) {
    return "請先選擇完整的統計期間。";
  }

  const startDate = parseDateInput(query.start_date);
  const endDate = parseDateInput(query.end_date);

  if (!startDate || !endDate) {
    return "統計期間格式錯誤，請重新選擇。";
  }

  if (endDate < startDate) {
    return "統計期間的終點不能早於起點。";
  }

  return "";
}
