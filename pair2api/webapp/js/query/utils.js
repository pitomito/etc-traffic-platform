import { VEHICLE_LABELS } from "./config.js";

export function getDirectionLabel(direction) {
  if (direction === "N") {
    return "北向（N）";
  }
  if (direction === "S") {
    return "南向（S）";
  }
  return "全部方向";
}

export function getVehicleLabel(vehicleType) {
  if (!vehicleType) {
    return "全部車種";
  }

  // 使用者介面只顯示中文車種名稱；資料庫代號仍保留在 value 與 API payload 中。
  return VEHICLE_LABELS[String(vehicleType)] || "其他車種";
}

export function formatQueryTime(value) {
  return value ? value.replaceAll("-", "/") : "--";
}

export function firstValue(object, keys) {
  for (const key of keys) {
    const value = object?.[key];
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }
  return "";
}

export function parseNumericValue(value) {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0;
  }

  if (typeof value === "string") {
    const parsed = Number(value.replaceAll(",", "").trim());
    return Number.isFinite(parsed) ? parsed : 0;
  }

  return 0;
}

export function getDatePart(timeValue) {
  const match = String(timeValue).match(/\d{4}-\d{2}-\d{2}/);
  return match ? match[0] : "";
}

export function formatDisplayTimestamp(timeValue) {
  if (!timeValue) {
    return "--";
  }

  return String(timeValue)
    .replace("T", " ")
    .replace(/:\d{2}(?:\.\d+)?(?:Z)?$/, match => match.startsWith(":") ? match : match)
    .slice(0, 16)
    .replaceAll("-", "/");
}

export function sumBy(rows, keyGetter) {
  const mapResult = new Map();

  rows.forEach(row => {
    const key = keyGetter(row);
    mapResult.set(key, (mapResult.get(key) || 0) + row.volume);
  });

  return mapResult;
}

export function getQuerySpanDays(query) {
  const startDate = new Date(query.start_time.replace(" ", "T"));
  const endDate = new Date(query.end_time.replace(" ", "T"));
  const milliseconds = Math.max(0, endDate - startDate);
  return milliseconds / 86400000;
}

export function parseDateTime(value) {
  if (!value) {
    return null;
  }

  const date = new Date(String(value).trim().replace(" ", "T"));
  return Number.isNaN(date.getTime()) ? null : date;
}

export function parseDurationMinutes(value) {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value !== "string" || !value.trim()) {
    return null;
  }

  const text = value.trim();

  if (/^\d+(?:\.\d+)?$/.test(text)) {
    return Number(text);
  }

  const match = text.match(/^(\d{1,3}):(\d{2})(?::(\d{2}))?$/);
  if (!match) {
    return null;
  }

  if (match[3] !== undefined) {
    return Number(match[1]) * 60 + Number(match[2]) + Number(match[3]) / 60;
  }

  return Number(match[1]) + Number(match[2]) / 60;
}

export function getQuerySpanHours(query) {
  const startDate = parseDateTime(query.start_time);
  const endDate = parseDateTime(query.end_time);

  if (!startDate || !endDate || endDate <= startDate) {
    return 1;
  }

  return Math.max((endDate - startDate) / 3600000, 1 / 12);
}
