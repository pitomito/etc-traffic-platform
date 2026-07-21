// 查詢頁固定設定。
// 頁面初始化入口。

export const QUERY_API_URL = "/api/query";
export const QUERY_META_URL = "/api/query/meta";

// 單次查詢最大跨度（天）：太大的範圍 Spark 掃不完會逾時，直接在送出前擋下
export const MAX_SPAN_DAYS = { M03A: 366, M06A: 92 };

export const TAIWAN_BOUNDS = [
  [21.7, 119.2],
  [25.7, 122.3]
];

export const MAP_OPTIONS = {
  center: [23.7, 121.0],
  zoom: 7,
  minZoom: 8,
  maxZoom: 18,
  maxBoundsViscosity: 1.0
};

export const VEHICLE_LABELS = {
  "31": "小客車",
  "32": "小貨車",
  "41": "大客車",
  "42": "大貨車",
  "5": "聯結車"
};

export const DATASET_HINTS = {
  M03A: "M03A：依時間區間、方向、門架位置與車種查詢各類車種通行量統計。方向、門架位置、車種皆可不選，代表全部。",
  M06A: "M06A：依時間區間、起始門架位置、終點門架位置與車種查詢旅次路徑原始資料。起訖門架位置本身已包含方向資訊。"
};

export const NUMBER_FORMATTER = new Intl.NumberFormat("zh-TW", {
  maximumFractionDigits: 0
});
