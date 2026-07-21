// 統計儀表板固定設定。

export const STATS_API_URL = "/api/stats";
export const STATS_META_URL = "/api/stats/meta";

export const VEHICLE_LABELS = {
  "31": "小客車",
  "32": "小貨車",
  "41": "大客車",
  "42": "大貨車",
  "5": "聯結車"
};

export const STATS_CONFIG = {
  daily: {
    title: "日流量查詢",
    hint: "請選擇欲查詢的日期。",
    trendTitle: "每小時車流量趨勢",
    trendDescription: "將當日全部門架與全部車種依一小時彙總。"
  },
  weekly: {
    title: "週流量查詢",
    hint: "請選擇週別，系統會以週日到週六作為查詢區間。",
    trendTitle: "每日車流量趨勢",
    trendDescription: "比較該週每日全部門架的累積通行量。"
  },
  monthly: {
    title: "月流量查詢",
    hint: "請選擇年份與月份。",
    trendTitle: "每日車流量趨勢",
    trendDescription: "比較該月份每日全部門架的累積通行量。"
  }
};

export const NUMBER_FORMATTER = new Intl.NumberFormat("zh-TW", {
  maximumFractionDigits: 0
});

export const PERCENT_FORMATTER = new Intl.NumberFormat("zh-TW", {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1
});
