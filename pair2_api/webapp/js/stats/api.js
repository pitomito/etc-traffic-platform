// 統一管理 HTTP 請求。

import { STATS_API_URL, STATS_META_URL } from "./config.js";

async function parseJsonResponse(response) {
  const text = await response.text();

  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`伺服器回傳內容不是合法 JSON：${text.slice(0, 300)}`);
  }
}

function getErrorMessage(data, status) {
  return data?.error || data?.message || data?.detail || `HTTP ${status}`;
}

export async function fetchStatsMeta() {
  const response = await fetch(STATS_META_URL);
  const data = await parseJsonResponse(response);

  if (!response.ok) {
    throw new Error(getErrorMessage(data, response.status));
  }

  return data;
}

export async function fetchStats(query) {
  const response = await fetch(STATS_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(query)
  });

  const data = await parseJsonResponse(response);

  if (!response.ok) {
    throw new Error(getErrorMessage(data, response.status));
  }

  return data;
}
