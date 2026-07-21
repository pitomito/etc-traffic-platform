// Leaflet 地圖

import { MAP_OPTIONS, NUMBER_FORMATTER, TAIWAN_BOUNDS } from "./config.js";
import { state } from "./state.js";
import {
  findGantryGroupById,
  getGantryDirection,
  getGantryLatLng,
  getGantryName,
  getOrderedRouteGroupsBetween
} from "./gantries.js";
import { getDirectionLabel } from "./utils.js";

export function initializeMap() {
  if (state.map) {
    return state.map;
  }

  if (typeof L === "undefined") {
    throw new Error("Leaflet 尚未載入，請確認 query.html 中的 Leaflet script 位於 query.js 之前。");
  }

  const taiwanBounds = L.latLngBounds(TAIWAN_BOUNDS);

  state.map = L.map("map", {
    ...MAP_OPTIONS,
    maxBounds: taiwanBounds
  });

  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors",
    noWrap: true
  }).addTo(state.map);

  state.gantryLayer = L.layerGroup().addTo(state.map);
  state.selectionLayer = L.layerGroup().addTo(state.map);

  state.map.on("zoomend", () => {
    updateAllGantryMarkerOffsets();
    updateRankingMarkerOffsets();
    updateM06AEndpointOffsets();
  });

  return state.map;
}

export function setMapDescription(message) {
  const description = document.querySelector(".query-map-panel .panel-title-row p");
  if (description) {
    description.textContent = message;
  }
}

export function setMapLegend(items) {
  const legend = document.querySelector(".query-map-panel .map-legend");
  if (!legend) {
    return;
  }

  legend.innerHTML = items.map(item => (
    `<span class="legend-dot" style="background:${item.color}"></span>${item.label}`
  )).join(" ");
}

export function resetMapLegend() {
  setMapLegend([
    { color: "#2563eb", label: "北向" },
    { color: "#f97316", label: "南向" }
  ]);
}

export function getAllGantryMarkerOffset(index, total, direction) {
  if (total <= 1) {
    return L.point(0, 0);
  }

  // 最常見的情況是同一位置各有一個北向與南向門架。
  // 北向往左、南向往右，讓兩種顏色同時可見。
  if (total === 2) {
    if (direction === "N") {
      return L.point(-4, 0);
    }
    if (direction === "S") {
      return L.point(4, 0);
    }
    return L.point(index === 0 ? -4 : 4, 0);
  }

  // 極少數同座標超過兩筆時，以小圓周均勻分散。
  const distance = 5;
  const angle = -Math.PI / 2 + (index * Math.PI * 2) / total;

  return L.point(
    Math.round(Math.cos(angle) * distance),
    Math.round(Math.sin(angle) * distance)
  );
}

export function renderGantryMarkers() {
  state.allGantryMarkerOffsetEntries = [];
  state.rankingMarkerOffsetEntries = [];
  state.m06aEndpointOffsetEntries = [];
  state.gantryLayer.clearLayers();
  state.selectionLayer?.clearLayers();
  state.highlightedGantryMarkers.clear();

  const bounds = [];
  const locationGroups = new Map();

  state.gantryGroups.forEach(gantryGroup => {
    const latLng = getGantryLatLng(gantryGroup);
    if (!latLng) {
      return;
    }

    const locationKey = getRankingLocationKey(latLng);
    if (!locationGroups.has(locationKey)) {
      locationGroups.set(locationKey, []);
    }

    locationGroups.get(locationKey).push({
      gantryGroup,
      baseLatLng: L.latLng(latLng),
      direction: getGantryDirection(gantryGroup)
    });

    bounds.push(latLng);
  });

  locationGroups.forEach(items => {
    // 固定北向先、南向後，確保每次重新整理的偏移方向一致。
    items.sort((a, b) => {
      const directionOrder = { N: 0, S: 1 };
      return (directionOrder[a.direction] ?? 2) - (directionOrder[b.direction] ?? 2);
    });

    items.forEach((item, index) => {
      const color = item.direction === "S" ? "#f97316" : "#2563eb";
      const gantryName = getGantryName(item.gantryGroup);
      const pixelOffset = getAllGantryMarkerOffset(
        index,
        items.length,
        item.direction
      );
      const displayLatLng = offsetLatLngByPixels(item.baseLatLng, pixelOffset);

      const marker = L.circleMarker(displayLatLng, {
        radius: 3,
        color,
        weight: 1.4,
        fillColor: color,
        fillOpacity: 0.78
      });

      marker.bindTooltip(gantryName, {
        className: "gantry-tooltip",
        sticky: true,
        direction: "top"
      });

      // M06A 用：點門架選起訖（實際邏輯在 forms.js 監聽，避免模組循環依賴）
      marker.on("click", () => {
        document.dispatchEvent(new CustomEvent("gantry-map-click", {
          detail: { groupId: item.gantryGroup.group_id }
        }));
      });

      marker.addTo(state.gantryLayer);

      state.allGantryMarkerOffsetEntries.push({
        marker,
        baseLatLng: item.baseLatLng,
        pixelOffset
      });
    });
  });

  setMapDescription("一進入頁面即顯示全部門架位置；同座標的南北向門架會稍微錯開，送出查詢後則切換為指定門架或流量排行門架。");
  resetMapLegend();

  if (bounds.length > 0) {
    state.map.fitBounds(bounds, { padding: [28, 28], maxZoom: 8 });
  }

  updateAllGantryMarkerOffsets();
}

export function createRankingIcon(rank, color) {
  const size = rank === 1 ? 34 : rank === 2 ? 32 : rank === 3 ? 30 : 26;

  return L.divIcon({
    className: "",
    html: `<div style="
      width:${size}px;
      height:${size}px;
      display:flex;
      align-items:center;
      justify-content:center;
      border-radius:50%;
      color:#fff;
      background:${color};
      border:3px solid rgba(255,255,255,.96);
      box-shadow:0 3px 10px rgba(15,23,42,.34);
      font-size:${rank <= 3 ? 13 : 11}px;
      font-weight:900;
      line-height:1;
    ">${rank}</div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
    tooltipAnchor: [0, -(size / 2 + 4)]
  });
}

export function renderSelectedGantryMarker(query, rows) {
  state.allGantryMarkerOffsetEntries = [];
  state.rankingMarkerOffsetEntries = [];
  state.m06aEndpointOffsetEntries = [];

  const selectedGroup = state.gantryGroups.find(group => group.group_id === query.gantry_group_id)
    || findGantryGroupById(query.gantry_id)
    || findGantryGroupById(rows[0]?.gantryId);

  const latLng = getGantryLatLng(selectedGroup);
  if (!selectedGroup || !latLng) {
    renderGantryMarkers();
    setMapDescription("已指定門架，但門架群組資料沒有可用經緯度，因此暫時維持全部門架地圖。");
    return;
  }

  state.gantryLayer.clearLayers();
  state.selectionLayer?.clearLayers();
  state.highlightedGantryMarkers.clear();

  const totalVolume = rows.reduce((sum, row) => sum + row.volume, 0);
  const name = getGantryName(selectedGroup);
  const marker = L.circleMarker(latLng, {
    radius: 10,
    color: "#991b1b",
    weight: 3,
    fillColor: "#ef4444",
    fillOpacity: 0.94
  });

  marker.bindTooltip(
    `<strong>${name}</strong><br>本次總流量：${NUMBER_FORMATTER.format(totalVolume)} 輛`,
    {
      className: "gantry-tooltip",
      permanent: true,
      direction: "top",
      offset: [0, -8]
    }
  );

  marker.addTo(state.gantryLayer);
  state.highlightedGantryMarkers.set(selectedGroup.group_id, marker);
  state.map.setView(latLng, 13, { animate: true });

  setMapDescription("本次指定單一門架，地圖僅保留該門架；紅色標記代表查詢位置。");
  setMapLegend([{ color: "#ef4444", label: "指定門架" }]);
}

export function getRankingMarkerColor(rank) {
  if (rank === 1) return "#d4a017";
  if (rank === 2) return "#8b95a5";
  if (rank === 3) return "#b87333";
  return "#0f766e";
}

export function getRankingLocationKey(latLng) {
  /*
    五位小數約為 1 公尺尺度。
    南北向門架若共用同一組座標，會被視為同一個碰撞群組。
  */
  return `${Number(latLng[0]).toFixed(5)},${Number(latLng[1]).toFixed(5)}`;
}

export function getCollisionOffset(index, total) {
  if (index === 0 || total <= 1) {
    return L.point(0, 0);
  }

  /*
    最高名次固定留在真實位置。
    其餘標記沿圓周分散，避免第 2、3 名蓋住第 1 名。
  */
  const distance = 40;
  const movableCount = total - 1;
  const angle = -Math.PI / 2 + ((index - 1) * Math.PI * 2) / movableCount;

  return L.point(
    Math.round(Math.cos(angle) * distance),
    Math.round(Math.sin(angle) * distance)
  );
}

export function offsetLatLngByPixels(baseLatLng, pixelOffset) {
  if (!pixelOffset || (pixelOffset.x === 0 && pixelOffset.y === 0)) {
    return L.latLng(baseLatLng);
  }

  const zoom = state.map.getZoom();
  const projected = state.map.project(baseLatLng, zoom).add(pixelOffset);
  return state.map.unproject(projected, zoom);
}

export function updateAllGantryMarkerOffsets() {
  state.allGantryMarkerOffsetEntries.forEach(entry => {
    const displayLatLng = offsetLatLngByPixels(entry.baseLatLng, entry.pixelOffset);
    entry.marker.setLatLng(displayLatLng);
  });
}

export function updateRankingMarkerOffsets() {
  state.rankingMarkerOffsetEntries.forEach(entry => {
    const displayLatLng = offsetLatLngByPixels(entry.baseLatLng, entry.pixelOffset);
    entry.marker.setLatLng(displayLatLng);

    if (entry.connector) {
      entry.connector.setLatLngs([entry.baseLatLng, displayLatLng]);
    }
  });
}

export function updateM06AEndpointOffsets() {
  state.m06aEndpointOffsetEntries.forEach(entry => {
    entry.marker.setLatLng(offsetLatLngByPixels(entry.baseLatLng, entry.pixelOffset));
  });
}

export function renderRankingGantryMarkers(ranking) {
  state.allGantryMarkerOffsetEntries = [];
  state.m06aEndpointOffsetEntries = [];
  state.gantryLayer.clearLayers();
  state.selectionLayer?.clearLayers();
  state.highlightedGantryMarkers.clear();
  state.rankingMarkerOffsetEntries = [];

  const bounds = [];
  const preparedItems = [];
  const locationGroups = new Map();

  ranking.forEach((item, index) => {
    const rank = index + 1;
    const group = item.group || findGantryGroupById(item.gantryId);
    const latLng = getGantryLatLng(group);

    if (!group || !latLng) {
      return;
    }

    const prepared = {
      item,
      rank,
      group,
      baseLatLng: L.latLng(latLng)
    };

    preparedItems.push(prepared);
    bounds.push(latLng);

    const locationKey = getRankingLocationKey(latLng);
    if (!locationGroups.has(locationKey)) {
      locationGroups.set(locationKey, []);
    }
    locationGroups.get(locationKey).push(prepared);
  });

  /*
    同座標群組依名次排序：
    最前面的最高名次保留在真實座標，其餘才向外位移。
  */
  locationGroups.forEach(items => {
    items.sort((a, b) => a.rank - b.rank);

    items.forEach((prepared, collisionIndex) => {
      prepared.pixelOffset = getCollisionOffset(collisionIndex, items.length);
    });
  });

  preparedItems.forEach(prepared => {
    const { item, rank, baseLatLng } = prepared;
    const color = getRankingMarkerColor(rank);
    const pixelOffset = prepared.pixelOffset || L.point(0, 0);
    const displayLatLng = offsetLatLngByPixels(baseLatLng, pixelOffset);

    let connector = null;

    if (pixelOffset.x !== 0 || pixelOffset.y !== 0) {
      connector = L.polyline([baseLatLng, displayLatLng], {
        color,
        weight: 1.6,
        opacity: 0.72,
        dashArray: "4 4",
        interactive: false
      }).addTo(state.gantryLayer);
    }

    const marker = L.marker(displayLatLng, {
      icon: createRankingIcon(rank, color),
      riseOnHover: true,
      keyboard: true,
      /*
        名次越前面 z-index 越高。
        即使標記仍有局部重疊，第 1 名也一定顯示在最上層。
      */
      zIndexOffset: 12000 - rank * 100,
      title: `第 ${rank} 名：${item.name}`
    });

    marker.bindTooltip(
      `<strong>第 ${rank} 名｜${item.name}</strong><br>${getDirectionLabel(item.direction)}<br>總流量：${NUMBER_FORMATTER.format(item.volume)} 輛`,
      {
        className: "gantry-tooltip",
        sticky: false,
        direction: "top"
      }
    );

    marker.bindPopup(
      `<strong>第 ${rank} 名｜${item.name}</strong><br>` +
      `方向：${getDirectionLabel(item.direction)}<br>` +
      `查詢期間總流量：${NUMBER_FORMATTER.format(item.volume)} 輛`
    );

    marker.addTo(state.gantryLayer);
    state.highlightedGantryMarkers.set(item.key, marker);

    state.rankingMarkerOffsetEntries.push({
      marker,
      connector,
      baseLatLng,
      pixelOffset
    });
  });

  if (bounds.length === 1) {
    state.map.setView(bounds[0], 12, { animate: true });
  } else if (bounds.length > 1) {
    state.map.fitBounds(bounds, { padding: [40, 40], maxZoom: 11 });
  }

  /*
    fitBounds 可能改變縮放層級，重新以目前 zoom 計算固定像素位移。
  */
  setTimeout(updateRankingMarkerOffsets, 0);

  setMapDescription("未指定門架時，地圖顯示本次車流量排行前 12 名；同座標門架會自動錯開，虛線指向實際位置。");
  setMapLegend([
    { color: "#d4a017", label: "第 1 名" },
    { color: "#8b95a5", label: "第 2 名" },
    { color: "#b87333", label: "第 3 名" },
    { color: "#0f766e", label: "第 4–12 名" }
  ]);
}

export function focusRankingMarker(item) {
  const marker = state.highlightedGantryMarkers.get(item.key);
  const group = item.group || findGantryGroupById(item.gantryId);
  const latLng = getGantryLatLng(group);

  if (!marker || !latLng) {
    return;
  }

  state.map.setView(latLng, 13, { animate: true });

  state.map.once("moveend", () => {
    updateRankingMarkerOffsets();
    marker.openPopup();
  });

  /*
    如果地圖原本已在相同中心與縮放，moveend 可能不觸發。
  */
  setTimeout(() => {
    updateRankingMarkerOffsets();
    marker.openPopup();
  }, 350);
}

// M06A 點選起訖時的即時預覽：在全部門架地圖上疊加「起／終」標記，不清除底圖
export function renderM06ASelectionPreview(startGroup, endGroup) {
  if (!state.selectionLayer) {
    return;
  }

  state.selectionLayer.clearLayers();

  const endpoints = [
    { group: startGroup, label: "起", color: "#15803d" },
    { group: endGroup, label: "終", color: "#dc2626" }
  ].filter(endpoint => endpoint.group && getGantryLatLng(endpoint.group));

  endpoints.forEach(endpoint => {
    L.marker(getGantryLatLng(endpoint.group), {
      icon: createRouteEndpointIcon(endpoint.label, endpoint.color),
      zIndexOffset: 6000,
      interactive: false
    }).addTo(state.selectionLayer);
  });

  if (startGroup && endGroup) {
    setMapDescription(
      `已選擇 ${getGantryName(startGroup)} → ${getGantryName(endGroup)}，按「查詢」送出；再點其他門架會重新選起點。`
    );
  } else if (startGroup) {
    setMapDescription(
      `起點：${getGantryName(startGroup)}（${getDirectionLabel(getGantryDirection(startGroup))}）。請再點一個同方向的門架作為終點。`
    );
  }
}

export function createRouteEndpointIcon(label, color) {
  return L.divIcon({
    className: "route-endpoint-icon",
    html: `<div class="route-endpoint-marker" style="background:${color}">${label}</div>`,
    iconSize: [38, 38],
    iconAnchor: [19, 19],
    tooltipAnchor: [0, -24]
  });
}

export function getActualRouteLatLngs(data) {
  const routeIds = Array.isArray(data?.route_gantry_ids) ? data.route_gantry_ids : [];
  const latLngs = [];

  routeIds.forEach(gantryId => {
    const group = findGantryGroupById(gantryId);
    const latLng = group ? getGantryLatLng(group) : null;
    if (!latLng) {
      return;
    }

    const point = L.latLng(latLng);
    const previous = latLngs[latLngs.length - 1];
    if (!previous || !previous.equals(point)) {
      latLngs.push(point);
    }
  });

  return latLngs;
}

export function getRouteLatLngsForM06A(startGroup, endGroup) {
  const startLatLng = getGantryLatLng(startGroup);
  const endLatLng = getGantryLatLng(endGroup);

  if (!startLatLng || !endLatLng) {
    return [];
  }

  const routeGroups = getOrderedRouteGroupsBetween(startGroup, endGroup);

  if (routeGroups.length >= 2) {
    return routeGroups
      .map(group => getGantryLatLng(group))
      .filter(Boolean)
      .map(latLng => L.latLng(latLng));
  }

  return [L.latLng(startLatLng), L.latLng(endLatLng)];
}

export function renderM06ARouteMap(query, data = null) {
  state.allGantryMarkerOffsetEntries = [];
  state.rankingMarkerOffsetEntries = [];
  state.m06aEndpointOffsetEntries = [];
  state.gantryLayer.clearLayers();
  state.selectionLayer?.clearLayers();
  state.highlightedGantryMarkers.clear();

  const startGroup =
    state.gantryGroups.find(group => group.group_id === query.start_gantry_group_id) ||
    findGantryGroupById(query.start_gantry);

  const endGroup =
    state.gantryGroups.find(group => group.group_id === query.end_gantry_group_id) ||
    findGantryGroupById(query.end_gantry);

  const startLatLngArray = getGantryLatLng(startGroup);
  const endLatLngArray = getGantryLatLng(endGroup);

  if (!startGroup || !endGroup || !startLatLngArray || !endLatLngArray) {
    renderGantryMarkers();
    setMapDescription("已收到 M06A 起訖門架，但其中一個門架缺少經緯度，因此暫時顯示全部門架。");
    return;
  }

  const startLatLng = L.latLng(startLatLngArray);
  const endLatLng = L.latLng(endLatLngArray);
  const sameLocation =
    getRankingLocationKey(startLatLngArray) === getRankingLocationKey(endLatLngArray);

  /*
    路線來源優先序：
    1. 後端從 M06A TripInformation 抽樣出的「實際旅次門架序列」——跨國道也正確。
    2. 同路同方向時以里程推算的門架序列。
    3. 都拿不到才退回起訖兩點直線。
  */
  const actualRouteLatLngs = getActualRouteLatLngs(data);
  const routeIsActualPath = actualRouteLatLngs.length >= 2;
  const routeLatLngs = routeIsActualPath
    ? actualRouteLatLngs
    : getRouteLatLngsForM06A(startGroup, endGroup);
  const routeFollowsGantryOrder = routeIsActualPath || (
    routeLatLngs.length >= 2 &&
    getOrderedRouteGroupsBetween(startGroup, endGroup).length >= 2
  );

  L.polyline(routeLatLngs, {
    color: "#0f766e",
    weight: 4,
    opacity: .78,
    dashArray: "10 7",
    interactive: false
  }).addTo(state.gantryLayer);

  const startMarker = L.marker(startLatLng, {
    icon: createRouteEndpointIcon("起", "#15803d"),
    zIndexOffset: 5000,
    title: `起始門架：${getGantryName(startGroup)}`
  });

  const endOffset = sameLocation ? L.point(44, 0) : L.point(0, 0);
  const endDisplayLatLng = offsetLatLngByPixels(endLatLng, endOffset);
  const endMarker = L.marker(endDisplayLatLng, {
    icon: createRouteEndpointIcon("終", "#dc2626"),
    zIndexOffset: 5100,
    title: `終點門架：${getGantryName(endGroup)}`
  });

  startMarker.bindPopup(
    `<strong>起始門架｜${getGantryName(startGroup)}</strong><br>` +
    `方向：${getDirectionLabel(getGantryDirection(startGroup))}`
  );

  endMarker.bindPopup(
    `<strong>終點門架｜${getGantryName(endGroup)}</strong><br>` +
    `方向：${getDirectionLabel(getGantryDirection(endGroup))}`
  );

  startMarker.addTo(state.gantryLayer);
  endMarker.addTo(state.gantryLayer);

  state.m06aEndpointOffsetEntries.push({
    marker: endMarker,
    baseLatLng: endLatLng,
    pixelOffset: endOffset
  });

  if (sameLocation) {
    state.map.setView(startLatLng, 13, { animate: true });
  } else {
    state.map.fitBounds(L.latLngBounds(routeLatLngs), {
      padding: [70, 70],
      maxZoom: 11
    });
  }

  setTimeout(updateM06AEndpointOffsets, 0);

  setMapDescription(
    routeIsActualPath
      ? "M06A 路線為本次起訖旅次「實際行經的門架序列」連線，綠色為起點、紅色為終點。"
      : routeFollowsGantryOrder
        ? "M06A 地圖會依同一路線上的門架順序連線，綠色為起點、紅色為終點。"
        : "M06A 地圖以綠色標示起點、紅色標示終點；若無法判斷完整門架序列，則以起訖位置連線顯示。"
  );
  setMapLegend([
    { color: "#15803d", label: "起始門架" },
    { color: "#dc2626", label: "終點門架" },
    { color: "#0f766e", label: "區間路線" }
  ]);
}
