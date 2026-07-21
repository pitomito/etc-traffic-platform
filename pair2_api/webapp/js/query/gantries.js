// 門架資料與下拉選單

import { state } from "./state.js";

export function getGantryLatLng(gantry) {
  const lat = Number(gantry.lat ?? gantry.latitude ?? gantry.Latitude ?? gantry.y);
  const lng = Number(gantry.lng ?? gantry.lon ?? gantry.longitude ?? gantry.Longitude ?? gantry.x);

  if (Number.isFinite(lat) && Number.isFinite(lng)) {
    return [lat, lng];
  }

  return null;
}

export function normalizeGantryGroup(item) {
  const currentGantryId = item.current_gantry_id || item.gantry_id || "";
  const gantryIds = Array.isArray(item.gantry_ids) && item.gantry_ids.length > 0
    ? item.gantry_ids
    : currentGantryId
      ? [currentGantryId]
      : [];

  const displayName =
    item.display_name ||
    item.name ||
    item.gantry_name ||
    currentGantryId ||
    "未命名門架";

  const rawDirection = item.direction || String(currentGantryId).slice(-1);
  const direction = rawDirection === "S" ? "S" : "N";

  return {
    ...item,
    group_id: item.group_id || `G_${currentGantryId}`,
    current_gantry_id: currentGantryId,
    gantry_ids: gantryIds,
    display_name: displayName,
    name: displayName,
    direction
  };
}

export function getGantryName(gantryGroup) {
  return gantryGroup?.display_name || gantryGroup?.name || "未命名門架";
}

export function getGantryDirection(gantryGroup) {
  return gantryGroup?.direction === "S" ? "S" : "N";
}

export async function loadGantries() {
  try {
    const candidatePaths = [
      "data/gantry_groups_frontend.json",
      "gantry_groups_frontend.json",
      "data/gantry_groups_frontend(1).json",
      "gantry_groups_frontend(1).json",
      "data/gantries_frontend.json",
      "gantries_frontend.json"
    ];

    let response = null;
    let usedPath = "";

    for (const path of candidatePaths) {
      response = await fetch(path);
      if (response.ok) {
        usedPath = path;
        break;
      }
    }

    if (!response || !response.ok) {
      throw new Error("門架群組 JSON 載入失敗，請確認 data/gantry_groups_frontend.json 是否存在。");
    }

    const rawGroups = await response.json();

    if (!Array.isArray(rawGroups)) {
      throw new Error("門架群組 JSON 格式錯誤：最外層必須是陣列。");
    }

    state.gantryGroups = rawGroups.map(normalizeGantryGroup);
    console.log("門架群組 JSON 載入成功：", usedPath, state.gantryGroups.length);
    return state.gantryGroups;
  } catch (error) {
    console.error(error);
    throw error;
  }
}

export function getFormDateRange(prefix) {
  const startDateValue = document.getElementById(`${prefix}StartDate`)?.value || "";
  const endDateValue = document.getElementById(`${prefix}EndDate`)?.value || "";

  if (!startDateValue || !endDateValue) {
    return null;
  }

  const startDate = new Date(`${startDateValue}T00:00:00`);
  const endDate = new Date(`${endDateValue}T23:59:59`);

  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
    return null;
  }

  return { startDate, endDate };
}

export function groupOverlapsDateRange(gantryGroup, dateRange) {
  if (!dateRange) {
    return true;
  }

  const versions = Array.isArray(gantryGroup.versions) && gantryGroup.versions.length > 0
    ? gantryGroup.versions
    : [gantryGroup];

  return versions.some(version => {
    const isActive = version.is_active === true;

    const firstSeen = version.first_seen
      ? new Date(`${version.first_seen}T00:00:00`)
      : null;

    /*
      active 門架的 last_seen 只是目前資料集最後觀測日，
      不是門架失效日，因此視為「仍持續有效」。
      只有 inactive 的歷史版本，才使用 last_seen 當作有效期限。
    */
    const lastSeen = !isActive && version.last_seen
      ? new Date(`${version.last_seen}T23:59:59`)
      : null;

    /*
      某些舊版本只有官方版本紀錄，卻沒有實際 first_seen / last_seen。
      這種 inactive 且完全無日期的版本不能視為任何日期都有效。
    */
    if (!isActive && !firstSeen && !lastSeen) {
      return false;
    }

    const startsBeforeQueryEnds = !firstSeen || firstSeen <= dateRange.endDate;
    const endsAfterQueryStarts = !lastSeen || lastSeen >= dateRange.startDate;

    return startsBeforeQueryEnds && endsAfterQueryStarts;
  });
}

export function populateGantrySelect(selectId, defaultText, direction = "", timePrefix = "") {
  const select = document.getElementById(selectId);
  const previousValue = select.value;
  const dateRange = timePrefix ? getFormDateRange(timePrefix) : null;

  select.innerHTML = "";

  const defaultOption = document.createElement("option");
  defaultOption.value = "";
  defaultOption.textContent = defaultText;
  select.appendChild(defaultOption);

  const filteredGroups = state.gantryGroups
    .filter(gantryGroup => {
      const directionMatches = !direction || getGantryDirection(gantryGroup) === direction;
      const dateMatches = groupOverlapsDateRange(gantryGroup, dateRange);
      return directionMatches && dateMatches;
    })
    .sort((a, b) => {
      const roadCompare = String(a.road || "").localeCompare(String(b.road || ""), "zh-Hant");
      if (roadCompare !== 0) {
        return roadCompare;
      }

      const directionA = getGantryDirection(a);
      const directionB = getGantryDirection(b);
      if (directionA !== directionB) {
        return directionA === "N" ? -1 : 1;
      }

      // 依行車順序排列：北上里程由大到小、南下由小到大
      const mileageA = parseGantryMileage(a);
      const mileageB = parseGantryMileage(b);
      if (Number.isFinite(mileageA) && Number.isFinite(mileageB) && mileageA !== mileageB) {
        return directionA === "N" ? mileageB - mileageA : mileageA - mileageB;
      }

      return getGantryName(a).localeCompare(getGantryName(b), "zh-Hant");
    });

  filteredGroups.forEach(gantryGroup => {
    const option = document.createElement("option");
    const directionLabel = gantryGroup.direction_label ||
      (getGantryDirection(gantryGroup) === "S" ? "南下" : "北上");
    option.value = gantryGroup.group_id;
    option.textContent = `${getGantryName(gantryGroup)}｜${directionLabel}`;
    option.dataset.currentGantryId = gantryGroup.current_gantry_id || "";
    option.dataset.gantryIds = JSON.stringify(gantryGroup.gantry_ids || []);
    select.appendChild(option);
  });

  if (filteredGroups.length === 0) {
    const emptyOption = document.createElement("option");
    emptyOption.value = "";
    emptyOption.textContent = "此條件下沒有可用的門架位置";
    emptyOption.disabled = true;
    select.appendChild(emptyOption);
  }

  const optionValues = Array.from(select.options).map(option => option.value);
  if (optionValues.includes(previousValue)) {
    select.value = previousValue;
  }
}

// ---------- M06A 階梯式選單：道路 → 方向 → 門架（照行車順序） ----------

export function getM06ARoadKey(gantryGroup) {
  return String(gantryGroup.road || "").trim() || "其他道路";
}

export function sortByDrivingOrder(groups, direction) {
  // 北上：里程由大到小（南→北）；南下：里程由小到大（北→南）
  return [...groups].sort((a, b) => {
    const mileageA = parseGantryMileage(a);
    const mileageB = parseGantryMileage(b);
    if (Number.isFinite(mileageA) && Number.isFinite(mileageB) && mileageA !== mileageB) {
      return direction === "N" ? mileageB - mileageA : mileageA - mileageB;
    }
    return getGantryName(a).localeCompare(getGantryName(b), "zh-Hant");
  });
}

export function getM06ARouteGroups(road, direction) {
  const dateRange = getFormDateRange("m06a");
  const groups = state.gantryGroups.filter(gantryGroup =>
    getM06ARoadKey(gantryGroup) === road &&
    getGantryDirection(gantryGroup) === direction &&
    groupOverlapsDateRange(gantryGroup, dateRange)
  );
  return sortByDrivingOrder(groups, direction);
}

export function makeM06AOption(gantryGroup) {
  const option = document.createElement("option");
  const mileage = parseGantryMileage(gantryGroup);
  option.value = gantryGroup.group_id;
  option.textContent = Number.isFinite(mileage)
    ? `${getGantryName(gantryGroup)}（${mileage}k）`
    : getGantryName(gantryGroup);
  return option;
}

export function populateM06ARoadSelect() {
  const select = document.getElementById("m06aRoad");
  const previousValue = select.value;
  const roads = [...new Set(state.gantryGroups.map(getM06ARoadKey))]
    .sort((a, b) => a.localeCompare(b, "zh-Hant"));

  select.innerHTML = "";
  roads.forEach(road => {
    const option = document.createElement("option");
    option.value = road;
    option.textContent = road;
    select.appendChild(option);
  });

  if (roads.includes(previousValue)) {
    select.value = previousValue;
  }
}

export function populateM06AStartSelect() {
  const select = document.getElementById("m06aStartGantry");
  const previousValue = select.value;
  const road = document.getElementById("m06aRoad").value;
  const direction = document.getElementById("m06aDirection").value;

  select.innerHTML = "";
  const defaultOption = document.createElement("option");
  defaultOption.value = "";
  defaultOption.textContent = "請選擇起始門架位置";
  select.appendChild(defaultOption);

  const groups = getM06ARouteGroups(road, direction);
  groups.forEach(gantryGroup => select.appendChild(makeM06AOption(gantryGroup)));

  if (groups.some(gantryGroup => gantryGroup.group_id === previousValue)) {
    select.value = previousValue;
  }
}

export function populateM06AEndSelect() {
  const select = document.getElementById("m06aEndGantry");
  const previousValue = select.value;
  const road = document.getElementById("m06aRoad").value;
  const direction = document.getElementById("m06aDirection").value;
  const startGroup = getSelectedGantryGroup("m06aStartGantry");

  select.innerHTML = "";
  const defaultOption = document.createElement("option");
  defaultOption.value = "";
  defaultOption.textContent = startGroup ? "請選擇終點門架位置" : "請先選擇起始門架";
  select.appendChild(defaultOption);

  const sameRoadGroups = getM06ARouteGroups(road, direction);
  let candidates = sameRoadGroups;

  // 已選起點：同路線只列「行車順序在起點之後」的門架，從結構上排除順序顛倒
  if (startGroup) {
    const startIndex = sameRoadGroups.findIndex(
      gantryGroup => gantryGroup.group_id === startGroup.group_id
    );
    if (startIndex >= 0) {
      candidates = sameRoadGroups.slice(startIndex + 1);
    }
  }

  const sameRoadOptgroup = document.createElement("optgroup");
  sameRoadOptgroup.label = `${road}（起點之後）`;
  candidates.forEach(gantryGroup => sameRoadOptgroup.appendChild(makeM06AOption(gantryGroup)));
  if (candidates.length > 0) {
    select.appendChild(sameRoadOptgroup);
  }

  // 跨國道：其他道路的同方向門架（跨路旅次經系統交流道轉換，無法驗證順序）
  const otherRoads = [...new Set(state.gantryGroups.map(getM06ARoadKey))]
    .filter(otherRoad => otherRoad !== road)
    .sort((a, b) => a.localeCompare(b, "zh-Hant"));

  otherRoads.forEach(otherRoad => {
    const groups = getM06ARouteGroups(otherRoad, direction);
    if (groups.length === 0) {
      return;
    }
    const optgroup = document.createElement("optgroup");
    optgroup.label = `${otherRoad}｜跨道路`;
    groups.forEach(gantryGroup => optgroup.appendChild(makeM06AOption(gantryGroup)));
    select.appendChild(optgroup);
  });

  const optionValues = Array.from(select.options).map(option => option.value);
  if (optionValues.includes(previousValue)) {
    select.value = previousValue;
  }
}

export function populateM06ASelects() {
  populateM06AStartSelect();
  populateM06AEndSelect();
}

export function populateAllGantrySelects() {
  const m03aDirection = document.getElementById("m03aDirection").value;

  populateGantrySelect("m03aGantry", "全部門架位置", m03aDirection, "m03a");
  populateM06ARoadSelect();
  populateM06ASelects();
}

export function getSelectedGantryGroup(selectId) {
  const groupId = document.getElementById(selectId).value;
  if (!groupId) {
    return null;
  }
  return state.gantryGroups.find(gantryGroup => gantryGroup.group_id === groupId) || null;
}

export function getGantrySelectionPayload(gantryGroup) {
  if (!gantryGroup) {
    return {
      group_id: "",
      display_name: "",
      current_gantry_id: "",
      gantry_ids: [],
      direction: ""
    };
  }

  return {
    group_id: gantryGroup.group_id,
    display_name: getGantryName(gantryGroup),
    current_gantry_id: gantryGroup.current_gantry_id || "",
    gantry_ids: Array.isArray(gantryGroup.gantry_ids) ? gantryGroup.gantry_ids : [],
    direction: getGantryDirection(gantryGroup),
    has_code_change: Boolean(gantryGroup.has_code_change)
  };
}

export function findGantryGroupById(gantryId) {
  if (!gantryId) {
    return null;
  }

  return state.gantryGroups.find(group => {
    const ids = Array.isArray(group.gantry_ids) ? group.gantry_ids : [];
    return group.current_gantry_id === gantryId || ids.includes(gantryId);
  }) || null;
}

export function parseGantryMileage(gantryGroup) {
  const candidateId =
    gantryGroup?.current_gantry_id ||
    gantryGroup?.gantry_id ||
    (Array.isArray(gantryGroup?.gantry_ids) ? gantryGroup.gantry_ids[0] : "") ||
    "";

  // F=一般國道、H=汐五高架(01H)、A=國3甲(03A)；里程都在同一個四碼欄位
  const match = String(candidateId).match(/^\d{2}[FHA](\d{4})[NS]$/);
  if (!match) {
    return null;
  }

  return Number(match[1]) / 10;
}

export function getGantryRoadKey(gantryGroup) {
  return String(
    gantryGroup?.road ||
    gantryGroup?.road_name ||
    gantryGroup?.road_label ||
    ""
  ).trim();
}

export function getOrderedRouteGroupsBetween(startGroup, endGroup) {
  if (!startGroup || !endGroup) {
    return [];
  }

  const direction = getGantryDirection(startGroup);
  const startRoad = getGantryRoadKey(startGroup);
  const endRoad = getGantryRoadKey(endGroup);
  const startMileage = parseGantryMileage(startGroup);
  const endMileage = parseGantryMileage(endGroup);

  if (
    !startRoad ||
    !endRoad ||
    startRoad !== endRoad ||
    !Number.isFinite(startMileage) ||
    !Number.isFinite(endMileage)
  ) {
    return [];
  }

  const minMileage = Math.min(startMileage, endMileage);
  const maxMileage = Math.max(startMileage, endMileage);

  return state.gantryGroups
    .filter(group => {
      const latLng = getGantryLatLng(group);
      const mileage = parseGantryMileage(group);

      return (
        !!latLng &&
        getGantryDirection(group) === direction &&
        getGantryRoadKey(group) === startRoad &&
        Number.isFinite(mileage) &&
        mileage >= minMileage &&
        mileage <= maxMileage
      );
    })
    .sort((a, b) => {
      const mileageA = parseGantryMileage(a) ?? 0;
      const mileageB = parseGantryMileage(b) ?? 0;

      // 北向：里程由大到小；南向：里程由小到大
      return direction === "N"
        ? mileageB - mileageA
        : mileageA - mileageB;
    });
}
