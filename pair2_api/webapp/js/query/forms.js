// 表單、查詢條件、事件綁定

import { DATASET_HINTS } from "./config.js";
import { state } from "./state.js";
import {
  getGantrySelectionPayload,
  getM06ARoadKey,
  getM06ARouteGroups,
  getSelectedGantryGroup,
  populateGantrySelect,
  populateM06AEndSelect,
  populateM06ASelects,
  populateM06AStartSelect
} from "./gantries.js";
import { renderGantryMarkers, renderM06ASelectionPreview, setMapDescription } from "./map.js";
import { sendQueryToFlask } from "./api.js";
import { resetResultDashboard } from "./ui.js";

export function fillHourSelects() {
  document.querySelectorAll(".hour-select").forEach(select => {
    select.innerHTML = "";

    for (let hour = 0; hour < 24; hour += 1) {
      const option = document.createElement("option");
      option.value = String(hour).padStart(2, "0");
      option.textContent = `${String(hour).padStart(2, "0")} 時`;
      select.appendChild(option);
    }
  });
}

export function fillMinuteSelects() {
  document.querySelectorAll(".minute-select").forEach(select => {
    select.innerHTML = "";

    for (let minute = 0; minute < 60; minute += 5) {
      const option = document.createElement("option");
      option.value = String(minute).padStart(2, "0");
      option.textContent = `${String(minute).padStart(2, "0")} 分`;
      select.appendChild(option);
    }
  });
}

export function switchDataset(datasetType) {
  const m03aForm = document.getElementById("m03aForm");
  const m06aForm = document.getElementById("m06aForm");
  const datasetHint = document.getElementById("datasetHint");
  const resultDatasetBadge = document.getElementById("resultDatasetBadge");
  const resultPanelDescription = document.getElementById("resultPanelDescription");

  m03aForm.classList.toggle("hidden", datasetType !== "M03A");
  m06aForm.classList.toggle("hidden", datasetType !== "M06A");
  datasetHint.textContent = DATASET_HINTS[datasetType];
  resultDatasetBadge.textContent = datasetType;

  resultPanelDescription.textContent = datasetType === "M03A"
    ? "送出 M03A 查詢後，這裡會顯示條件摘要、重要指標、圖表與統計表格。"
    : "送出 M06A 查詢後，這裡會顯示起訖門架、總旅次數、時間趨勢、車種組成與統計表格。";

  resetResultDashboard();
  if (state.gantryGroups.length > 0) {
    renderGantryMarkers();
  }

  if (datasetType === "M06A") {
    setMapDescription("M06A 可直接在地圖上點門架：第一下選起點、第二下選終點（需同方向），也可以用下方的道路／方向／門架下拉選單。");
  }

  setTimeout(() => state.map.invalidateSize(), 80);
}

// 日期選擇器固定開放 2021 ～ 2026 年；實際資料涵蓋範圍由提示文字與送出前驗證把關
export function applyDataCoverage() {
  ["m03aStartDate", "m03aEndDate", "m06aStartDate", "m06aEndDate"].forEach(inputId => {
    const input = document.getElementById(inputId);
    input.min = "2021-01-01";
    input.max = "2026-12-31";
  });

  const meta = state.queryMeta;
  if (!meta) {
    return;
  }

  if (meta.m03a?.min_date) {
    DATASET_HINTS.M03A += `（資料範圍：${meta.m03a.min_date} ～ ${meta.m03a.max_date}）`;
  }
  if (meta.m06a?.min_date) {
    DATASET_HINTS.M06A += `（資料範圍：${meta.m06a.min_date} ～ ${meta.m06a.max_date}，單次查詢最長 3 個月）`;
  }

  document.getElementById("datasetHint").textContent =
    DATASET_HINTS[document.getElementById("datasetType").value];
}

export function updateGantryLabels() {
  const labels = {
    m03aGantry: "門架位置",
    m06aStartGantry: "起始門架位置",
    m06aEndGantry: "終點門架位置"
  };

  Object.entries(labels).forEach(([inputId, labelText]) => {
    const label = document.querySelector(`label[for="${inputId}"]`);
    if (label) {
      label.textContent = labelText;
    }
  });
}

export function getTimeRange(prefix) {
  const startDate = document.getElementById(`${prefix}StartDate`).value;
  const startHour = document.getElementById(`${prefix}StartHour`).value;
  const startMinute = document.getElementById(`${prefix}StartMinute`).value;
  const endDate = document.getElementById(`${prefix}EndDate`).value;
  const endHour = document.getElementById(`${prefix}EndHour`).value;
  const endMinute = document.getElementById(`${prefix}EndMinute`).value;

  return {
    start_time: startDate ? `${startDate} ${startHour}:${startMinute}` : "",
    end_time: endDate ? `${endDate} ${endHour}:${endMinute}` : ""
  };
}

export function makeM03AQuery() {
  const selectedGroup = getSelectedGantryGroup("m03aGantry");
  const selection = getGantrySelectionPayload(selectedGroup);

  return {
    dataset: "M03A",
    ...getTimeRange("m03a"),
    direction: document.getElementById("m03aDirection").value,
    gantry_group_id: selection.group_id,
    gantry_name: selection.display_name,
    gantry_direction: selection.direction,
    gantry_ids: selection.gantry_ids,
    gantry_id: selection.current_gantry_id,
    vehicle_type: document.getElementById("m03aVehicleType").value
  };
}

export function makeM06AQuery() {
  const startGroup = getSelectedGantryGroup("m06aStartGantry");
  const endGroup = getSelectedGantryGroup("m06aEndGantry");
  const startSelection = getGantrySelectionPayload(startGroup);
  const endSelection = getGantrySelectionPayload(endGroup);

  return {
    dataset: "M06A",
    ...getTimeRange("m06a"),
    start_gantry_group_id: startSelection.group_id,
    start_gantry_name: startSelection.display_name,
    start_gantry_ids: startSelection.gantry_ids,
    end_gantry_group_id: endSelection.group_id,
    end_gantry_name: endSelection.display_name,
    end_gantry_ids: endSelection.gantry_ids,
    start_gantry: startSelection.current_gantry_id,
    end_gantry: endSelection.current_gantry_id,
    vehicle_type: document.getElementById("m06aVehicleType").value
  };
}


export function bindQueryEvents() {
  document.getElementById("datasetType").addEventListener("change", event => {
    switchDataset(event.target.value);
  });

  document.getElementById("m03aDirection").addEventListener("change", () => {
    populateGantrySelect(
      "m03aGantry",
      "全部門架位置",
      document.getElementById("m03aDirection").value,
      "m03a"
    );
  });

  ["m03aStartDate", "m03aEndDate"].forEach(inputId => {
    document.getElementById(inputId).addEventListener("change", () => {
      populateGantrySelect(
        "m03aGantry",
        "全部門架位置",
        document.getElementById("m03aDirection").value,
        "m03a"
      );
    });
  });

  // M06A 階梯式選單：道路／方向決定門架清單；起點決定終點可選範圍
  ["m06aRoad", "m06aDirection"].forEach(selectId => {
    document.getElementById(selectId).addEventListener("change", () => {
      populateM06ASelects();
      syncM06ASelectionPreview();
    });
  });

  document.getElementById("m06aStartGantry").addEventListener("change", () => {
    populateM06AEndSelect();
    syncM06ASelectionPreview();
  });

  document.getElementById("m06aEndGantry").addEventListener("change", syncM06ASelectionPreview);

  ["m06aStartDate", "m06aEndDate"].forEach(inputId => {
    document.getElementById(inputId).addEventListener("change", () => {
      populateM06ASelects();
    });
  });

  bindM06AMapSelection();

  document.getElementById("m03aSearchBtn").addEventListener("click", () => {
    const query = makeM03AQuery();
    console.log("M03A 查詢條件：", query);
    sendQueryToFlask(query);
  });

  document.getElementById("m06aSearchBtn").addEventListener("click", () => {
    const query = makeM06AQuery();
    console.log("M06A 查詢條件：", query);
    sendQueryToFlask(query);
  });

  document.getElementById("m03aForm").addEventListener("reset", () => {
    setTimeout(() => {
      populateGantrySelect("m03aGantry", "全部門架位置", "", "m03a");
      resetResultDashboard();
      renderGantryMarkers();
    }, 0);
  });

  document.getElementById("m06aForm").addEventListener("reset", () => {
    setTimeout(() => {
      populateM06ASelects();
      resetResultDashboard();
      renderGantryMarkers();
      renderM06ASelectionPreview(null, null);
    }, 0);
  });
}

// ---------- M06A 地圖點選起訖門架 ----------

function syncM06ASelectionPreview() {
  renderM06ASelectionPreview(
    getSelectedGantryGroup("m06aStartGantry"),
    getSelectedGantryGroup("m06aEndGantry")
  );
}

function bindM06AMapSelection() {
  document.addEventListener("gantry-map-click", event => {
    if (document.getElementById("datasetType").value !== "M06A") {
      return;
    }

    const clickedGroup = state.gantryGroups.find(
      group => group.group_id === event.detail.groupId
    );
    if (!clickedGroup) {
      return;
    }

    const startSelect = document.getElementById("m06aStartGantry");
    const endSelect = document.getElementById("m06aEndGantry");
    const startGroup = getSelectedGantryGroup("m06aStartGantry");
    const endGroup = getSelectedGantryGroup("m06aEndGantry");

    const setAsStart = () => {
      document.getElementById("m06aRoad").value = getM06ARoadKey(clickedGroup);
      document.getElementById("m06aDirection").value = clickedGroup.direction;
      populateM06AStartSelect();
      startSelect.value = clickedGroup.group_id;
      populateM06AEndSelect();
      endSelect.value = "";
      syncM06ASelectionPreview();
    };

    // 還沒選起點，或起訖都選好了（重新開始）→ 這一下點的是新起點
    if (!startGroup || endGroup) {
      setAsStart();
      return;
    }

    if (clickedGroup.group_id === startGroup.group_id) {
      return;
    }

    // 方向不同的門架不能當終點 → 視為重選起點
    if (clickedGroup.direction !== startGroup.direction) {
      setAsStart();
      return;
    }

    // 同路線但行車順序在起點之前 → 自動對調起訖
    if (getM06ARoadKey(clickedGroup) === getM06ARoadKey(startGroup)) {
      const routeGroups = getM06ARouteGroups(
        getM06ARoadKey(clickedGroup),
        clickedGroup.direction
      );
      const clickedIndex = routeGroups.findIndex(g => g.group_id === clickedGroup.group_id);
      const startIndex = routeGroups.findIndex(g => g.group_id === startGroup.group_id);

      if (clickedIndex >= 0 && clickedIndex < startIndex) {
        startSelect.value = clickedGroup.group_id;
        populateM06AEndSelect();
        endSelect.value = startGroup.group_id;
        syncM06ASelectionPreview();
        return;
      }
    }

    endSelect.value = clickedGroup.group_id;
    syncM06ASelectionPreview();
  });
}
