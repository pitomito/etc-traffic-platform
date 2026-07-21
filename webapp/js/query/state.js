// 查詢頁共用的可變狀態。
// 模組只共享這個物件，避免散落多份彼此不同步的全域變數。

export const state = {
  map: null,
  gantryLayer: null,
  selectionLayer: null,
  gantryGroups: [],
  queryMeta: null,
  m03aChartInstance: null,
  m06aTrendChartInstance: null,
  m06aVehicleChartInstance: null,
  highlightedGantryMarkers: new Map(),
  allGantryMarkerOffsetEntries: [],
  rankingMarkerOffsetEntries: [],
  m06aEndpointOffsetEntries: []
};
