#!/usr/bin/env bash
#
# put_to_hdfs.sh
# ==============
# 職責範圍：只做「把本機 staging 的 csv 整批 put 上 HDFS」。這是從舊
#           m03a_extract_to_hdfs.sh 抽離出來的 put 段落，獨立成兩條流程共用的一支。
#           解壓/攤平/抓檔不在這裡（那是 01 的 extract_to_hdfs.sh 與
#           02 的 tdcs_daily_fetch.py 的事）。
#
# 介面：    put_to_hdfs.sh <type> <year> <month>
#             <type>  = M03A | M06A
#             <year>  = YYYY
#             <month> = M 或 MM（自動補零）
#
# 輸入來源：本機 staging（兩條流程統一的落地路徑，故本支通吃 01 / 02 兩邊產出）
#             staging/extract/<type>/year=<year>/month=<month>/*.csv
# 輸出位置：HDFS 最終路徑
#             /raw/<type>/year=<year>/month=<month>/
# 銜接對象：
#   - 上游 A（歷史）：01_historical_backfill/extract_to_hdfs.sh
#   - 上游 B（每日）：02_daily_automation/tdcs_daily_fetch.py
#   - 兩者都把攤平/抓下來的 csv 放到同一個本機 staging 路徑，本支再整批上傳。
#
# 冪等（三態，逐「資料日」判斷；用檔名日期前綴分組）：
#   數 HDFS 當天檔數 → >= 門檻(EXPECTED_COUNT)：完整,跳過
#                      0 < 檔數 < 門檻          ：不完整,覆蓋重傳
#                      = 0                      ：正常處理
#   門檻依 type：M03A=288（每 5 分 1 檔）、M06A=24（每小時 1 檔）。
#
# 用法：
#   ./put_to_hdfs.sh M03A 2022 01
#   DRY_RUN=1 ./put_to_hdfs.sh M06A 2026 06        # 只印不做
#   FORCE=1   ./put_to_hdfs.sh M03A 2022 01        # 不論完整與否一律覆蓋重傳
#
set -euo pipefail

# --- 參數 ---
if [ "$#" -ne 3 ]; then
  echo "用法: $0 <type> <year> <month>   例: $0 M03A 2022 01" >&2
  exit 2
fi
TYPE="$1"
YEAR="$2"
MONTH=$(printf '%02d' "$((10#$3))")   # 補零成 MM,並容忍 "6" / "06"

case "$TYPE" in
  M03A|M06A) ;;
  *) echo "type 只接受 M03A 或 M06A,收到: $TYPE" >&2; exit 2 ;;
esac

# ====================== CONFIG（可被環境變數覆蓋） ======================
# 本機 staging 根目錄（要跟 extract_to_hdfs.sh / tdcs_daily_fetch.py 一致）
STAGING_BASE="${STAGING_BASE:-$HOME/wulin/staging/extract}"

# HDFS 落地根目錄
HDFS_BASE="${HDFS_BASE:-/raw}"

# 每日預期 CSV 檔數,依 type 決定（用於三態判斷分區是否完整）
case "$TYPE" in
  M03A) EXPECTED_COUNT="${EXPECTED_COUNT:-288}" ;;
  M06A) EXPECTED_COUNT="${EXPECTED_COUNT:-24}" ;;
esac

# 行為旗標
DRY_RUN="${DRY_RUN:-0}"        # 1=只印不執行
FORCE="${FORCE:-0}"            # 1=不論完整與否一律覆蓋重傳
# =====================================================================

SRC="$STAGING_BASE/$TYPE/year=$YEAR/month=$MONTH"
PART="$HDFS_BASE/$TYPE/year=$YEAR/month=$MONTH"

log() { echo "[$(date '+%F %T')] $*"; }

run() {
  if [ "$DRY_RUN" = "1" ]; then
    echo "DRY  > $*"
  else
    eval "$@"
  fi
}

# --- 防重複行程（Conventions:落地前先 pgrep,避免 ._COPYING_ 撞路徑） ---
# pgrep 會把腳本自己、以及 $(...) fork 出的子 shell 一起抓到,
# 因此排除:自身 PID($$)、父 PID($PPID)、以及「父行程是自己」的子行程。
SCRIPT_TAG="put_to_hdfs"
others=""
for pid in $(pgrep -f "$SCRIPT_TAG"); do
  [ "$pid" = "$$" ] && continue
  [ "$pid" = "$PPID" ] && continue
  ppid=$(awk '{print $4}' "/proc/$pid/stat" 2>/dev/null || echo "")
  [ "$ppid" = "$$" ] && continue          # 自己 fork 的子 shell
  others="$others $pid"
done
others=$(echo "$others" | xargs || true)
if [ -n "$others" ]; then
  log "偵測到其他同名行程,為避免 ._COPYING_ 撞路徑請先確認:"
  for pid in $others; do
    echo "  PID $pid: $(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)"
  done
  exit 1
fi

# --- 前置檢查 ---
command -v hdfs >/dev/null || { log "找不到 hdfs 指令"; exit 1; }
[ -d "$SRC" ] || { log "本機 staging 分區不存在: $SRC"; exit 1; }

log "TYPE=$TYPE 分區 year=$YEAR/month=$MONTH"
log "來源 $SRC | 落地 $PART"
log "DRY_RUN=$DRY_RUN FORCE=$FORCE EXPECTED_COUNT=$EXPECTED_COUNT"

# --- 找出本機 staging 這個月分區裡有哪些「資料日」（從檔名 TDCS_<type>_YYYYMMDD_… 取） ---
days=$(
  find "$SRC" -maxdepth 1 -name "TDCS_${TYPE}_*.csv" -type f -printf '%f\n' 2>/dev/null \
    | cut -d_ -f3 | sort -u
)
if [ -z "$days" ]; then
  log "分區內沒有任何 TDCS_${TYPE}_*.csv,無事可做"
  exit 0
fi

ok=0; skip=0; fail=0

for day in $days; do
  # 以「當天 HDFS 檔數」判斷:完整(=EXPECTED_COUNT)才跳過;不足則覆蓋重傳
  existing=$(hdfs dfs -ls "$PART/TDCS_${TYPE}_${day}_"*.csv 2>/dev/null | grep -c '\.csv$' || true)
  existing="${existing:-0}"
  if [ "$FORCE" != "1" ]; then
    if [ "$existing" -ge "$EXPECTED_COUNT" ]; then
      log "[$day] HDFS 已有 $existing 檔(完整,門檻 $EXPECTED_COUNT),跳過"
      skip=$((skip+1))
      continue
    elif [ "$existing" -gt 0 ]; then
      log "[$day] HDFS 僅 $existing 檔(不足 $EXPECTED_COUNT),視為不完整 → 覆蓋重傳"
    fi
  else
    [ "$existing" -gt 0 ] && log "[$day] FORCE=1,已有 $existing 檔仍覆蓋重傳"
  fi

  # 本機當天檔數（sanity check）
  n=$(find "$SRC" -maxdepth 1 -name "TDCS_${TYPE}_${day}_"'*.csv' -type f | wc -l)
  if [ "$n" -eq 0 ]; then
    log "[$day] 本機沒有 CSV,略過"
    fail=$((fail+1))
    continue
  fi
  log "[$day] 本機 $n 檔 → 上傳 $PART"

  # 落 HDFS:建分區 → 只清當天那批 → 整批 put（單次 JVM,避免逐檔開銷）
  run "hdfs dfs -mkdir -p '$PART'"
  run "hdfs dfs -rm -f '$PART/TDCS_${TYPE}_${day}_'*.csv"
  run "hdfs dfs -put -f '$SRC'/TDCS_${TYPE}_${day}_*.csv '$PART'/"

  ok=$((ok+1))
done

log "完成。成功 $ok 天 | 跳過 $skip 天 | 失敗 $fail 天"
