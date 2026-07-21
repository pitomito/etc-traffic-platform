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
#                      0 < 檔數 < 門檻          ：不完整,增量補傳（put -f）
#                      = 0                      ：正常處理
#   門檻依 type：M03A=288（每 5 分 1 檔）、M06A=24（每小時 1 檔）。
#
#   增量補傳＝直接 put -f：同名檔覆蓋、新檔加入、HDFS 上其他檔「不動」。
#   刻意不先 rm 當天整批 —— 檔級補漏時 staging 只有缺的那幾個檔,
#   若先 rm 會把 HDFS 上原本好的檔刪掉、只補回少數幾個 → 資料變少。
#   要整天重灌用 FORCE=1（唯一保留 rm 的路徑）。
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
# 本機 staging 根目錄（要跟 extract_to_hdfs.sh / tdcs_daily_fetch.py 一致）。
# 預設寫死絕對路徑,不能用 $HOME 推:Airflow 的 kubectl exec 是 root 進來
# （$HOME=/root）,會找錯地方(2026-07-17 踩到);要換路徑用環境變數蓋。
STAGING_BASE="${STAGING_BASE:-/home/bigred/wulin/staging/extract}"

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
KEEP_STAGING="${KEEP_STAGING:-0}"  # 1=上傳成功後「不」刪本機 staging（除錯用）；預設 0=刪，避免無限累積
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

# --- 防重複執行：flock 檔案鎖（避免兩個 put 同時寫同路徑撞 ._COPYING_）---
# 為什麼不用 pgrep：pgrep -f "put_to_hdfs" 會誤判「命令列剛好含這字串」的祖先行程。
#   DAG 是用 `find … | while read p; do bash put_to_hdfs.sh …; done` 一整串呼叫，
#   那串 cmdline 就含 "put_to_hdfs"；又因為管線會多生一層 subshell，它是「祖父」
#   而非「父」，舊的排除邏輯漏掉 → 本支把自己的祖先當成別人、自己擋自己。
#   flock 只認真正的併發，不看字串。
LOCK_FILE="${LOCK_FILE:-/tmp/put_to_hdfs.lock}"
# 鎖檔可能被「另一個身分」先建走（root 手動測 vs Airflow exec 進來的使用者），
# 用 > 開寫模式會 Permission denied（2026-07-17 踩到）。flock 用唯讀 fd 一樣能鎖，
# 所以：不存在才建（順手開 666 給所有身分），一律以唯讀模式開。
[ -e "$LOCK_FILE" ] || { touch "$LOCK_FILE" 2>/dev/null; chmod 666 "$LOCK_FILE" 2>/dev/null; }
exec 9<"$LOCK_FILE" || { log "無法開啟鎖檔 $LOCK_FILE"; exit 1; }
if ! flock -n 9; then
  log "另一個 put_to_hdfs 正在執行（鎖：$LOCK_FILE），本次結束"
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
      # 順手清掉本機同日殘檔:HDFS 已完整,staging 這批是重複品,留著只會
      # 讓每天的 put 重掃幾萬個檔(2026-07-17 累積到 2 萬檔才發現)
      if [ "$KEEP_STAGING" != "1" ]; then
        run "rm -f '$SRC'/TDCS_${TYPE}_${day}_*.csv"
      fi
      skip=$((skip+1))
      continue
    elif [ "$existing" -gt 0 ]; then
      log "[$day] HDFS 僅 $existing 檔(不足 $EXPECTED_COUNT),視為不完整 → 增量補傳(put -f,不動既有檔)"
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

  # 落 HDFS:建分區 → 增量 put（單次 JVM,避免逐檔開銷）
  # put -f:同名覆蓋、新檔加入、其他既有檔不動 → 對「staging 只有缺檔」的
  # 檔級補漏安全。先 rm 只在 FORCE=1(整天重灌)才做。
  # set -e 之下,put 失敗會直接中止整支腳本;能走到下一行就代表 put 成功。
  run "hdfs dfs -mkdir -p '$PART'"
  if [ "$FORCE" = "1" ]; then
    run "hdfs dfs -rm -f '$PART/TDCS_${TYPE}_${day}_'*.csv"
  fi
  run "hdfs dfs -put -f '$SRC'/TDCS_${TYPE}_${day}_*.csv '$PART'/"

  # 上傳成功 → 刪本機當天那批,避免 staging 無限累積吃磁碟（KEEP_STAGING=1 可保留）
  if [ "$KEEP_STAGING" != "1" ]; then
    run "rm -f '$SRC'/TDCS_${TYPE}_${day}_*.csv"
    log "[$day] 已上傳並清掉本機 staging 那批"
  fi

  ok=$((ok+1))
done

# 收尾:分區清空後順手移除空的本機分區目錄（非空/刪不掉都不影響）
if [ "$KEEP_STAGING" != "1" ] && [ "$DRY_RUN" != "1" ]; then
  rmdir "$SRC" 2>/dev/null || true
fi

log "完成。成功 $ok 天 | 跳過 $skip 天 | 失敗 $fail 天"
