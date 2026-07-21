#!/usr/bin/env bash
#
# extract_to_hdfs.sh
# ==================
# 職責範圍：只做「解壓 + 攤平」。把本機歷史 tar.gz 解開、把任意巢狀層的 csv
#           攤平到單層，落到本機 staging。不碰 HDFS、不刪任何本機檔案。
#           （put 到 HDFS 由 03_shared/put_to_hdfs.sh 負責；兩支以本機 staging 銜接。）
#
# 由 m03a_extract_to_hdfs.sh 合併改寫而來：用 DTYPE 參數（M03A / M06A）取代寫死
# 的檔案類型，M03A / M06A 共用同一支通用腳本。
#
# 輸入來源：本機 tar.gz，檔名規則 <DTYPE>_YYYYMMDD.tar.gz
#           位置 SRC_DIR（預設 $HOME/wulin/raw/<DTYPE>/）
# 輸出位置：本機 staging，攤平後的 csv 落到
#           staging/extract/<DTYPE>/year=YYYY/month=MM/
#           （用資料日期分區；/raw 保留給 HDFS 最終路徑，這裡一律走本機 staging）
# 銜接對象：
#   - 上游：01_historical_backfill/tdcs_downloader.py（下載 tar.gz 到 SRC_DIR）
#   - 下游：03_shared/put_to_hdfs.sh <type> <year> <month>
#           （讀本機 staging 同一路徑，整批 put 上 HDFS /raw/…）
#
# 用法：
#   ./extract_to_hdfs.sh                                  # 用下方 CONFIG 預設（DTYPE=M03A、預設區間）
#   DTYPE=M06A ./extract_to_hdfs.sh                       # 改抓 M06A
#   DTYPE=M06A START_DATE=20251201 END_DATE=20251215 ./extract_to_hdfs.sh
#   DRY_RUN=1 ./extract_to_hdfs.sh                        # 只印不做
#
set -euo pipefail

# ====================== CONFIG（可被環境變數覆蓋） ======================
# 資料類型（M03A 或 M06A）。這是本支「通用化」的關鍵參數。
DTYPE="${DTYPE:-M03A}"

# 日期區間（含起含訖，格式 YYYYMMDD）
START_DATE="${START_DATE:-20220101}"
END_DATE="${END_DATE:-20220131}"

# 本機 tar.gz 來源（檔名規則 <DTYPE>_YYYYMMDD.tar.gz）
SRC_DIR="${SRC_DIR:-$HOME/wulin/raw/${DTYPE}}"

# 本機 staging 落地根目錄（攤平後的 csv 落這裡，供 put_to_hdfs.sh 取用）
STAGING_BASE="${STAGING_BASE:-$HOME/wulin/staging/extract}"

# 行為旗標
DRY_RUN="${DRY_RUN:-0}"        # 1=只印不執行
# =====================================================================

log() { echo "[$(date '+%F %T')] $*"; }

run() {
  if [ "$DRY_RUN" = "1" ]; then
    echo "DRY  > $*"
  else
    eval "$@"
  fi
}

# --- 前置檢查（本支不碰 HDFS，故不檢查 hdfs 指令） ---
[ -d "$SRC_DIR" ] || { log "來源目錄不存在: $SRC_DIR"; exit 1; }

log "DTYPE=$DTYPE 區間 $START_DATE ~ $END_DATE"
log "來源 $SRC_DIR | 落地 $STAGING_BASE/$DTYPE/year=…/month=…"
log "DRY_RUN=$DRY_RUN"

ok=0; miss=0; fail=0
cur="$START_DATE"

while [ "$cur" -le "$END_DATE" ]; do
  y="${cur:0:4}"; m="${cur:4:2}"
  tarfile="$SRC_DIR/${DTYPE}_${cur}.tar.gz"
  # 本機 staging 分區（用資料日期分區，同月多天的 csv 共存於此）
  part_local="$STAGING_BASE/$DTYPE/year=$y/month=$m"

  # 來源不存在 → 略過（最後幾天可能還沒有）
  if [ ! -f "$tarfile" ]; then
    log "[$cur] 來源缺檔,略過: $tarfile"
    miss=$((miss+1))
    cur=$(date -d "$y-$m-${cur:6:2} +1 day" +%Y%m%d)
    continue
  fi

  log "[$cur] 解壓攤平 → $part_local"

  # 1) 解壓到本機 staging 分區，攤平（任意巢狀層的 csv 全搬到分區根層）
  #    注意：不清空整個分區（會誤刪同月其他天）；tar 直接覆蓋同名檔即可。
  run "mkdir -p '$part_local'"
  run "tar -xzf '$tarfile' -C '$part_local'"
  run "find '$part_local' -mindepth 2 -name '*.csv' -type f -exec mv -f -t '$part_local' {} +"
  run "find '$part_local' -mindepth 1 -type d -empty -delete || true"

  # 2) 計數（非 DRY_RUN 時才有實檔）：只數當天那批
  if [ "$DRY_RUN" != "1" ]; then
    n=$(find "$part_local" -maxdepth 1 -name "TDCS_${DTYPE}_${cur}_"'*.csv' -type f | wc -l)
    if [ "$n" -eq 0 ]; then
      log "[$cur] 解壓後沒有 CSV,標記失敗"
      fail=$((fail+1))
      cur=$(date -d "$y-$m-${cur:6:2} +1 day" +%Y%m%d)
      continue
    fi
    log "[$cur] 攤平完成,取得 $n 個 CSV(保留在本機 staging,交由 put_to_hdfs.sh 上傳)"
  fi

  ok=$((ok+1))
  cur=$(date -d "$y-$m-${cur:6:2} +1 day" +%Y%m%d)
done

log "完成。成功 $ok | 缺源 $miss | 失敗 $fail"
log "下一步：對每個 year/month 分區執行 03_shared/put_to_hdfs.sh ${DTYPE} <year> <month>"
