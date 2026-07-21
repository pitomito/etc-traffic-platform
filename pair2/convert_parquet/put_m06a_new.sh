#!/bin/bash
# put_m06a_new.sh — 一次性：把 raw/parquet_output/（重做的 M06A 月分區 parquet，
# 保留 TripInformation）上傳到 HDFS /dataset/M06A_new/。
# 逐月冪等：目標已存在且大小一致就跳過，中斷後重跑會自動接續。
# 刻意先傳到 M06A_new 而不是直接蓋 /dataset/M06A：
#   舊 M06A 是 day= 分區、新資料是 month 一檔，兩種目錄深度混在同一棵樹
#   Spark 會讀不了（conflicting directory structures）；且每晚 DAG 的舊版
#   convert 還在往 /dataset/M06A 寫 day 分區。等 Iceberg 遷移＋DAG 切換完成
#   後，再一次把舊樹汰換掉。
set -uo pipefail

# 2026-07-16 更新：本機來源已改名 raw/M06A_new（只剩 2024、2025）；
# 目標統一為 /dataset/M06A。跳過判斷改成「月份目錄已存在」——
# 因為 unify_m06a_schema.py 重寫型別後檔案大小會變，舊的大小比對
# 會誤判成「要重傳」，把統一過型別的月份蓋回全 string 版。
SRC=/home/bigred/wulin/raw/M06A_new
DST=/dataset/M06A

ok=0; skip=0; fail=0
for d in "$SRC"/year=*/month=*; do
  rel=${d#"$SRC"/}
  f="$d/data.parquet"
  if [ ! -f "$f" ]; then
    echo "[MISS] $rel 沒有 data.parquet"; fail=$((fail+1)); continue
  fi
  lsize=$(stat -c %s "$f")
  if hdfs dfs -test -d "$DST/$rel" 2>/dev/null; then
    echo "[SKIP] $rel（月份目錄已存在）"; skip=$((skip+1)); continue
  fi
  echo "[PUT ] $rel（$((lsize/1024/1024)) MB）…"
  if hdfs dfs -mkdir -p "$DST/$rel" && hdfs dfs -put -f "$f" "$DST/$rel/data.parquet"; then
    ok=$((ok+1)); echo "[OK  ] $rel"
  else
    fail=$((fail+1)); echo "[FAIL] $rel"
    # HDFS 掛了就立刻停（NameNode heap 只有 384m，連續大量寫入曾把它壓垮、
    # 連帶 datanode 一起重啟），不要對著死掉的叢集連噴失敗
    if ! hdfs dfs -test -d / 2>/dev/null; then
      echo "!!! HDFS 連不上，中止。叢集恢復後重跑本腳本即可接續"
      exit 2
    fi
  fi
  sleep 15   # 月與月之間喘口氣，讓 NameNode/DataNode 消化 block report
done
echo "=== 上傳結束：OK $ok | SKIP $skip | FAIL $fail ==="
[ "$fail" -eq 0 ] || exit 1
