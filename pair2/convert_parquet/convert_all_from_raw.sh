#!/bin/bash
# convert_all_from_raw.sh — 一鍵批次（2026-07-16）：
#   1) M06A：raw CSV → /dataset/M06A 補缺月（已存在的月自動跳過）
#   2) M03A：舊 day 分區樹 → /dataset/M03A_new（year/month，逐月筆數驗證）
#   3) 驗證全 OK 才把 M03A_new 換名為 M03A（舊樹留 M03A_day_old，確認後手動刪）
#   4) M03A：raw 補轉 2026-06、2026-07（舊樹只到 2026-05）
# 在 dtadm 內以 nohup 執行；各步驟 log 集中在 $LOG。
set -uo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
SUBMIT="spark-submit --master yarn --deploy-mode client \
  --driver-memory 2g --num-executors 2 --executor-memory 3g"

echo "=== [1/4] M06A raw→parquet 補缺月 ==="
while :; do
  $SUBMIT "$DIR/convert_raw_to_parquet.py" M06A --max-months 2
  rc=$?
  [ $rc -eq 3 ] && { echo "--- 換新 JVM 續跑 ---"; sleep 3; continue; }
  break
done
[ $rc -ne 0 ] && echo "!!! M06A 轉換失敗（rc=$rc），中止" && exit $rc

echo "=== [2/4] M03A day→month 重寫 ==="
# state/log 一律放共享掛載（~/wulin）：dtadm pod 的 /home/bigred 非持久，
# pod 重啟 log 就沒了（2026-07-16 已踩過）
STATE_DIR="$DIR/logs"; mkdir -p "$STATE_DIR"
# --max-months 4：每輪全新 JVM。M03A 舊樹幾千個 day 小目錄，列檔的執行緒池
# 會把 driver 的 pids 額度榨乾（2026-07-17 實測 5 個月內就懸死），
# script 內部的 SparkSession 重啟不夠徹底，必須整個 JVM 換掉。
while :; do
  $SUBMIT --conf spark.sql.shuffle.partitions=20 \
    "$DIR/repartition_m03a.py" \
    --src /dataset/M03A --dst /dataset/M03A_new \
    --max-months 4 \
    --state-file "$STATE_DIR/repart_m03a_state.jsonl" \
    --log-file "$STATE_DIR/repart_m03a.log"
  rc=$?
  [ $rc -eq 3 ] && { echo "--- repartition 換新 JVM 續跑 ---"; sleep 3; continue; }
  break
done

SUMMARY=$(grep "總計" "$STATE_DIR/repart_m03a.log" | tail -1)
echo "repartition 結果：$SUMMARY"
if ! echo "$SUMMARY" | grep -q "MISMATCH=0, ERROR=0"; then
  echo "!!! M03A 重寫有 MISMATCH/ERROR，不換名，請看 $HOME/repart_m03a.log"
  exit 1
fi

echo "=== [3/4] 換名（舊樹保留為 M03A_day_old）==="
hdfs dfs -mv /dataset/M03A /dataset/M03A_day_old || exit 1
hdfs dfs -mv /dataset/M03A_new /dataset/M03A || exit 1
echo "換名完成"

echo "=== [4/4] M03A raw 補轉 2026-06、2026-07 ==="
$SUBMIT "$DIR/convert_raw_to_parquet.py" M03A --months 2026-06,2026-07
rc=$?
[ $rc -ne 0 ] && echo "!!! M03A 2026-06/07 補轉失敗（rc=$rc）" && exit $rc

echo "=== ALL DONE ==="
