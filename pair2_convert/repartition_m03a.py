#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
repartition_m03a.py
====================
把 /dataset/M03A/year=YYYY/month=MM/day=DD/*.parquet
重寫成          /dataset/M03A_new/year=YYYY/month=MM/*.parquet

功能：
  1. 逐年逐月處理 (預設 2022-01 ~ 2026-<目前有資料的最後一個月>)
  2. 自動偵測該 partition 是否存在，不存在就跳過
  3. 依照來源資料在 HDFS 上的實際大小 (含副本) 估算目標檔案數
     (預設每個輸出檔案約 128MB)，讓每個 month partition 檔案數/大小一致
  4. 用 repartition(N, "year", "month") 確保同一個 (year, month)
     partition 內恰好切成 N 個檔案，不同 partition 之間不互相污染
  5. 寫完後立即讀回來做「筆數校驗」(source count vs target count)，
     不一致會記錄 ERROR 並繼續處理下一個 partition（不中斷整體流程）
  6. 每個 partition、以及整體流程的花費時間，會同時印到 console 和寫入 log 檔

使用方式：
  spark-submit \
      --master yarn \
      --conf spark.sql.shuffle.partitions=200 \
      repartition_m03a.py \
      --src /dataset/M03A \
      --dst /dataset/M03A_new \
      --start-year 2022 --end-year 2026 \
      --target-file-mb 128 \
      --min-files 1 --max-files 64 \
      --log-file /home/bigred/logs/repartition_m03a.log

備註：
  - 預設「不」直接覆寫原始路徑，先寫到 --dst 的新路徑，
    確認資料無誤後，再自行用 `hdfs dfs -mv` 切換，或改 Hive metastore 的
    table location，比較安全。
  - 如果 Hive 上有對應的 external table，記得處理完後：
        ALTER TABLE m03a DROP PARTITION (day=...)  -- 如果舊 table 保留 day
    或直接重建 table 並 MSCK REPAIR TABLE / RECOVER PARTITIONS。
"""

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# --------------------------------------------------------------------------- #
# 基礎工具：呼叫 hdfs 指令
# --------------------------------------------------------------------------- #

# 注意：本腳本刻意不使用 `subprocess` 呼叫 `hdfs dfs ...` CLI 指令。
# 每次呼叫 hdfs CLI 都會另外啟動一個全新的 JVM process，在容器化環境
# (例如 Kubernetes pod 的 cgroup pids.max 限制) 下，會跟 Spark driver
# 一起搶佔本來就緊繃的 pid 額度，容易導致 pthread_create 失敗。
# 所有 HDFS 操作一律透過 Spark driver 既有 JVM 內的 Hadoop FileSystem
# API 完成，不額外開 process。


def get_hadoop_fs(spark: SparkSession):
    """取得跟 Spark driver 共用同一個 JVM 的 Hadoop FileSystem 物件"""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm = spark._jvm
    return jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf), jvm


def hdfs_path_exists(spark: SparkSession, path: str) -> bool:
    """
    直接用 Hadoop FileSystem API 判斷路徑是否存在，不另外開 process。
    原本用 subprocess 呼叫 `hdfs dfs -test -e` 的問題是：每次呼叫都會
    啟動一個全新的 JVM process，在 Kubernetes pod 的 cgroup pids.max
    額度很緊的情況下（例如 2048），逐月累積下來很容易把 pid 額度榨乾，
    這通常比 Spark driver 自己的 thread 數量更早撞到上限。
    """
    fs, jvm = get_hadoop_fs(spark)
    p = jvm.org.apache.hadoop.fs.Path(path)
    return bool(fs.exists(p))


def hdfs_dir_size_bytes(spark: SparkSession, path: str) -> Optional[int]:
    """
    用 Hadoop FileSystem 的 getContentSummary 取得目錄大小 (bytes)，
    效果等同 `hdfs dfs -du -s` 的第一欄 (raw size)，但不需要開新 process。
    """
    fs, jvm = get_hadoop_fs(spark)
    p = jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(p):
        return None
    try:
        summary = fs.getContentSummary(p)
        return int(summary.getLength())
    except Exception:  # noqa: BLE001
        return None


def list_existing_days(spark: SparkSession, path: str) -> List[str]:
    """列出 month 目錄下有哪些 day=xx 子目錄，僅用於 log 顯示參考"""
    fs, jvm = get_hadoop_fs(spark)
    p = jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(p):
        return []
    days = []
    for status in fs.listStatus(p):
        name = status.getPath().getName()
        if name.startswith("day="):
            days.append(name.split("day=")[-1])
    return days


# --------------------------------------------------------------------------- #
# 設定 logging：同時輸出到 console 及 log 檔
# --------------------------------------------------------------------------- #

def setup_logger(log_file: str) -> logging.Logger:
    logger = logging.getLogger("repartition_m03a")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger


def fmt_duration(seconds: float) -> str:
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{int(h):02d}:{int(m):02d}:{s:05.2f}"


# --------------------------------------------------------------------------- #
# 斷點續跑：進度檔 (state file)
# --------------------------------------------------------------------------- #
#
# 每處理完一個 partition 就用 append 模式寫一行 JSON 到 state file，
# 並立刻 flush + fsync，確保即使程式被砍掉，已經寫進去的紀錄不會遺失。
# 下次啟動時讀取這個檔案，只有狀態是 "OK" 的 (year, month) 才會被跳過，
# "MISMATCH" / "ERROR" 的月份會被視為未完成，重新處理一次
# (因為 write.mode("overwrite") 本來就會整個 partition 覆寫，安全可重跑)。

def load_completed_partitions(state_file: str, logger: logging.Logger) -> Set[Tuple[int, int]]:
    """讀取 state file，回傳已經標記為 OK 的 (year, month) 集合"""
    completed: Dict[Tuple[int, int], str] = {}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    key = (int(rec["year"]), int(rec["month"]))
                    completed[key] = rec["status"]  # 同一個 partition 後面的紀錄覆蓋前面的
                except (json.JSONDecodeError, KeyError, ValueError):
                    continue
    except FileNotFoundError:
        logger.info(f"找不到 state file ({state_file})，視為全新開始")
        return set()

    ok_keys = {k for k, v in completed.items() if v == "OK"}
    logger.info(
        f"讀取到 state file: {state_file}，共 {len(completed)} 筆紀錄，"
        f"其中 {len(ok_keys)} 個 partition 已標記 OK，將會跳過"
    )
    return ok_keys


def append_state(state_file: str, result: "MonthResult") -> None:
    """把單一 partition 的結果即時寫進 state file (append + flush + fsync)"""
    rec = {
        "year": result.year,
        "month": result.month,
        "status": result.status,
        "src_count": result.src_count,
        "dst_count": result.dst_count,
        "dst_files": result.dst_files,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }
    with open(state_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


# --------------------------------------------------------------------------- #
# 每個 (year, month) partition 的處理結果紀錄
# --------------------------------------------------------------------------- #

@dataclass
class MonthResult:
    year: int
    month: int
    status: str = "SKIPPED"     # SKIPPED / OK / MISMATCH / ERROR
    src_files: int = 0
    dst_files: int = 0
    src_size_mb: float = 0.0
    src_count: int = 0
    dst_count: int = 0
    duration_sec: float = 0.0
    message: str = ""


def count_parquet_files(spark: SparkSession, path: str) -> int:
    """直接透過 driver JVM 內建的 Hadoop FileSystem API 列出檔案數"""
    fs, jvm = get_hadoop_fs(spark)
    p = jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(p):
        return 0
    statuses = fs.listStatus(p)
    count = 0
    for s in statuses:
        name = s.getPath().getName()
        if name.endswith(".parquet"):
            count += 1
    return count


def estimate_num_files(size_bytes: int, target_file_mb: int, min_files: int, max_files: int) -> int:
    """依照來源大小估算目標檔案數，並限制在 [min_files, max_files] 之間"""
    target_bytes = target_file_mb * 1024 * 1024
    n = max(1, round(size_bytes / target_bytes))
    n = max(min_files, min(max_files, n))
    return n


def process_month(
    spark: SparkSession,
    logger: logging.Logger,
    src_base: str,
    dst_base: str,
    year: int,
    month: int,
    target_file_mb: int,
    min_files: int,
    max_files: int,
    sort_columns: Optional[List[str]] = None,
) -> MonthResult:
    month_str = f"{month:02d}"
    src_path = f"{src_base}/year={year}/month={month_str}"
    dst_path = dst_base  # partitionBy 會自動長出 year=/month= 子目錄

    result = MonthResult(year=year, month=month)
    t0 = time.time()

    if not hdfs_path_exists(spark, src_path):
        result.status = "SKIPPED"
        result.message = f"來源路徑不存在: {src_path}"
        logger.info(f"[{year}-{month_str}] 跳過，路徑不存在: {src_path}")
        return result

    try:
        # 1) 估算目標檔案數
        size_bytes = hdfs_dir_size_bytes(spark, src_path)
        if size_bytes is None or size_bytes == 0:
            result.status = "SKIPPED"
            result.message = "無法取得大小或大小為 0"
            logger.warning(f"[{year}-{month_str}] {result.message}，跳過")
            return result

        num_files = estimate_num_files(size_bytes, target_file_mb, min_files, max_files)
        result.src_size_mb = round(size_bytes / (1024 * 1024), 2)

        # 動態調整 shuffle partitions，跟這個月要切的檔案數一致。
        # 如果整個 job 都用固定的 --conf spark.sql.shuffle.partitions=200，
        # 對這種每月才 20~30MB 的小資料來說會開出遠超過需要的 shuffle task /
        # thread，逐月疊加下去很容易把系統的 thread/process 上限榨乾
        # (pthread_create failed / unable to create native thread)。
        shuffle_partitions = max(num_files, 1)
        spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

        days = list_existing_days(spark, src_path)
        logger.info(
            f"[{year}-{month_str}] 來源大小={result.src_size_mb} MB, "
            f"day partitions={len(days)}, 目標檔案數={num_files}"
        )

        # 2) 讀取整個 month 底下所有 day，拿掉 day 欄位
        # 注意：一定要指定 basePath = src_base，Spark 才會把 year / month
        # 也當成 partition 欄位解析出來；否則因為 year=xxx/month=xxx 已經
        # 寫死在起始路徑裡，Spark 只會把再下一層的 day=xx 當成 partition 欄位，
        # year、month 完全不會出現在 DataFrame schema 裡。
        df = spark.read.option("basePath", src_base).parquet(src_path)
        if "day" in df.columns:
            df = df.drop("day")
        # year / month 欄位由 partition 路徑推斷產生。這裡強制轉成「補零字串」
        # (例如 month -> "01")，確保寫出來的資料夾路徑格式
        # (year=2026/month=01) 跟原本 2022~2025 的命名慣例完全一致，
        # 不會因為 Spark 把型別推斷成 int 而讓 month=01 變成 month=1。
        df = df.withColumn("year", F.col("year").cast("string"))
        df = df.withColumn(
            "month", F.lpad(F.col("month").cast("string"), 2, "0")
        )

        src_count = df.count()
        result.src_count = src_count
        result.src_files = num_files  # 記錄用，实际來源檔案數見 hdfs -ls，這裡先不重複列

        # 3) 合併成目標檔案數
        #
        # 重要修正：這個 DataFrame 本來就已經鎖定在單一一個 (year, month)，
        # 用 repartition(N, "year", "month") 依這兩個「值全部相同」的欄位
        # 做 hash shuffle 完全沒有分流意義，只會白白觸發一次全量網路
        # shuffle，把原本 30 個 day 檔案依時間循序寫入的物理順序打亂、
        # 重新交錯排列，導致 Parquet 的 dictionary/RLE 壓縮效率下降，
        # 檔案反而變大。
        #
        # 改用 coalesce()：只在同一個 executor 內合併相鄰 partition，
        # 不經過網路 shuffle，原始資料順序完整保留，效果等同把 30 個小檔案
        # 依原本順序首尾接起來，壓縮率應該回到接近 (甚至優於) 原始檔案。
        #
        # coalesce 只能「減少」partition 數；如果 num_files 剛好比目前來源
        # 的 partition 數還多 (少見，例如來源只有 1 個檔案但想切成更多份)，
        # coalesce 做不到，必須退回用 repartition (此時無可避免要 shuffle)。
        current_partitions = df.rdd.getNumPartitions()
        if num_files >= current_partitions:
            if num_files > current_partitions:
                logger.info(
                    f"[{year}-{month_str}] 目標檔案數({num_files}) > 來源 partition 數"
                    f"({current_partitions})，需要 shuffle 才能增加檔案數"
                )
            writer_df = df.repartition(num_files) if num_files > current_partitions else df
        else:
            writer_df = df.coalesce(num_files)

        if sort_columns:
            existing_sort_cols = [c for c in sort_columns if c in writer_df.columns]
            if existing_sort_cols:
                writer_df = writer_df.sortWithinPartitions(*existing_sort_cols)

        (
            writer_df
            .write.mode("overwrite")
            .partitionBy("year", "month")
            .parquet(dst_path)
        )

        # 4) 校驗筆數
        written_path = f"{dst_path}/year={year}/month={month_str}"
        df_check = spark.read.parquet(written_path)
        dst_count = df_check.count()
        result.dst_count = dst_count

        # 順便統計實際寫出的檔案數：改用 JVM 內建的 Hadoop FileSystem API，
        # 不透過 subprocess 開新 process，避免在系統 thread/process 資源
        # 緊張時 (例如前面提到的 pthread_create 問題) 連 `hdfs dfs -ls`
        # 這種外部指令都開不出來，導致統計失準。
        try:
            actual_files = count_parquet_files(spark, written_path)
        except Exception as list_exc:  # noqa: BLE001
            logger.warning(f"[{year}-{month_str}] 統計檔案數失敗，改用估算值: {list_exc}")
            actual_files = num_files
        result.dst_files = actual_files

        if src_count == dst_count:
            result.status = "OK"
            result.message = "筆數校驗通過"
            logger.info(
                f"[{year}-{month_str}] 完成，來源筆數={src_count}, 目標筆數={dst_count}, "
                f"輸出檔案數={actual_files}"
            )
        else:
            result.status = "MISMATCH"
            result.message = f"筆數不一致！來源={src_count}, 目標={dst_count}"
            logger.error(f"[{year}-{month_str}] {result.message}")

    except Exception as exc:  # noqa: BLE001
        result.status = "ERROR"
        result.message = str(exc)
        logger.exception(f"[{year}-{month_str}] 處理失敗: {exc}")

    finally:
        # 每個月結束後主動清一下 cache / broadcast，減少長時間迴圈下的資源累積，
        # 降低系統 thread/process 資源被逐月榨乾的風險。
        try:
            spark.catalog.clearCache()
        except Exception:  # noqa: BLE001
            pass
        result.duration_sec = time.time() - t0
        logger.info(f"[{year}-{month_str}] 花費時間: {fmt_duration(result.duration_sec)}")

    return result


def get_available_months_2026(spark: SparkSession, src_base: str, logger: logging.Logger) -> List[int]:
    """2026 年資料尚未跑完整年，動態偵測目前有哪些 month 目錄存在"""
    months = []
    for m in range(1, 13):
        path = f"{src_base}/year=2026/month={m:02d}"
        if hdfs_path_exists(spark, path):
            months.append(m)
    logger.info(f"偵測到 2026 年目前已有資料的月份: {months}")
    return months


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def parse_args():
    parser = argparse.ArgumentParser(description="M03A day->month repartition batch job")
    parser.add_argument("--src", default="/dataset/M03A", help="來源 base 路徑 (含 year=/month=/day=)")
    parser.add_argument("--dst", default="/dataset/M03A_new", help="輸出 base 路徑 (year=/month=)")
    parser.add_argument("--start-year", type=int, default=2022)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--target-file-mb", type=int, default=128, help="每個輸出檔案的目標大小 (MB)")
    parser.add_argument("--min-files", type=int, default=1, help="每個 partition 最少檔案數")
    parser.add_argument("--max-files", type=int, default=64, help="每個 partition 最多檔案數")
    parser.add_argument(
        "--restart-every",
        type=int,
        default=12,
        help="每處理幾個 partition 就重啟一次 SparkSession 釋放資源 (設 0 代表不重啟)",
    )
    parser.add_argument(
        "--max-months",
        type=int,
        default=0,
        help="本輪最多處理幾個 partition 就結束 (exit code 3=還有剩)。"
             "SparkSession 重啟無法完全歸還 driver JVM 的 thread 額度"
             "(dtadm pod 有 cgroup pids 上限)，用外部 wrapper 每輪換全新 JVM 才徹底；"
             "0=不限",
    )
    parser.add_argument(
        "--sort-columns",
        default="",
        help="寫檔前依這些欄位重新排序 (逗號分隔)，用於改善壓縮率；"
             "預設空字串代表不排序 (改用 coalesce 保留原始資料順序，"
             "通常不需要額外排序)",
    )
    parser.add_argument(
        "--state-file",
        default="repartition_m03a_state.jsonl",
        help="斷點續跑進度檔路徑；已標記 OK 的 partition 下次執行會自動跳過",
    )
    parser.add_argument(
        "--force-redo",
        action="store_true",
        help="忽略 state file，全部 partition 重新處理 (不會刪除 state file 內容，仍會 append 新紀錄)",
    )
    parser.add_argument(
        "--log-file",
        default=f"repartition_m03a_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        help="log 檔輸出路徑",
    )
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("M03A_repartition_day_to_month")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def main():
    args = parse_args()
    logger = setup_logger(args.log_file)

    logger.info("=" * 70)
    logger.info("M03A day -> month repartition 批次作業開始")
    logger.info(f"來源: {args.src}    輸出: {args.dst}")
    logger.info(f"年份範圍: {args.start_year} ~ {args.end_year}")
    logger.info(f"目標檔案大小: {args.target_file_mb} MB, 檔案數限制: [{args.min_files}, {args.max_files}]")
    logger.info(f"log 檔: {args.log_file}")
    logger.info(f"每處理 {args.restart_every} 個 partition 會重啟一次 SparkSession，避免 thread/資源長期累積")
    logger.info(f"state file: {args.state_file}  (force_redo={args.force_redo})")
    logger.info("=" * 70)

    if args.force_redo:
        completed: Set[Tuple[int, int]] = set()
        logger.info("--force-redo 已指定，忽略 state file，全部重新處理")
    else:
        completed = load_completed_partitions(args.state_file, logger)

    sort_columns = [c.strip() for c in args.sort_columns.split(",") if c.strip()]
    logger.info(f"寫檔前排序欄位: {sort_columns if sort_columns else '(不排序)'}")

    spark = build_spark_session()

    overall_start = time.time()
    results: List[MonthResult] = []
    processed_since_restart = 0
    total_processed = 0          # 本輪實際處理數（配 --max-months 分輪）
    stopped_early = False

    for year in range(args.start_year, args.end_year + 1):
        if stopped_early:
            break
        if year == 2026:
            months = get_available_months_2026(spark, args.src, logger)
        else:
            months = list(range(1, 13))

        for month in months:
            if args.max_months > 0 and total_processed >= args.max_months:
                logger.info(f"本輪已處理 {total_processed} 個 partition，"
                            "達 --max-months 上限，結束本輪（exit 3）")
                stopped_early = True
                break
            if (year, month) in completed:
                logger.info(f"[{year}-{month:02d}] 已在 state file 標記 OK，跳過")
                continue

            res = process_month(
                spark=spark,
                logger=logger,
                src_base=args.src,
                dst_base=args.dst,
                year=year,
                month=month,
                target_file_mb=args.target_file_mb,
                min_files=args.min_files,
                max_files=args.max_files,
                sort_columns=sort_columns,
            )
            results.append(res)

            # partition 一處理完就立刻寫進 state file，即使程式接下來被中斷，
            # 這筆紀錄也不會遺失，下次啟動可以正確接續。
            if res.status != "SKIPPED":
                append_state(args.state_file, res)

            # 路徑不存在被跳過的不算在重啟計數內，只算真正執行過的 partition
            if res.status != "SKIPPED":
                processed_since_restart += 1
                total_processed += 1

            if args.restart_every > 0 and processed_since_restart >= args.restart_every:
                logger.info(
                    f"已累積處理 {processed_since_restart} 個 partition，"
                    f"重啟 SparkSession 釋放 JVM 資源 (thread / memory map)..."
                )
                try:
                    spark.stop()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(3)  # 給 OS 一點時間回收資源
                spark = build_spark_session()
                processed_since_restart = 0

    overall_duration = time.time() - overall_start

    # ------------------------------------------------------------------- #
    # 總結報告
    # ------------------------------------------------------------------- #
    logger.info("=" * 70)
    logger.info("批次作業總結")
    logger.info("=" * 70)

    header = f"{'年-月':<10}{'狀態':<10}{'來源大小(MB)':<14}{'來源筆數':<12}{'目標筆數':<12}{'輸出檔案數':<10}{'花費時間':<12}"
    logger.info(header)
    logger.info("-" * len(header))

    ok_count = mismatch_count = error_count = skipped_count = 0
    for r in results:
        logger.info(
            f"{r.year}-{r.month:02d}   {r.status:<10}{r.src_size_mb:<14}{r.src_count:<12}"
            f"{r.dst_count:<12}{r.dst_files:<10}{fmt_duration(r.duration_sec):<12}"
        )
        if r.status == "OK":
            ok_count += 1
        elif r.status == "MISMATCH":
            mismatch_count += 1
        elif r.status == "ERROR":
            error_count += 1
        else:
            skipped_count += 1

    logger.info("-" * len(header))
    logger.info(
        f"總計: OK={ok_count}, MISMATCH={mismatch_count}, ERROR={error_count}, SKIPPED={skipped_count}"
    )
    logger.info(f"整體批次作業總花費時間: {fmt_duration(overall_duration)}")
    logger.info("=" * 70)

    if mismatch_count > 0 or error_count > 0:
        logger.warning(
            "有 partition 校驗失敗或處理錯誤，請檢查 log 內容，"
            "在確認前不要把 --dst 切換成正式路徑！"
        )

    spark.stop()
    if stopped_early:
        sys.exit(3)   # 還有 partition 沒處理，wrapper 據此換新 JVM 續跑


if __name__ == "__main__":
    main()
