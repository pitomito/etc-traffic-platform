#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
convert_raw_to_parquet.py — /raw CSV → 年月分區 Parquet（純轉換，零清洗）
=========================================================================
把 hdfs:///raw/<DTYPE>/year=YYYY/month=MM/TDCS_*.csv 一個月一批轉成
hdfs:///dataset/<DTYPE>_new/year=YYYY/month=MM/*.parquet。

與 convert_batch.py（每日、寫 Iceberg）不同，這支是手動批次工具，輸出
跟 raw/parquet_output 上傳的格式一致：year=/month= 目錄、月內單檔（預設）、
檔內只有原始資料欄位（不塞 year/month/day 欄位，讀取時由目錄路徑推斷）。

零清洗原則：
  - 保留全部欄位（M06A 含 TripInformation）
  - 不過濾、不去重、不丟 malformed row（PERMISSIVE：欄位解析失敗補 null，
    整列仍保留）
  - 唯一的轉換是 schema 型別（VehicleType int、TripLength double 等）

注意：/raw 目前只有 2026 年（抓檔 pipeline 2026 才上線），2021-2025 的
歷史沒有 raw CSV 可轉，只存在於 parquet_output 上傳的 M06A_new 裡。

用法（dtadm 內；已轉過的月份自動跳過，重轉加 --force）：
  spark-submit --master yarn --deploy-mode client \
      --driver-memory 2g --num-executors 2 --executor-memory 3g \
      ~/wulin/pair2/convert_parquet/convert_raw_to_parquet.py M06A
  # 指定範圍／輸出：
  #   ... convert_raw_to_parquet.py M06A --months 2026-01,2026-02 --dst hdfs:///dataset/M06A_new
  # 月份多時建議分輪跑（每輪全新 JVM，避開 pods 上限）：--max-months 3，
  # exit code 3 = 還有月份沒轉，再跑一次即可。
"""
import argparse
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

SCHEMAS = {
    "M03A": StructType([
        StructField("TimeInterval", StringType()),
        StructField("GantryID",     StringType()),
        StructField("Direction",    StringType()),
        StructField("VehicleType",  IntegerType()),
        StructField("Volume",       IntegerType()),
    ]),
    "M06A": StructType([
        StructField("VehicleType",      IntegerType()),
        StructField("DetectionTime_O",  StringType()),
        StructField("GantryID_O",       StringType()),
        StructField("DetectionTime_D",  StringType()),
        StructField("GantryID_D",       StringType()),
        StructField("TripLength",       DoubleType()),
        StructField("TripEnd",          StringType()),
        StructField("TripInformation",  StringType()),   # 保留，不清洗
    ]),
}


def log(msg):
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def _fs(spark):
    jvm = spark._jvm
    return jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()), jvm


def hdfs_exists(spark, path):
    fs, jvm = _fs(spark)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def list_raw_months(spark, src_base):
    """列出 raw 實際存在的 (year, month)，由舊到新。"""
    fs, jvm = _fs(spark)
    months = []
    base = jvm.org.apache.hadoop.fs.Path(src_base)
    if not fs.exists(base):
        return months
    for ydir in fs.listStatus(base):
        yname = ydir.getPath().getName()
        if not yname.startswith("year="):
            continue
        for mdir in fs.listStatus(ydir.getPath()):
            mname = mdir.getPath().getName()
            if mname.startswith("month="):
                months.append((yname.split("=")[1], mname.split("=")[1]))
    return sorted(months)


def convert_month(spark, dtype, src_base, dst_base, year, month,
                  files_per_month, verify):
    src = f"{src_base}/year={year}/month={month}/TDCS_{dtype}_*.csv"
    dst = f"{dst_base}/year={year}/month={month}"

    df = (spark.read.option("header", False)
          .schema(SCHEMAS[dtype]).csv(src))
    src_count = df.count() if verify else None

    (df.coalesce(files_per_month)
       .write.mode("overwrite").parquet(dst))

    if verify:
        dst_count = spark.read.parquet(dst).count()
        status = "OK" if src_count == dst_count else "MISMATCH"
        log(f"  [{year}-{month}] {status}：CSV {src_count} / parquet {dst_count}")
        return status == "OK"
    log(f"  [{year}-{month}] 完成（未驗證筆數）")
    return True


def main():
    p = argparse.ArgumentParser(description="/raw CSV → 年月分區 Parquet（零清洗）")
    p.add_argument("dtype", choices=list(SCHEMAS))
    p.add_argument("--src", default=None, help="raw base（預設 hdfs:///raw/<DTYPE>）")
    p.add_argument("--dst", default=None,
                   help="輸出 base（預設 hdfs:///dataset/<DTYPE>，統一樹）")
    p.add_argument("--months", default=None,
                   help="只轉這些月，逗號分隔 YYYY-MM（預設：raw 有的全部）")
    p.add_argument("--files-per-month", type=int, default=1,
                   help="每月輸出檔數（預設 1，與 parquet_output 格式一致）")
    p.add_argument("--force", action="store_true", help="已存在的月份也重轉")
    p.add_argument("--no-verify", action="store_true", help="跳過筆數驗證（快）")
    p.add_argument("--max-months", type=int, default=0,
                   help="本輪最多轉幾個月就結束（exit 3=還有剩），0=不限")
    a = p.parse_args()

    src_base = a.src or f"hdfs:///raw/{a.dtype}"
    dst_base = a.dst or f"hdfs:///dataset/{a.dtype}"

    spark = (SparkSession.builder
             .appName(f"convert_raw_{a.dtype}_to_parquet")
             .config("spark.task.cpus", "1")   # 蓋掉 dtadm conf 飄移的 task.cpus=4
             .config("spark.sql.shuffle.partitions", "20")
             .getOrCreate())

    months = list_raw_months(spark, src_base)
    if a.months:
        wanted = {tuple(m.split("-")) for m in a.months.split(",")}
        months = [m for m in months if m in wanted]
        missing = wanted - set(months)
        if missing:
            log(f"警告：raw 沒有這些月份，略過：{sorted(missing)}")
    if not months:
        raise SystemExit(f"raw（{src_base}）沒有可轉的月份")

    # 跳過判斷用「月份目錄存在」：上傳版月份沒有 _SUCCESS 標記，用 _SUCCESS
    # 判斷會誤把它們重轉覆蓋
    todo = [m for m in months
            if a.force or not hdfs_exists(
                spark, f"{dst_base}/year={m[0]}/month={m[1]}")]
    batch = todo[:a.max_months] if a.max_months > 0 else todo
    log(f"===== {a.dtype}：raw {len(months)} 個月，待轉 {len(todo)}，本輪 {len(batch)} "
        f"→ {dst_base} =====")

    ok = fail = 0
    for year, month in batch:
        try:
            log(f"  [{year}-{month}] 轉換中…")
            good = convert_month(spark, a.dtype, src_base, dst_base, year, month,
                                 a.files_per_month, not a.no_verify)
            ok += good
            fail += not good
        except Exception as e:  # noqa: BLE001
            log(f"  [{year}-{month}] ERROR：{e}")
            fail += 1
        finally:
            spark.catalog.clearCache()

    remaining = len(todo) - len(batch)
    log(f"===== 本輪結束：OK {ok} | 失敗 {fail} | 還剩 {remaining} 個月 =====")
    spark.stop()
    raise SystemExit(1 if fail else (3 if remaining else 0))


if __name__ == "__main__":
    main()
