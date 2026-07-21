#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
unify_m06a_schema.py — 統一 M06A parquet 樹的欄位型別（string 版 → 正確型別）
==============================================================================
背景：/dataset/M06A 裡混了兩種產出——
  - parquet_output 上傳版：全部欄位 string（含 VehicleType、TripLength）
  - convert_raw_to_parquet.py 版：VehicleType int、TripLength double
  兩種混讀，Spark 會報 Parquet column cannot be converted（Expected string,
  Found INT32）。本腳本掃描整棵樹，把 string 型別的月份重寫成正確型別。

安全設計：
  - 逐月處理；先寫到 <base>/.retype_tmp/，筆數驗證通過才刪舊目錄、原子搬入。
    中途被砍最多留下 tmp 垃圾，不會弄丟任何月份。
  - 已是正確型別的月份自動跳過，重跑無害。
  - --max-months N + exit 3：分輪跑、每輪全新 JVM（dtadm pids 上限對策）。

用法（dtadm 內）：
  while :; do
    spark-submit --master yarn --deploy-mode client \
        --driver-memory 2g --num-executors 2 --executor-memory 3g \
        ~/wulin/pair2/convert_parquet/unify_m06a_schema.py --max-months 4
    rc=$?; [ $rc -eq 3 ] || break; sleep 3
  done
"""
import argparse
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 目標型別（其餘欄位維持 string）
TARGET_CASTS = {"VehicleType": "int", "TripLength": "double"}


def log(msg):
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def _fs(spark):
    jvm = spark._jvm
    return jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()), jvm


def list_months(spark, base):
    fs, jvm = _fs(spark)
    months = []
    b = jvm.org.apache.hadoop.fs.Path(base)
    if not fs.exists(b):
        return months
    for ydir in fs.listStatus(b):
        yname = ydir.getPath().getName()
        if not yname.startswith("year="):
            continue
        for mdir in fs.listStatus(ydir.getPath()):
            mname = mdir.getPath().getName()
            if mname.startswith("month="):
                months.append((yname.split("=")[1], mname.split("=")[1]))
    return sorted(months)


def needs_retype(spark, path):
    """讀 footer schema（不掃資料），string 型別的目標欄位就要重寫。"""
    sch = spark.read.parquet(path).schema
    for f in sch.fields:
        if f.name in TARGET_CASTS and f.dataType.simpleString() == "string":
            return True
    return False


def retype_month(spark, base, year, month, files_per_month):
    src = f"{base}/year={year}/month={month}"
    tmp = f"{base}/.retype_tmp/year={year}/month={month}"
    fs, jvm = _fs(spark)
    P = jvm.org.apache.hadoop.fs.Path

    df = spark.read.parquet(src)
    for c, t in TARGET_CASTS.items():
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(t))
    src_count = df.count()

    (df.coalesce(files_per_month)
       .write.mode("overwrite").parquet(tmp))

    dst_count = spark.read.parquet(tmp).count()
    if src_count != dst_count:
        raise RuntimeError(f"筆數不符：來源 {src_count} / 重寫 {dst_count}（tmp 保留於 {tmp}）")

    # 驗證通過才換入：刪舊、搬新（HDFS rename 是原子操作）
    fs.delete(P(src), True)
    fs.mkdirs(P(f"{base}/year={year}"))
    if not fs.rename(P(tmp), P(src)):
        raise RuntimeError(f"rename {tmp} → {src} 失敗，資料仍在 tmp，手動 hdfs dfs -mv 即可")
    log(f"  [{year}-{month}] OK：{src_count} 筆，型別已統一")


def main():
    p = argparse.ArgumentParser(description="統一 M06A parquet 樹欄位型別")
    p.add_argument("--base", default="hdfs:///dataset/M06A")
    p.add_argument("--files-per-month", type=int, default=1)
    p.add_argument("--max-months", type=int, default=0,
                   help="本輪最多處理幾個月（exit 3=還有剩），0=不限")
    a = p.parse_args()

    spark = (SparkSession.builder
             .appName("unify_m06a_schema")
             .config("spark.task.cpus", "1")
             .config("spark.sql.parquet.compression.codec", "zstd")
             .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
             .getOrCreate())

    months = list_months(spark, a.base)
    if not months:
        raise SystemExit(f"{a.base} 沒有 year=/month= 目錄")

    todo = [(y, m) for y, m in months
            if needs_retype(spark, f"{a.base}/year={y}/month={m}")]
    batch = todo[:a.max_months] if a.max_months > 0 else todo
    log(f"===== 共 {len(months)} 個月，需重寫 {len(todo)}，本輪 {len(batch)} =====")

    ok = fail = 0
    for year, month in batch:
        try:
            log(f"  [{year}-{month}] 重寫中…")
            retype_month(spark, a.base, year, month, a.files_per_month)
            ok += 1
        except Exception as e:  # noqa: BLE001
            log(f"  [{year}-{month}] ERROR：{e}")
            fail += 1
        finally:
            spark.catalog.clearCache()

    remaining = len(todo) - len(batch)
    log(f"===== 本輪結束：OK {ok} | 失敗 {fail} | 還剩 {remaining} =====")
    spark.stop()
    raise SystemExit(1 if fail else (3 if remaining else 0))


if __name__ == "__main__":
    main()
