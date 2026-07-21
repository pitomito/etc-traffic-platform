#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""一次性檢查：parquet_output 上傳月份 vs convert_raw 轉出月份的 schema 相容性。"""
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("check_m06a_schema")
         .config("spark.task.cpus", "1")
         .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
         .getOrCreate())

paths = {
    "上傳版 2025-06 (data.parquet)": "hdfs:///dataset/M06A/year=2025/month=06",
    "raw轉換版 2026-05 (part-*)":    "hdfs:///dataset/M06A_new/year=2026/month=05",
}
schemas = {}
for name, p in paths.items():
    df = spark.read.parquet(p)
    schemas[name] = df.schema
    print(f"\n== {name} ==")
    print(f"rows={df.count()}")   # parquet count 走 footer metadata，快
    for f in df.schema.fields:
        print(f"  {f.name:20s} {f.dataType.simpleString()}")

a, b = list(schemas.values())
print("\n== 結論 ==")
if [(f.name, f.dataType) for f in a.fields] == [(f.name, f.dataType) for f in b.fields]:
    print("SCHEMA IDENTICAL — 兩種格式混在同一棵樹查詢完全沒問題")
else:
    print("SCHEMA MISMATCH — 欄位或型別不一致，混讀可能報錯，需先統一")
print("\n== 整棵樹混讀測試（year/month 由路徑推斷）==")
df_all = (spark.read
          .option("basePath", "hdfs:///dataset/M06A")
          .parquet("hdfs:///dataset/M06A/year=*/month=*"))
df_all.printSchema()
print(f"M06A 全樹 rows={df_all.count()}")
spark.stop()
