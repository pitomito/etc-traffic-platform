from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("cmp_2026_05")
         .config("spark.task.cpus", "1").getOrCreate())
for name, p in [("上傳版 (data.parquet 1.2G)", "hdfs:///dataset/M06A/year=2026/month=05"),
                ("raw轉換版 (part-* 6.2G)",   "hdfs:///dataset/M06A/year=2026/year=2026/month=05")]:
    df = spark.read.parquet(p)
    print(f"== {name} ==")
    print("rows =", df.count())
    print("cols =", [(f.name, f.dataType.simpleString()) for f in df.schema.fields])
spark.stop()
