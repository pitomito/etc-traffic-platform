from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = (SparkSession.builder.appName("chk_daterange")
         .config("spark.task.cpus", "1").getOrCreate())
for name, p in [("上傳版", "hdfs:///dataset/M06A/year=2026/month=05"),
                ("raw轉換版", "hdfs:///dataset/M06A/year=2026/year=2026/month=05")]:
    df = spark.read.parquet(p)
    df.agg(F.min("DetectionTime_O").alias("min_t"), F.max("DetectionTime_O").alias("max_t"),
           F.countDistinct(F.substring("DetectionTime_O",1,10)).alias("days")).show(truncate=False)
    print("^==", name)
spark.stop()
