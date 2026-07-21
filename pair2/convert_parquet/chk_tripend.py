from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = (SparkSession.builder.appName("chk_tripend")
         .config("spark.task.cpus", "1").getOrCreate())
raw = spark.read.parquet("hdfs:///dataset/M06A/year=2026/year=2026/month=05")
print("raw版 TripEnd 分佈:")
raw.groupBy("TripEnd").count().show()
up = spark.read.parquet("hdfs:///dataset/M06A/year=2026/month=05")
print("上傳版 TripEnd 分佈:")
up.groupBy("TripEnd").count().show()
spark.stop()
