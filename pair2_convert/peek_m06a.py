from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = (SparkSession.builder.appName("peek_m06a")
         .config("spark.task.cpus", "1").getOrCreate())
df = spark.read.parquet("hdfs:///dataset/M06A/year=2025/month=01")
df.printSchema()
(df.select("VehicleType","DetectionTime_O","GantryID_O","DetectionTime_D","GantryID_D",
           "TripLength","TripEnd", F.substring("TripInformation",1,120).alias("TripInfo_head"))
   .show(2, truncate=False, vertical=True))
spark.stop()
