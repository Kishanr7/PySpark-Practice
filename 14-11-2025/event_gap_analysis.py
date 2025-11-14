# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySpark_Easy").getOrCreate()

data = [
    ("a1", "2025-10-01 09:00:00", "open",  10),
    ("a1", "2025-10-01 09:05:00", "click", 20),
    ("a1", "2025-10-01 09:05:00", "click", 25),  # duplicate ts, different payload
    ("a2", "2025-10-02 11:00:00", "open",  5),
    ("a2", "2025-10-02 11:30:00", "open",  7),
    ("a3", "2025-10-03 08:00:00", "open",  1),
]

schema = StructType([
    StructField("id", StringType(), True),
    StructField("event_ts", StringType(), True),   # treat as timestamp in your solution
    StructField("event_type", StringType(), True),
    StructField("value", IntegerType(), True),
])

events_df = spark.createDataFrame(data, schema) \
                 .withColumn("event_ts", F.to_timestamp("event_ts"))

events_df = events_df.repartition("id")

w = Window.partitionBy("id").orderBy(F.col("event_ts").asc())

final = (events_df
         .withColumn("prev_event_ts", F.lag(F.col('event_ts'),1).over(w))
         .withColumn("gap_seconds",  F.col("event_ts").cast("long") - F.col("prev_event_ts").cast("long"))
         .withColumn("is_gap_large", F.when(F.col("gap_seconds") > 300, "Y").otherwise("N"))
         .orderBy(F.col('id'),F.col('event_ts'))
         .select("id","event_ts","event_type","value","prev_event_ts","gap_seconds","is_gap_large")
)

final.show(truncate=False)