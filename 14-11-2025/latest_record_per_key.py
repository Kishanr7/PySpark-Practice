from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("DailyPySpark_Easy").getOrCreate()

data = [
    ("a1", "2025-10-01 09:00:00", "open",  10),
    ("a1", "2025-10-01 09:05:00", "click", 20),
    ("a1", "2025-10-01 09:05:00", "click", 25),
    ("a2", "2025-10-02 11:00:00", "open",  5),
    ("a2", "2025-10-02 11:30:00", "open",  7),
    ("a3", "2025-10-03 08:00:00", "open",  1),
]

schema = StructType([
    StructField("id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("value", IntegerType(), True),
])

events_df = spark.createDataFrame(data, schema) \
                 .withColumn("event_ts", F.to_timestamp("event_ts"))

events_df = events_df.repartition("id")

w = Window.partitionBy("id").orderBy(F.col("event_ts").desc(), F.col("value").desc())

final_latest = (
    events_df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("id", "event_ts", "event_type", "value")
)

final_latest.show(truncate=False)
