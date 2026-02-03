from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "session_start", "2026-01-01 09:00:00", "2026-01-01 09:01:00"),
    ("U1", "click",         "2026-01-01 09:05:00", "2026-01-01 09:06:00"),
    ("U1", "purchase",      "2026-01-01 09:10:00", "2026-01-01 09:30:00"),  # late ingest

    ("U1", "session_start", "2026-01-01 10:00:00", "2026-01-01 10:00:30"),
    ("U1", "click",         "2026-01-01 10:05:00", "2026-01-01 10:05:10"),

    ("U2", "click",         "2026-01-01 08:00:00", "2026-01-01 08:10:00"),
    ("U2", "session_start", "2026-01-01 08:30:00", "2026-01-01 08:31:00"),
    ("U2", "purchase",      "2026-01-01 08:40:00", "2026-01-01 08:41:00"),
]

df = (
    spark.createDataFrame(
        data,
        ["user_id", "event_type", "event_time", "ingestion_time"]
    )
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("ingestion_time", F.to_timestamp("ingestion_time"))
)

w = Window.partitionBy("user_id").orderBy(F.col("event_time"))

result = (
    df
    .withColumn("gap", F.when(F.col("event_type") == 'session_start', 1).otherwise(0))
    .withColumn("session_id", F.sum('gap').over(w))
    .filter(F.col("session_id") > 0)
    .select("user_id", "event_type", "event_time", "session_id")
)

result.show(truncate=False)