# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd


spark = SparkSession.builder.appName("DailyPySpark").getOrCreate()

data = [
    ("u1", "2025-11-15 09:00:00", "page_view"),
    ("u1", "2025-11-15 09:02:00", "click"),
    ("u1", "2025-11-15 09:20:00", "purchase"),
    ("u1", "2025-11-15 10:00:00", "page_view"),
    ("u2", "2025-11-15 08:00:00", "page_view"),
    ("u2", "2025-11-15 08:03:00", "scroll"),
    ("u2", "2025-11-15 08:45:00", "click"),
]

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_ts", StringType(), True),  # treat as timestamp
    StructField("event_type", StringType(), True),
])

df = spark.createDataFrame(data, schema) \
          .withColumn("event_ts", F.to_timestamp("event_ts"))

events_df = df.repartition("user_id")

w = Window.partitionBy("user_id").orderBy(F.col("event_ts").asc())
final = (events_df
         .withColumn("prev_event_ts", F.lag(F.col('event_ts'),1).over(w))
         .withColumn("gap_seconds",  F.col("event_ts").cast("long") - F.col("prev_event_ts").cast("long"))
         .withColumn("new_session_flag", F.when(F.col('prev_event_ts').isNull(), 1).when(F.col("gap_seconds") > 900, 1).otherwise(0))
         .withColumn("session_id", F.sum('new_session_flag').over(w))
         .orderBy(F.col('user_id'),F.col('event_ts'))
         .select("user_id","event_ts","event_type","session_id")
)

final.show(truncate=False)