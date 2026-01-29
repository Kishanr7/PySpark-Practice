from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-29 09:00:00"),
    ("U1", "2026-01-29 09:10:00"),
    ("U1", "2026-01-29 10:00:00"),
    ("U1", "2026-01-29 10:20:00"),
    ("U1", "2026-01-29 11:00:00"),

    ("U2", "2026-01-29 08:00:00"),
    ("U2", "2026-01-29 08:20:00"),
    ("U2", "2026-01-29 09:00:00"),
]

df = (
    spark.createDataFrame(data, ["user_id", "event_time"])
    .withColumn("event_time", F.to_timestamp("event_time"))
)

w = Window.partitionBy("user_id").orderBy(F.col("event_time"))

result = (
    df
    .withColumn("prev_event_time", F.lag(F.col("event_time")).over(w))
    .withColumn("gap", F.col("event_time").cast("long") - F.col("prev_event_time").cast("long"))
    .withColumn(
      "brk_flg", 
      F.when(F.col("prev_event_time").isNull(), 1)
      .when(F.col("gap") > 30*60 , 1)
      .otherwise(0)
    )
    .withColumn("session_id", F.sum(F.col("brk_flg")).over(w))
    .select("user_id", "gap", "event_time", "prev_event_time","brk_flg","session_id")
)

result.show(truncate=False)