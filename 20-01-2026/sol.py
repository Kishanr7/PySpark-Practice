# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, lit, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-01 10:00:00", "2026-01-01 11:00:00"),
    ("U1", "2026-01-01 10:30:00", "2026-01-01 12:00:00"),
    ("U1", "2026-01-02 09:00:00", "2026-01-02 10:00:00"),
    ("U2", "2026-01-03 14:00:00", "2026-01-03 15:00:00"),
    ("U2", "2026-01-03 15:00:00", "2026-01-03 16:00:00"),
    ("U3", "2026-01-04 08:00:00", "2026-01-04 09:30:00"),
    ("U3", "2026-01-04 09:00:00", "2026-01-04 10:00:00"),
]

sessions_df = (
    spark.createDataFrame(data, ["user_id", "session_start", "session_end"])
         .withColumn("session_start", F.to_timestamp("session_start"))
         .withColumn("session_end", F.to_timestamp("session_end"))
)

sessions_df.show(truncate=False)

w = (
  Window
  .partitionBy(F.col("user_id"))
  .orderBy(F.col("session_start"), F.col("session_end"))
)

final = (sessions_df
         .withColumn(
           "prev_session_end_date", 
           F.lag("session_end", 1).over(w))
         .withColumn(
           "has_overlap", 
           F.when(F.col("prev_session_end_date") >= F.col("session_start"), True)
           # .when(datediff(F.col("login_date"),F.col("prev_date")) > 1, 1)
           .otherwise(False)
         )
         .filter(F.col("has_overlap") == True)
         .select("user_id","has_overlap")
        ) 
final.show(truncate=False)