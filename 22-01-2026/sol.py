# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-01 09:00:00", "2026-01-01 09:30:00"),
    ("U1", "2026-01-01 09:45:00", "2026-01-01 10:15:00"),
    ("U1", "2026-01-01 11:00:00", "2026-01-01 11:30:00"),
    ("U2", "2026-01-02 14:00:00", "2026-01-02 14:20:00"),
    ("U2", "2026-01-02 14:40:00", "2026-01-02 15:00:00"),
    ("U2", "2026-01-02 16:00:00", "2026-01-02 16:30:00"),
]

sessions_df = (
    spark.createDataFrame(
        data,
        ["user_id", "session_start", "session_end"]
    )
    .withColumn("session_start", F.to_timestamp("session_start"))
    .withColumn("session_end", F.to_timestamp("session_end"))
)

sessions_df.show(truncate=False)

w = Window.partitionBy("user_id").orderBy(F.col("session_start"))

final = (sessions_df
         .withColumn("prev_session_end", F.lag(F.col("session_end"), 1).over(w))
         .withColumn("gap",F.col("session_start").cast("long") - F.col("prev_session_end").cast("long"))
         .withColumn(
           "brk_flg", 
           F.when(F.col("gap")/60 >= 30, 1)
             .when(F.col("prev_session_end").isNull(), 1)
             .otherwise(0)
         )
         .withColumn("streak_id", F.sum('brk_flg').over(w))
         .select("user_id","session_start","session_end","streak_id")
        )
         
final.show(truncate=False)