# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, lit, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-01"),
    ("U1", "2026-01-02"),
    ("U1", "2026-01-04"),
    ("U1", "2026-01-05"),
    ("U1", "2026-01-06"),
    ("U2", "2026-01-03"),
    ("U2", "2026-01-05"),
    ("U2", "2026-01-06"),
    ("U3", "2026-01-10"),
]

logins_df = (
    spark.createDataFrame(data, ["user_id", "login_date"])
        .withColumn("login_date", F.to_date("login_date"))
)

logins_df.show(truncate=False)

w = (
Window
.partitionBy(F.col("user_id"))
.orderBy(F.col("login_date"))
)

final = (logins_df
        .withColumn(
            "prev_date", 
            F.lag("login_date", 1).over(w))
        .withColumn(
            "break_flg", 
            F.when(F.col("prev_date").isNull(), 1)
            .when(datediff(F.col("login_date"),F.col("prev_date")) > 1, 1)
            .otherwise(0)
        )
        .withColumn("session_id", F.sum('break_flg').over(w))
        .groupBy('user_id','session_id')
        .agg(F.count('session_id').alias("max_consecutive_days_per_session"))
        .groupBy('user_id')
        .agg(F.max('max_consecutive_days_per_session').alias("longest_login_streak"))
        .select("user_id","longest_login_streak")
        ) 
final.show(truncate=False)