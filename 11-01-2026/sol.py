# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd
spark = SparkSession.builder.getOrCreate()

users_data = [
    ("U1", "2026-01-01"),
    ("U2", "2026-01-03"),
    ("U3", "2026-01-05"),
    ("U4", "2026-01-07"),
]

orders_data = [
    ("O1", "U1", "2025-12-31"),
    ("O2", "U1", "2026-01-02"),
    ("O3", "U1", "2026-01-10"),
    ("O4", "U2", "2026-01-01"),
    ("O5", "U2", "2026-01-03"),
    ("O6", "U3", "2026-01-06"),
]

users_df = spark.createDataFrame(
    users_data, ["user_id", "signup_date"]
)

orders_df = spark.createDataFrame(
    orders_data, ["order_id", "user_id", "order_date"]
)

users_df.show(truncate=False)
orders_df.show(truncate=False)

joined_df = users_df.join(orders_df, on=['user_id'], how='left')
joined_df.show()
w = Window.partitionBy("user_id").orderBy(F.col("gap_days"))

final = (joined_df
         .withColumn("gap_days", datediff("order_date", "signup_date"))
         .filter((col("gap_days") >= 0) | col("gap_days").isNull())
         .withColumn(
           "rnk", 
           F.row_number().over(w)
         )
         .filter(col("rnk") == 1)
         .select('user_id','order_id','order_date')
        )
final.show(truncate=False)