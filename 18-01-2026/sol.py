# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, lit, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-02", 100),
    ("U1", "2026-01-15", 200),
    ("U1", "2026-02-01", 300),
    ("U2", "2026-01-05", 50),
    ("U2", "2026-01-20", 80),
    ("U2", "2026-02-10", 120),
    ("U3", "2026-01-01", 60),
]

purchases_df = (
    spark.createDataFrame(data, ["user_id", "purchase_date", "amount"])
         .withColumn("purchase_date", F.to_date("purchase_date"))
)

purchases_df.show(truncate=False)

w = (
  Window
  .partitionBy(F.col("user_id"), F.date_format("purchase_date", "yyyy-MM"))
  .orderBy(F.col("purchase_date"))
)

final = (purchases_df
         .withColumn(
           "month", 
           F.date_format("purchase_date", "yyyy-MM"))
         .withColumn("rnk",F.row_number().over(w))
         .filter(F.col("rnk") == 1)
         .select("user_id","month","purchase_date","amount")
        ) 
final.show(truncate=False)