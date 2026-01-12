# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, lit, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd
spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "ACTIVE",   "2026-01-01"),
    ("U1", "INACTIVE", "2026-01-05"),
    ("U1", "ACTIVE",   "2026-01-12"),
    ("U2", "ACTIVE",   "2026-01-03"),
    ("U2", "SUSPENDED","2026-01-08"),
    ("U3", "ACTIVE",   "2026-01-11"),
    ("U4", "INACTIVE", "2025-12-31"),
]

columns = ["user_id", "status", "status_date"]

status_df = spark.createDataFrame(data, columns)

status_df.show(truncate=False)
w = Window.partitionBy("user_id").orderBy(F.col("gap_days"))
users_df = status_df.select("user_id").distinct()
final = (status_df
         .filter((col("status_date") <= '2026-01-10'))
         .withColumn(
             "gap_days", 
           datediff(
             to_date(lit('2026-01-10')), 
             "status_date")
         )
         .withColumn(
           "rnk", 
           F.row_number().over(w)
         )
         .filter(col("rnk") == 1)
         .select('user_id','status','status_date')
        )
final_df = users_df.join(final, on = ['user_id'], how ='left') 
final_df.show(truncate=False)