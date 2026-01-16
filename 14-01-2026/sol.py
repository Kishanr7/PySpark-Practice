# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, lit, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("P1", "2026-01-01", 100),
    ("P1", "2026-01-02", 120),
    ("P1", "2026-01-03", 110),
    ("P1", "2026-01-04", 130),
    ("P2", "2026-01-01", 200),
    ("P2", "2026-01-02", 210),
    ("P2", "2026-01-03", 220),
    ("P3", "2026-01-01", 50),
]

sales_df = (
    spark.createDataFrame(data, ["product_id", "sale_date", "amount"])
         .withColumn("sale_date", F.to_date("sale_date"))
)

sales_df.show(truncate=False)

w = Window.partitionBy("product_id").orderBy(F.col("sale_date"))
final = (sales_df
         .withColumn("prev_amount", F.lag(F.col("amount"), 1).over(w))
         .withColumn(
           "decline_date", 
           F.when(F.col('amount') < F.col("prev_amount"), F.col("sale_date"))
         )
         .groupBy("product_id")
         .agg(F.min("decline_date").alias("sale_date"))
         .select('product_id','sale_date')
        ) 
final.show(truncate=False)