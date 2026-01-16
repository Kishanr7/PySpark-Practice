# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, lit, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-01", 100),
    ("U1", "2026-01-05", 200),
    ("U1", "2026-01-20", 300),
    ("U1", "2026-01-22", 150),
    ("U2", "2026-01-03", 50),
    ("U2", "2026-01-06", 80),
    ("U3", "2026-01-01", 120),
]

purchases_df = (
    spark.createDataFrame(data, ["user_id", "purchase_date", "amount"])
         .withColumn("purchase_date", F.to_date("purchase_date"))
)

purchases_df.show(truncate=False)

w = Window.partitionBy("user_id").orderBy(F.col("purchase_date"))
final = (purchases_df
         .withColumn("prev_date", F.lag(F.col("purchase_date"), 1).over(w))
         .withColumn(
           "gaps", 
           F.when(datediff(to_date(F.col('purchase_date')), to_date(F.col('prev_date'))) > 7, F.col("purchase_date"))
           .otherwise(lit(None))
         )
         .groupBy('user_id')
         .agg(F.min('gaps').alias("purchase_date"))
         .select('user_id','purchase_date')
        ) 
final.show(truncate=False)