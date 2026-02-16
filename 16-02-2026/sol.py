from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("C1", "P1", 100.0),
    ("C1", "P2", 200.0),
    ("C1", "P1", 150.0),
    ("C1", "P3", 50.0),

    ("C2", "P1", 300.0),
    ("C2", "P2", 100.0),

    ("C3", "P1", 400.0),
    ("C3", "P2", 200.0),
    ("C3", "P3", 100.0),

    ("C4", "P1", 100.0),
]

df = spark.createDataFrame(data, ["customer_id", "product_id", "amount"])

w = (Window
     .partitionBy("customer_id")
     .orderBy(F.col("product_revenue").desc(),F.col('product_id').asc())
    )

total_revenue = (
  df.groupBy('customer_id').agg(F.sum('amount').alias('total_revenue'))
    .filter(F.col('total_revenue') >= 500)
)
product_revenue = (
    df
    .groupBy('customer_id','product_id').agg(
      F.sum('amount').alias('product_revenue')
    )
)
result = total_revenue.join(product_revenue, on = 'customer_id')

final = (
  result
    .withColumn('product_rank', F.row_number().over(w))
    .filter(F.col('product_rank') <=2)
    .select('customer_id','product_id','product_revenue','product_rank')
)
final.show(truncate=False)