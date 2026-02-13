from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("Electronics", "P1", 2, 500.0),
    ("Electronics", "P2", 1, 1200.0),
    ("Electronics", "P1", 1, 500.0),
    ("Electronics", "P3", 5, 100.0),

    ("Clothing", "P4", 3, 50.0),
    ("Clothing", "P5", 2, 80.0),
    ("Clothing", "P4", 1, 50.0),
    ("Clothing", "P6", 4, 40.0),
]

df = spark.createDataFrame(
    data,
    ["category", "product_id", "quantity", "price"]
)

w = (Window
     .partitionBy("category")
     .orderBy(F.col("total_revenue").desc(), F.col('product_id').asc())

    )

result = (
    df
    .withColumn("total_revenue", (F.col('quantity') * F.col('price')))
    .groupBy('category','product_id').agg(F.sum('total_revenue').alias('total_revenue'))
    .withColumn("rank", F.row_number().over(w))
    .filter(F.col('rank') <= 2)
)

result.show(truncate=False)