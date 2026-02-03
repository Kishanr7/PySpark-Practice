from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "Books",  100, "2026-01-01"),
    ("U1", "Books",   50, "2026-01-03"),
    ("U1", "Games",  150, "2026-01-02"),
    ("U1", "Music",  150, "2026-01-01"),

    ("U2", "Books",   40, "2026-01-05"),
    ("U2", "Games",   40, "2026-01-01"),
    ("U2", "Movies",  20, "2026-01-03"),
]

df = (
    spark.createDataFrame(
        data,
        ["user_id", "category", "amount", "txn_date"]
    )
    .withColumn("txn_date", F.to_date("txn_date"))
)

w = Window.partitionBy("user_id").orderBy(F.col("total_spend").desc(),F.col("min_txn_date"), F.col("category"))

result = (
    df
    .groupBy("user_id","category").agg(F.sum(F.col("amount")).alias("total_spend"),F.min("txn_date").alias("min_txn_date"))
    .withColumn("rank", F.dense_rank().over(w))
    .filter(F.col("rank") <=2)
    .select("user_id", "category", "total_spend", "rank")
)

result.show(truncate=False)