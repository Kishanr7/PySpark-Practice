from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkChallenge").getOrCreate()

data = [
    ("r1", "p1", "u1", 5, "2025-10-01"),
    ("r2", "p1", "u2", 4, "2025-10-02"),
    ("r3", "p2", "u1", 3, "2025-10-03"),
    ("r4", "p2", "u3", 4, "2025-10-04"),
    ("r5", "p3", "u2", 5, "2025-10-05"),
    ("r6", "p3", "u2", 5, "2025-10-06"),
    ("r7", "p4", "u4", 2, "2025-10-07"),
    ("r8", "p5", "u5", 3, "2025-10-08"),
    ("r9", "p5", "u1", 4, "2025-10-09"),
    ("r10", "p5", "u2", 5, "2025-10-10"),
]

schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("rating", IntegerType(), True),
    StructField("review_date", StringType(), True)
])

reviews_df = spark.createDataFrame(data, schema=schema)
reviews_df = reviews_df.withColumn("review_date", F.to_date("review_date"))

product_df = (
    reviews_df
    .groupBy("product_id")
    .agg(F.round(F.avg("rating"),2).alias("avg_rating"),F.count('review_id').alias("num_reviews"))
    .orderBy(F.col("avg_rating").desc(),F.col("num_reviews").desc())
)
product_df.show(3)

users_df = (
    reviews_df
    .groupBy("user_id")
    .agg(F.round(F.avg("rating"),2).alias("avg_rating_given"),F.count('review_id').alias("reviews_written"))
    .orderBy(F.col("reviews_written").desc())
)
users_df.show(3)