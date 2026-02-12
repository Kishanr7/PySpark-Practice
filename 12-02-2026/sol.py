from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-05"),
    ("U1", "2026-02-10"),
    ("U1", "2026-03-02"),

    ("U2", "2026-01-15"),
    ("U2", "2026-03-10"),

    ("U3", "2026-02-01"),
    ("U3", "2026-03-05"),

    ("U4", "2026-03-20"),
]

df = (
    spark.createDataFrame(data, ["user_id", "activity_date"])
    .withColumn("activity_date", F.to_date("activity_date"))
)

w = (Window
     .partitionBy("user_id")
     .orderBy(F.col("activity_month"))

    )

result = (
    df
    .withColumn("activity_month", F.date_format(F.col('activity_date'), "yyyy-MM"))
    .withColumn("prev_date", F.lag('activity_date', 1).over(w))
    .withColumn("diff_month",F.round(F.months_between(F.col("activity_date"), F.col("prev_date"))))
    .filter(F.col('diff_month') == 1)
    .groupBy('activity_month').agg(F.count('user_id').alias('retained_users_count'))
    .select(
        F.col("activity_month").alias("month"),
        "retained_users_count"
    )
    .orderBy("month")
)

result.show(truncate=False)