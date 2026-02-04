from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-01"),
    ("U1", "2026-01-02"),
    ("U1", "2026-01-04"),
    ("U1", "2026-01-08"),
    ("U2", "2026-01-01"),
    ("U2", "2026-01-03"),
    ("U2", "2026-01-04"),
]

df = (
    spark.createDataFrame(data, ["user_id", "activity_date"])
    .withColumn("activity_date", F.to_date("activity_date"))
)

w = (Window
     .partitionBy("user_id")
     .orderBy(F.col("activity_date").cast("timestamp").cast("long"))
     .rangeBetween(-6 * 24 * 60 * 60 ,0)
    )

result = (
    df
    .withColumn("active_days_last_7_days", F.count("*").over(w))
    .select(
        "user_id",
        "activity_date",
        "active_days_last_7_days"
    )
    .orderBy("user_id","activity_date")
)

result.show(truncate=False)