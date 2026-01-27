from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-24"),
    ("U2", "2026-01-24"),
    ("U1", "2026-01-25"),
    ("U3", "2026-01-25"),
    ("U2", "2026-01-26"),
]

df = (
    spark.createDataFrame(data, ["user_id", "event_date"])
         .withColumn("event_date", F.to_date("event_date"))
)

df.show(truncate=False)

final = (df
         .groupBy('event_date').agg(F.count('user_id').alias('daily_active_users'))
         .select("event_date","daily_active_users")
        )
final.show()