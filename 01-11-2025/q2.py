from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkPractice").getOrCreate()

# Raw transactions (multiple per day per store)
data = [
    ("S1", "2025-10-01", 200.0),
    ("S1", "2025-10-01", 150.0),
    ("S1", "2025-10-02", 500.0),
    ("S1", "2025-10-03", 300.0),
    ("S1", "2025-10-04", 250.0),

    ("S2", "2025-10-01", 400.0),
    ("S2", "2025-10-02", 100.0),
    ("S2", "2025-10-03", 600.0),
    ("S2", "2025-10-04", 100.0),

    ("S3", "2025-10-01", 50.0),
    ("S3", "2025-10-02", 800.0),
    ("S3", "2025-10-03", 120.0),
    ("S3", "2025-10-04", 900.0),
]

schema = StructType([
    StructField("store_id", StringType(), True),
      StructField("txn_date", StringType(), True),
    StructField("amount",   DoubleType(), True)
])

txns_df = spark.createDataFrame(data, schema=schema) \
    .withColumn("txn_date", F.to_date("txn_date"))

w_store = Window.partitionBy("store_id").orderBy("txn_date")
w_rank = Window.partitionBy("txn_date").orderBy(col("daily_revenue").desc())
filter_df = (
    txns_df.
      groupBy("store_id","txn_date")
      .agg(F.sum("amount").alias("daily_revenue"))
      .withColumn("prev_day_revenue", lag("daily_revenue", 1).over(w_store)) 
      .withColumn("growth_pct", ((col("daily_revenue") - col("prev_day_revenue")) / col("prev_day_revenue")) * 100)
      .withColumn("rolling_3d_sum", F.sum("daily_revenue").over(w_store.rowsBetween(-2,0)))
      .withColumn("rank", dense_rank().over(w_rank))
      .filter(col("rank") <= 2)
      .orderBy(col("txn_date").asc(), col("rank").asc(), col("store_id").asc())
)

filter_df.show()
# grouped_df.show()
