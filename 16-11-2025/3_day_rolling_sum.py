# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd


spark = SparkSession.builder.appName("DailyPySpark_Rolling3Day").getOrCreate()

data = [
    ("u1", "2025-11-10", 100),
    ("u1", "2025-11-11",  50),
    ("u1", "2025-11-13", 200),
    ("u1", "2025-11-15", 100),

    ("u2", "2025-11-10",  80),
    ("u2", "2025-11-12",  20),
    ("u2", "2025-11-16",  40),
]

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_date", StringType(), True),  # yyyy-MM-dd
    StructField("amount", IntegerType(), True),
])

df = (spark.createDataFrame(data, schema)
          .withColumn("event_date", F.to_date("event_date")))

events_df = df.repartition("user_id")

w = (Window
     .partitionBy("user_id")
     .orderBy(F.col("event_date").cast("timestamp").cast("long"))
     .rangeBetween(-2 * 24 * 60 * 60, 0)  # last 2 days in seconds
)
final = (events_df
         .withColumn("rolling_3d_sum", F.sum(F.col('amount')).over(w))
)

final.show(truncate=False)