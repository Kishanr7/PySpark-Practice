# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("s1", "2025-11-12", 100),
    ("s1", "2025-11-13", 200),
    ("s1", "2025-11-15", 300),
    ("s1", "2025-11-16", 400),
    ("s2", "2025-11-12", 500),
    ("s2", "2025-11-13", 600),
    ("s2", "2025-11-14", 700),
    ("s2", "2025-11-16", 800),
]

columns = ["store_id", "date", "sales_amount"]

sales_df = spark.createDataFrame(data, columns).withColumn("date", F.to_date(col("date")))

w = (Window
     .partitionBy("store_id")
     .orderBy(F.col("date").cast("long"))
     .rangeBetween(-6 * 24 * 60 * 60 ,0)
)
final = (sales_df
         .withColumn("rolling_7d_avg", F.avg(F.col("sales_amount")).over(w))
)

final.show(truncate=False)