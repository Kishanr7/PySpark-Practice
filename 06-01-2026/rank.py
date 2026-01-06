# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("C1", "O1", "2026-01-01", 200.0),
    ("C1", "O2", "2026-01-03", 150.0),
    ("C2", "O3", "2026-01-02", 300.0),
    ("C2", "O4", "2026-01-02", 100.0),
    ("C3", "O5", "2026-01-01", 250.0),
]

columns = ["customer_id", "order_id", "order_date", "amount"]

orders_df = spark.createDataFrame(data, columns)
orders_df.show(truncate=False)

window_spec=Window.partitionBy("customer_id").orderBy(col('order_date').desc())
final = (orders_df
         .withColumn("rank", row_number().over(window_spec))
         .filter(F.col("rank") == 1)
         .select('customer_id','order_id', 'order_date', 'amount')
)
final.show(truncate=False)