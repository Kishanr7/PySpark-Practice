# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("C1", "O1", 200.0),
    ("C1", "O2", 150.0),
    ("C2", "O3", 300.0),
    ("C2", "O4", 100.0),
    ("C3", "O5", 250.0),
]

columns = ["customer_id", "order_id", "amount"]

orders_df = spark.createDataFrame(data, columns)
orders_df.show()
final = (orders_df
         .groupBy("customer_id")
         .agg(F.sum("amount").alias("total_amount"), F.count("order_id").alias("total_orders"))
)
final.show(truncate=False)