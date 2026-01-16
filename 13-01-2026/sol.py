# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, lit, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("C1", "O1", 200.0),
    ("C1", "O2", 500.0),
    ("C1", "O3", 300.0),
    ("C2", "O4", 100.0),
    ("C2", "O5", 100.0),
    ("C3", "O6", 250.0),
]

orders_df = spark.createDataFrame(
    data, ["customer_id", "order_id", "amount"]
)

orders_df.show(truncate=False)

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())
final = (orders_df
         .withColumn("rnk", F.row_number().over(w))
         .filter(F.col("rnk") == 1)
         .select('customer_id','order_id','amount')
        ) 
final.show(truncate=False)