# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-01", 100.0),
    ("U1", "2026-01-03", 150.0),
    ("U1", "2026-01-03", 200.0),
    ("U2", "2026-01-02", 300.0),
    ("U2", "2026-01-04", 50.0),
    ("U3", "2026-01-01", 80.0),
]

columns = ["user_id", "txn_date", "amount"]

txns_df = spark.createDataFrame(data, columns)
txns_df.show(truncate=False)


w = Window.partitionBy("user_id").orderBy(F.col("txn_date").desc())
final = (txns_df
         .withColumn("ranked", F.dense_rank().over(w))
         .filter(F.col("ranked") == 1)
         .groupBy(F.col('user_id')).agg(F.max('txn_date').alias('latest_date') ,F.sum('amount').alias('total_amount_spent'))
         .select('user_id','latest_date','total_amount_spent')
        )
final.show(truncate=False)