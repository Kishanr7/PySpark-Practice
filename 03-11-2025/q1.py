from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkPractice").getOrCreate()

data = [
    ("O1", "C1", "2025-10-01", "Laptop", 1, 1200.0),
    ("O2", "C1", "2025-10-05", "Mouse", 2, 25.0),
    ("O3", "C2", "2025-10-03", "Phone", 1, 800.0),
    ("O4", "C2", "2025-10-04", "Headset", 2, 100.0),
    ("O5", "C3", "2025-10-06", "Keyboard", 1, 90.0),
    ("O6", "C3", "2025-10-07", "Mouse", 1, 30.0),
    ("O7", "C4", "2025-10-02", "Laptop", 1, 1100.0),
    ("O8", "C4", "2025-10-08", "Monitor", 1, 300.0),
]

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("cust_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
])

orders_df = spark.createDataFrame(data, schema=schema)

filter_df = (
    orders_df
      .withColumn("line_amount", col("quantity") * col("unit_price"))
      .groupBy("cust_id")
      .agg(F.countDistinct("order_id").alias("total_orders"),
           F.sum('quantity').alias("total_quantity"),
           F.sum('line_amount').alias('total_amount'))
      .withColumn("avg_order_value", F.col('total_amount')/F.col('total_orders')) 
      .filter(F.col('avg_order_value') > 500)
      .orderBy(F.col('avg_order_value').desc())
)

filter_df.show()
# grouped_df.show()

