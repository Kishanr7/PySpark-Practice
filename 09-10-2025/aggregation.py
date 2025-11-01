from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("DailyPySparkPractice").getOrCreate()

# ---- Orders DataFrame ----
orders_data = [
    (1, 101, 250, "2025-10-01"),
    (2, 102, 300, "2025-10-01"),
    (3, 101, 450, "2025-10-02"),
    (4, 103, 200, "2025-10-03")
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("order_date", StringType(), True)
])

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)
orders_df.show()

# ---- Customers DataFrame ----
customers_data = [
    (101, "John Doe", "Mumbai"),
    (102, "Alice Roy", "Delhi"),
    (103, "Bob Smith", "Bangalore")
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True)
])

customers_df = spark.createDataFrame(customers_data, schema=customers_schema)
customers_df.show()

joined_df = orders_df.join(customers_df, on='customer_id')
total_amount = orders_df.groupBy('customer_id').agg(F.sum('amount').alias('total_amount'))
total_amount.orderBy(F.col('total_amount').desc())
total_amount.show(2)