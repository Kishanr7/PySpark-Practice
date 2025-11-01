from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum

# Initialize Spark
spark = SparkSession.builder.appName("DailyPySparkPractice").getOrCreate()

# Sample data
sales_data = [
    ("East", "A", 1200, "2025-10-01"),
    ("East", "B", 900, "2025-10-01"),
    ("West", "C", 1500, "2025-10-01"),
    ("West", "D", 700, "2025-10-02"),
    ("East", "A", 1100, "2025-10-02"),
    ("West", "C", 1000, "2025-10-02")
]

# Schema
sales_schema = StructType([
    StructField("region", StringType(), True),
    StructField("salesperson", StringType(), True),
    StructField("sales_amount", IntegerType(), True),
    StructField("date", StringType(), True)
])

# Create DataFrame
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)

# Show DataFrame
sales_df.show()

window_spec = Window.partitionBy("region",'date').orderBy(col("sales_amount").desc())

res_df = sales_df.withColumn("rank", rank().over(window_spec))
result_df = res_df.select('region','salesperson','sales_amount','date').filter(F.col('rank') ==1)
result_df.show()