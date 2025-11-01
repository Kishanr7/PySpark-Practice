from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum

# Initialize Spark
spark = SparkSession.builder.appName("DailyPySparkPractice").getOrCreate()

# Sample data
numbers_data = [
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40),
    (5, 50)
]

# Schema
numbers_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("value", IntegerType(), True)
])

# Create DataFrame
numbers_df = spark.createDataFrame(numbers_data, schema=numbers_schema)
numbers_df = numbers_df.withColumn("double_value", col("value") * 2)
numbers_df.show()