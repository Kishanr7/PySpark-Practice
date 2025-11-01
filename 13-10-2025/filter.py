from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum
# Initialize Spark
spark = SparkSession.builder.appName("DailyPySparkPractice").getOrCreate()

# Sample data
products_data = [
    (1, "Laptop", "Electronics", 800),
    (2, "Mouse", "Electronics", 20),
    (3, "T-Shirt", "Clothing", 15),
    (4, "Shoes", "Footwear", 60),
    (5, "Phone", "Electronics", 600),
    (6, "Jacket", "Clothing", 120)
]

# Schema
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", IntegerType(), True)
])

# Create DataFrame
products_df = spark.createDataFrame(products_data, schema=products_schema)

# Show DataFrame
products_df.show()

products_df.filter((col('category')=='Electronics') & (col('price')>100)).select('name','price').show()