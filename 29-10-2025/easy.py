from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkPractice").getOrCreate()

data = [
    ("Electronics", "John", 1200),
    ("Electronics", "Alice", 800),
    ("Furniture", "Bob", 1500),
    ("Furniture", "Carol", 700),
    ("Clothing", "David", 300),
    ("Clothing", "Emma", 2200),
    ("Sports", "Frank", 1800),
    ("Beauty", "Grace", 900),
    ("Beauty", "Hannah", 1300),
    ("Groceries", "Ian", 400),
    ("Groceries", "Jack", 1100)
]

schema = StructType([
    StructField("category", StringType(), True),
    StructField("salesperson", StringType(), True),
    StructField("sales", IntegerType(), True)
])

sales_df = spark.createDataFrame(data, schema=schema)
# filtered_df = sales_df.filter(F.col("sales") > 1000)

agg_df = (
    sales_df
    .groupBy("category")
    .agg(F.sum("sales").alias("total_sales"))
    .filter(F.col("total_sales") > 1000)
    .orderBy(F.col("total_sales").desc())
)
agg_df.show(3)