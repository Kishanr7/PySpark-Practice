from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkQuestion").getOrCreate()

data = [
    ("C001", "T001", "Laptop", "Electronics", 1200.0),
    ("C001", "T002", "Mouse", "Electronics", 25.0),
    ("C001", "T003", "Notebook", "Stationery", 15.0),

    ("C002", "T004", "Chair", "Furniture", 150.0),
    ("C002", "T005", "Desk", "Furniture", 350.0),
    ("C002", "T006", "Pen", "Stationery", 5.0),

    ("C003", "T007", "Phone", "Electronics", 800.0),
    ("C003", "T008", "Headset", "Electronics", 100.0),
    ("C003", "T009", "Notebook", "Stationery", 20.0),
]

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
])

transactions_df = spark.createDataFrame(data, schema)


window_spec = Window.partitionBy(F.col('customer_id')).orderBy(F.col('total_spent').desc(),F.col('category').asc())
result_df = (
  transactions_df
    .groupBy('customer_id','category')
    .agg(F.sum('amount').alias('total_spent'))
    .withColumn('rank', F.row_number().over(window_spec))
    .filter(F.col('rank') <= 1)
    .select('customer_id',col('category').alias('top_category'),'total_spent','rank')
)
  
result_df.show()