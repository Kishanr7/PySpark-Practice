from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkPractice").getOrCreate()
data = [
    ("Alice", "HR", 60000),
    ("Bob", "HR", 65000),
    ("Charlie", "HR", 60000),
    ("David", "IT", 75000),
    ("Eve", "IT", 72000),
    ("Frank", "IT", 75000),
    ("Grace", "Finance", 80000),
    ("Heidi", "Finance", 85000),
    ("Ivan", "Finance", 85000),
    ("Judy", "Finance", 78000)
]

columns = ["employee_name", "department", "salary"]
employees_df = spark.createDataFrame(data, columns)

window_spec = Window.partitionBy('department').orderBy(F.col('salary').desc(),F.col('employee_name').asc())
filter_df = (
    employees_df
      .withColumn("rank", row_number().over(window_spec))
      .filter(F.col('rank') <=2)
)

filter_df.show()
# grouped_df.show()
