from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkChallenge").getOrCreate()

data = [
    ("E01", "HR", 55000, "2022-03-10"),
    ("E02", "HR", 60000, "2021-06-15"),
    ("E03", "Finance", 75000, "2020-01-12"),
    ("E04", "Finance", 82000, "2022-11-20"),
    ("E05", "Finance", 66000, "2021-09-25"),
    ("E06", "IT", 95000, "2020-05-01"),
    ("E07", "IT", 87000, "2023-02-11"),
    ("E08", "IT", 72000, "2024-04-18"),
    ("E09", "Marketing", 45000, "2023-07-30"),
    ("E10", "Marketing", 50000, "2022-12-22"),
]

schema = StructType([
    StructField("emp_id", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("join_date", StringType(), True)
])

emp_df = spark.createDataFrame(data, schema=schema)
emp_df = emp_df.withColumn("join_date", F.to_date("join_date"))

grouped_df = (
    emp_df
    .groupBy("department")
    .agg(F.round(F.avg("salary"),2).alias("avg_salary"),
          F.min('salary').alias("min_salary"),
          F.max('salary').alias("max_salary"),
          F.count('emp_id').alias("num_employees"))
    .filter(F.col("avg_salary") > 60000)
)
grouped_df.show()
