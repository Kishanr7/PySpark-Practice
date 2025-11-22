# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("Alice", "HR", 50000),
    ("Bob", "HR", 60000),
    ("Charlie", "HR", 55000),
    ("David", "IT", 70000),
    ("Eve", "IT", 72000),
    ("Frank", "IT", 71000),
    ("Grace", "Finance", 65000),
    ("Heidi", "Finance", 64000),
]

columns = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data, columns)

w = (Window
     .partitionBy("department")
     .orderBy(F.col("salary").desc())
)
final = (df
         .withColumn("rnk", F.dense_rank().over(w))
         .filter(F.col("rnk") == 2)
)
final.show(truncate=False)