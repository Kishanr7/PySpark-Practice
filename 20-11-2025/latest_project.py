# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("e1", "ProjectA", "2025-11-01"),
    ("e1", "ProjectB", "2025-11-05"),
    ("e1", "ProjectC", "2025-11-10"),
    ("e2", "ProjectX", "2025-11-03"),
    ("e2", "ProjectY", "2025-11-07"),
    ("e3", "ProjectZ", "2025-11-08"),
]

columns = ["employee_id", "project_name", "assignment_date"]

df = spark.createDataFrame(data, columns).withColumn("assignment_date", F.to_date(col("assignment_date")))

w = (Window
     .partitionBy("employee_id")
     .orderBy(F.col("assignment_date").desc())
)
final = (df
         .withColumn("rnk", F.row_number().over(w))
         .filter(F.col("rnk") <= 2)
)

final.show(truncate=False)