from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkPractice").getOrCreate()

data = [
    ("S1", "John", 85, 90, 88),
    ("S2", "Alice", 70, 95, 92),
    ("S3", "Bob", 82, 85, 80),
    ("S4", "Carol", 78, 82, 89),
    ("S5", "David", 90, 91, 94)
]

schema = StructType([
    StructField("student_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("math", IntegerType(), True),
    StructField("science", IntegerType(), True),
    StructField("english", IntegerType(), True)
])

students_df = spark.createDataFrame(data, schema=schema)
filter_df = (
    students_df.filter((col("math") > 80) & (col("science") > 80) & (col("english") > 80))
)
grouped_df = (
    students_df
    .agg(F.avg("math").alias("avg_math"),
          F.avg('science').alias("avg_science"),
          F.avg('english').alias("avg_english"))
)
filter_df.show()
grouped_df.show()
