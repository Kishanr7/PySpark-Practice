from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, upper, round as rnd

# Initialize Spark
spark = SparkSession.builder.appName("DailyPySparkPractice").getOrCreate()

# Sample data
employees_data = [
    (1,  "Alice Johnson",  "IT",    85000),
    (2,  "Carol White",    "IT",    90000),
    (3,  "Ethan Kumar",    "IT",    78000),
    (4,  "Isha Verma",     "IT",    90000),  # tie on purpose
    (5,  "John Doe",       "HR",    50000),
    (6,  "David Black",    "HR",    48000),
    (7,  "Nina Kapoor",    "HR",    52000),
    (8,  "Bob Smith",      "Sales", 62000),
    (9,  "Emma Brown",     "Sales", 75000),
    (10, "Liam Davis",     "Sales", 75000),  # tie on purpose
    (11, "Mia Chen",       "Sales", 68000)
]

# Schema
employees_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Create DataFrame
employees_df = spark.createDataFrame(employees_data, schema=employees_schema)
# employees_df.show(truncate=False)

grouped_df = employees_df.groupBy(col("department")).agg(F.avg("salary").alias("dept_avg"))
joined_df = employees_df.join(grouped_df,on = 'department')
window_spec=Window.partitionBy("department").orderBy(col('salary').desc())
result = (
    joined_df.withColumn("pct_diff_from_dept_avg", ((col("salary") - col("dept_avg")) / col("dept_avg")) * 100)
             .withColumn("rank", dense_rank().over(window_spec))
             .filter(col('rank') <=2)
             .select("department","emp_id","name","salary","dept_avg","pct_diff_from_dept_avg","rank")
             .withColumn("dept_avg", rnd(col("dept_avg"), 2))
             .withColumn("pct_diff_from_dept_avg", rnd(col("pct_diff_from_dept_avg"), 2))
             .orderBy(col('department').asc(), col('rank').asc(), col('salary').desc())
)
result.show()