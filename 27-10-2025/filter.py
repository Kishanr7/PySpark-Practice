from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, upper

# Initialize Spark
spark = SparkSession.builder.appName("DailyPySparkPractice").getOrCreate()

# Sample data
employees_data = [
    (1, "John Doe", "HR", 50000),
    (2, "Alice Johnson", "IT", 85000),
    (3, "Bob Smith", "Sales", 62000),
    (4, "Carol White", "IT", 90000),
    (5, "David Black", "HR", 48000),
    (6, "Emma Brown", "Sales", 75000)
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
# employees_df.show()
filtered_df = employees_df.filter(employees_df.department == 'IT').withColumn('name_upper',upper(col("name")))
filtered_df.select('name_upper','salary').orderBy(col("salary").desc()).show()

