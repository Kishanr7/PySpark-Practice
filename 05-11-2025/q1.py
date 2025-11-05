from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkQuestion").getOrCreate()

data = [
    ("North", "Alice", "Laptop", 5, 1200.0),
    ("North", "Bob", "Mouse", 20, 25.0),
    ("North", "Alice", "Monitor", 3, 300.0),
    ("South", "Carol", "Phone", 4, 800.0),
    ("South", "Dan", "Laptop", 2, 1100.0),
    ("South", "Carol", "Headset", 10, 100.0),
    ("East", "Eve", "Laptop", 3, 1300.0),
    ("East", "Frank", "Mouse", 15, 20.0),
    ("East", "Eve", "Monitor", 2, 400.0),
]

columns = ["region", "salesperson", "product", "units_sold", "price_per_unit"]
sales_df = spark.createDataFrame(data, columns)

window_spec = Window.partitionBy('region').orderBy(col('total_revenue').desc(), col('product').asc())

result = (
    sales_df
      .withColumn('line_revenue', col('units_sold') * col('price_per_unit'))
      .groupBy('region','product')
      .agg(F.sum('line_revenue').alias('total_revenue'))
      .withColumn('rank', row_number().over(window_spec))
      .filter(col('rank') == 1)
      .orderBy('region')
)

result.show()