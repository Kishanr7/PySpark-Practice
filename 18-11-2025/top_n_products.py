# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySpark_TopN").getOrCreate()

data = [
    ("Electronics", "Laptop",     1200),
    ("Electronics", "Phone",       800),
    ("Electronics", "Headphones",  150),
    ("Electronics", "Monitor",     400),

    ("Books",       "Novel",       200),
    ("Books",       "Textbook",    200),
    ("Books",       "Comics",       50),
    ("Books",       "Magazine",     30),
]

schema = StructType([
    StructField("category", StringType(), True),
    StructField("product",  StringType(), True),
    StructField("revenue",  IntegerType(), True),
])

sales_df = spark.createDataFrame(data, schema)

w = (Window
     .partitionBy("category")
     .orderBy(F.col("revenue").desc(), F.col("product").asc())
)
final = (sales_df
         .withColumn("rank", F.row_number().over(w))
         .filter(F.col("rank") <= 2)
)

final.show(truncate=False)