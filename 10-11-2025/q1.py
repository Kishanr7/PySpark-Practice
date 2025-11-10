from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkQuestion").getOrCreate()

data = [
    ("C1","O1001","2025-09-28",250.0),
    ("C1","O1010","2025-10-05",300.0),
    ("C1","O1020","2025-10-20",180.0),

    ("C2","O2001","2025-10-01",800.0),
    ("C2","O2005","2025-10-04",200.0),

    ("C3","O3001","2025-10-02",120.0),

    ("C4","O4001","2025-10-01",1100.0),
    ("C4","O4010","2025-10-08",300.0),
    ("C4","O4020","2025-10-12",400.0),
]

schema = StructType([
    StructField("cust_id",    StringType(), True),
    StructField("order_id",   StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount",     DoubleType(), True),
])

orders_df = (spark.createDataFrame(data, schema)
             .withColumn("order_date", F.to_date("order_date")))

# Show the first few rows of the DataFrame
orders_df.show(5)

window_spec = Window.partitionBy(F.col('cust_id')).orderBy(F.col('order_date'))
ranked_spec = Window.partitionBy(F.col('cust_id')).orderBy(F.col('order_date').desc())
result_df = (
  orders_df
    .withColumn('prev_order_date', F.lag('order_date',1).over(window_spec))
    .withColumn('gap_days', datediff(col('order_date'),col('prev_order_date')))
    .withColumn('ranked', F.row_number().over(ranked_spec))
    .filter(F.col('ranked') <= 2)
).select('cust_id','order_id','order_date','prev_order_date','gap_days','amount')
  
result_df.show()