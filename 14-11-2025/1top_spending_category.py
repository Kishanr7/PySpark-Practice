#  First Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkQuestion").getOrCreate()

data = [
    ("C001", "T001", "Laptop",      "Electronics", "2025-09-01", 1200.0),
    ("C001", "T002", "Mouse",       "Electronics", "2025-09-10",   25.0),
    ("C001", "T003", "Notebook",    "Stationery",  "2025-09-12",   15.0),
    ("C001", "T010", "Printer",     "Office",      "2025-09-15",  100.0),
    ("C001", "T011", "Desk",        "Furniture",   "2025-09-20",  300.0),

    ("C002", "T004", "Chair",       "Furniture",   "2025-09-05",  150.0),
    ("C002", "T005", "Desk",        "Furniture",   "2025-09-06",  350.0),
    ("C002", "T006", "Pen",         "Stationery",  "2025-09-07",    5.0),
    ("C002", "T012", "Lamp",        "Home",        "2025-09-08",   50.0),

    ("C003", "T007", "Phone",       "Electronics", "2025-09-02",  800.0),
    ("C003", "T008", "Headset",     "Electronics", "2025-09-03",  100.0),
    ("C003", "T009", "Notebook",    "Stationery",  "2025-09-04",   20.0),
    ("C003", "T013", "Cabinet",     "Furniture",   "2025-09-10",  900.0),
]

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("amount", DoubleType(), True),
])

transactions_df = spark.createDataFrame(data, schema)


window_spec = Window.partitionBy(F.col('customer_id')).orderBy(F.col('total_spent').desc(),F.col('category').asc())
cust_df = (
  transactions_df
    .groupBy('customer_id')
    .agg(F.sum('amount').alias('top_spent_all'))
    .select('customer_id','top_spent_all')
)
result1_df = (
  transactions_df
    .groupBy('customer_id','category')
    .agg(F.sum('amount').alias('total_spent'))
    .withColumn('rank', F.row_number().over(window_spec))
    .filter(F.col('rank') == 1)
    .select('customer_id',col('category').alias('top1_category'),col('total_spent').alias('top1_spent'))
)

result2_df = (
  transactions_df
    .groupBy('customer_id','category')
    .agg(F.sum('amount').alias('total_spent'))
    .withColumn('rank', F.row_number().over(window_spec))
    .filter(F.col('rank') == 2)
    .select('customer_id',col('category').alias('top2_category'),col('total_spent').alias('top2_spent'))
)

result3_df = result1_df.join(result2_df, on ='customer_id')
final_df = result3_df.join(cust_df, on = 'customer_id')
final1_df = final_df.withColumn('share_top1', F.round(F.col('top1_spent')/F.col('top_spent_all'),2))
final1_df.select('customer_id','top1_category','top1_spent','top2_category','top2_spent','share_top1')
final1_df.show()