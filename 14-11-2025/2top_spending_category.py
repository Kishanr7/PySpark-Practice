# Second Approach
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


agg = (
  transactions_df
    .groupBy("customer_id","category")
    .agg(F.sum("amount").alias("total_spent"))
)

w = Window.partitionBy("customer_id").orderBy(F.col("total_spent").desc(), F.col("category").asc())

ranked = agg.withColumn("rn", F.row_number().over(w))

top1 = ranked.filter("rn = 1").select("customer_id",
                                      F.col("category").alias("top1_category"),
                                      F.col("total_spent").alias("top1_spent"))

top2 = ranked.filter("rn = 2").select("customer_id",
                                      F.col("category").alias("top2_category"),
                                      F.col("total_spent").alias("top2_spent"))

cust_total = agg.groupBy("customer_id").agg(F.sum("total_spent").alias("total_spent_all"))

final = (top1
         .join(top2, on="customer_id", how="left")
         .join(cust_total, on="customer_id", how="left")
         .withColumn("share_top1", F.round(F.col("top1_spent") / F.col("total_spent_all"), 2))
         .select("customer_id","top1_category","top1_spent","top2_category","top2_spent","share_top1")
)

final.show(truncate=False)