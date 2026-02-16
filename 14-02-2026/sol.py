from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()
data = [
    ("P1", "2026-02-01", 100),
    ("P1", "2026-02-02", 120),
    ("P1", "2026-02-03", 110),
    ("P1", "2026-02-04", 130),

    ("P2", "2026-02-01", 200),
    ("P2", "2026-02-02", 210),
    ("P2", "2026-02-03", 220),

    ("P3", "2026-02-01", 50),
]

df = (
    spark.createDataFrame(data, ["product_id", "sale_date", "amount"])
         .withColumn("sale_date", F.to_date("sale_date"))
)
w = (Window
     .partitionBy("product_id")
     .orderBy(F.col("sale_date"))
    )
w1 = (
  Window
  .partitionBy('product_id','session_id')
)
result = (
    df
    .withColumn("prev_amount", F.lag(F.col('amount'),1).over(w))
    .withColumn(
      "break_flg", 
      F.when(
        F.col('amount')< F.col('prev_amount'), 1)
       .when(F.col('prev_amount').isNull(), 1)
       .otherwise(0)
    )
    .withColumn('session_id', F.sum('break_flg').over(w))
    .withColumn('max_con_d_p_s', F.count('session_id').over(w1))
    .filter(F.col('max_con_d_p_s')>=2)
    .groupBy('product_id','session_id').agg(
      F.min(F.col('sale_date')).alias('streak_start_date'),
      F.max('sale_date').alias('streak_end_date'),
      F.max('max_con_d_p_s').alias('streak_length')
    )
)

result.show(truncate=False)