from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-24", 100),
    ("U1", "2026-01-25", 200),
    ("U1", "2026-01-26", 50),
    ("U2", "2026-01-25", 300),
    ("U2", "2026-01-26", 100),
]

df = (
    spark.createDataFrame(data, ["user_id", "event_date", "amount"])
         .withColumn("event_date", F.to_date("event_date"))
)

df.show(truncate=False)

w = (Window
     .partitionBy("user_id")
     .orderBy(F.col("event_date").cast("timestamp").cast("long"))
     .rangeBetween(-3 * 24 * 60 * 60, 0)
    )

final = (df
         .withColumn('rolling_3_day_sum', F.sum(F.col('amount')).over(w))
         .select("user_id","event_date","amount",'rolling_3_day_sum')
        )
final.show()