from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-20", "ACTIVE"),
    ("U1", "2026-01-25", "INACTIVE"),
    ("U2", "2026-01-22", "ACTIVE"),
    ("U2", "2026-01-24", "SUSPENDED"),
    ("U3", "2026-01-23", "ACTIVE"),
]

df = (
    spark.createDataFrame(data, ["user_id", "status_date", "status"])
         .withColumn("status_date", F.to_date("status_date"))
)

df.show(truncate=False)

w = Window.partitionBy("user_id").orderBy(F.col('status_date').desc())

final = (df
         .withColumn('rnk', F.row_number().over(w))
         .filter(F.col('rnk') == 1)
         .select("user_id","status_date","status")
        )
final.show()