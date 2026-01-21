# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-01 10:00:00", "John",  None,     "US"),
    ("U1", "2026-01-02 09:00:00", None,    "M",      None),
    ("U1", "2026-01-03 08:00:00", None,    None,     "CA"),
    ("U2", "2026-01-01 11:00:00", "Alice", "F",      "UK"),
    ("U2", "2026-01-04 12:00:00", None,    None,     None),
    ("U3", "2026-01-02 15:00:00", None,    "M",      None),
]

profile_df = (
    spark.createDataFrame(
        data,
        ["user_id", "update_ts", "name", "gender", "country"]
    )
    .withColumn("update_ts", F.to_timestamp("update_ts"))
)

profile_df.show(truncate=False)

w = Window.partitionBy("user_id").orderBy(F.col("update_ts"))
w2 = Window.partitionBy("user_id").orderBy(F.col("update_ts").desc())

final = (profile_df
         .withColumn("last_name", F.last_value(F.col("name"), ignoreNulls=True).over(w))
         .withColumn("last_gender", F.last_value(F.col("gender"), ignoreNulls=True).over(w))
         .withColumn("last_country", F.last_value(F.col("country"), ignoreNulls=True).over(w))
         .withColumn("rnk", F.row_number().over(w2))
         .filter(F.col("rnk") == 1)
         .select(
           "user_id",
           F.col("last_name").alias("name"),
           F.col("last_gender").alias("gender"),
           F.col("last_country").alias("country"),
         )
        )
         
final.show(truncate=False)