from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "A", "2026-01-01", "2026-01-10"),
    ("U1", "B", "2026-01-05", "2026-01-15"),
    ("U1", "C", "2026-01-15", "2026-01-20"),
    ("U2", "X", "2026-02-01", "2026-02-10"),
    ("U2", "Y", "2026-02-05", "2026-02-08"),
]

df = (
    spark.createDataFrame(
        data,
        ["user_id", "attr_value", "start_date", "end_date"]
    )
    .withColumn("start_date", F.to_date("start_date"))
    .withColumn("end_date", F.to_date("end_date"))
)

df.show(truncate=False)

w = Window.partitionBy("user_id").orderBy(F.col("start_date"))
df_tail = (
    df
    .withColumn("next_start_date", F.lead(F.col("start_date"), 1).over(w))
    .withColumn("next_end_date", F.lead(F.col("end_date"), 1).over(w))
    .filter(
        (F.col("next_start_date").isNotNull()) &
        (F.col("end_date") > F.col("next_end_date"))
    )
    .select(
        "user_id",
        "attr_value",
        F.date_add(F.col("next_end_date"), 1).alias("start_date"),
        F.col("end_date").alias("end_date")
    )
)
df_tail.show()

final = (df
         .withColumn("next_start_date", F.lead(F.col("start_date"), 1).over(w))
         .withColumn(
           "brk_flg", 
           F.when(F.col("end_date") >= F.col("next_start_date"), F.date_sub(F.col('next_start_date'), 1))
             .when(F.col("next_start_date").isNull(), F.col('end_date'))
             .otherwise(F.col('end_date'))
         )
         .select("user_id","attr_value","start_date",F.col("brk_flg").alias("end_date"))
        )
final_df = final.unionByName(df_tail)
final.show()
final_df.show(truncate=False)