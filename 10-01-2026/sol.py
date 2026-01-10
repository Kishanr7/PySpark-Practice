# Second Approach
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, to_date, col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.getOrCreate()

data = [
    ("U1", "2026-01-07"),
    ("U1", "2026-01-08"),
    ("U1", "2026-01-09"),
    ("U1", "2026-01-10"),
    ("U2", "2026-01-06"),
    ("U2", "2026-01-08"),
    ("U2", "2026-01-09"),
    ("U3", "2026-01-10"),
    ("U4", "2026-01-05"),
]

columns = ["user_id", "activity_date"]

activity_df = (
    spark.createDataFrame(data, columns)
         .withColumn("activity_date", to_date("activity_date"))
)
activity_df.show(truncate=False)

w = Window.partitionBy("user_id").orderBy(F.col("activity_date"))
w1 = Window.partitionBy("user_id").orderBy(col("session_end_date").desc())
final = (activity_df
         .withColumn("prev_date", F.lag(F.col('activity_date'),1).over(w))
         .withColumn(
           "break_flag", 
           F.when(F.col('prev_date').isNull(), 1)
            .when(datediff("activity_date", "prev_date") > 1, 1)
            .otherwise(0)
         )
         .withColumn("session_id", F.sum('break_flag').over(w))
         .groupBy('user_id','session_id')
         .agg(
           F.count('*').alias("streak_length"),
           F.max('activity_date').alias('session_end_date')
         )
         .withColumn(
           "rn",
           F.row_number().over(w1)
         )
         .filter(col("rn") == 1)
         .withColumn(
           'current_streak_length',
           F.when(
             datediff(F.current_date(), col("session_end_date")) == 0,
             F.col('streak_length')
           ).otherwise(0)
         )
         .select('user_id','current_streak_length'))
         
final.show(truncate=False)