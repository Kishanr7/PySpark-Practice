from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, row_number, rank, dense_rank, sum, upper, round as rnd

spark = SparkSession.builder.appName("DailyPySparkQuestion").getOrCreate()

data = [
    ("C1","O1","2025-10-01","Electronics",1200.0),
    ("C1","O2","2025-10-05","Accessories",  50.0),
    ("C1","O9","2025-10-06","Electronics", 300.0),

    ("C2","O3","2025-10-03","Electronics", 800.0),
    ("C2","O4","2025-10-04","Accessories", 200.0),
    ("C2","O10","2025-10-08","Accessories",150.0),

    ("C3","O5","2025-10-06","Home",         90.0),
    ("C3","O6","2025-10-07","Accessories",  30.0),
    ("C3","O11","2025-10-09","Home",        60.0),

    ("C4","O7","2025-10-02","Electronics",1100.0),
    ("C4","O8","2025-10-08","Electronics", 300.0),
    # Tie on purpose between Accessories (1400) and Electronics (1400)
    ("C4","O12","2025-10-10","Accessories",1400.0),
]

schema = StructType([
    StructField("cust_id",     StringType(), True),
    StructField("order_id",    StringType(), True),
    StructField("order_date",  StringType(), True),
    StructField("category",    StringType(), True),
    StructField("amount",      DoubleType(), True),
])

txns_df = spark.createDataFrame(data, schema)

window_spec = Window.partitionBy('cust_id').orderBy(col('total_spent').desc(), col('category').asc())

result = (
    txns_df
      .groupBy('cust_id','category')
      .agg(F.sum('amount').alias('total_spent'))
      .withColumn('rank', row_number().over(window_spec))
      .filter(col('rank') == 1)
      .select("cust_id", F.col("category").alias("top_category"), F.col("total_spent"))
)

result.show()
