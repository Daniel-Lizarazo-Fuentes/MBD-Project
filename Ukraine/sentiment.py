from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lower
from pyspark.sql import functions as F


# -------------------------------------- Setup -----------------------------------------
sc = SparkContext(appName="")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# ------------------------------------- Load data  --------------------------------------

path = "/user/s2551055/NewsData_full/*/*.parquet"
df = spark.read.parquet(path).filter(
    (col("language").isNotNull())
    & (col("published_date") >= "2022-01-01") 
)

# ------------------------------------- Filtering ---------------------------------------

filtered_df = df.filter(
    (col("title").contains("ukraine") | col("title").contains("russia"))
    | (col("title").contains("ucrania") | col("title").contains("rusia"))
    | (col("title").contains("украина") | col("title").contains("россия"))
    | (col("title").contains("ukrajina") | col("title").contains("rusko"))
    | (col("title").contains("أوكرانيا") | col("title").contains("روسيا"))
)

# --------------------------------- Printing results ----------------------------------

print(df.count())
print(filtered_df.count())
