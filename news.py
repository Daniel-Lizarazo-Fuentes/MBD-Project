from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, col, count, sum

# -------------------------------------- Setup -----------------------------------------
sc = SparkContext(appName="")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# -------------------------------------- Basic schema inference  -----------------------------------------

df = spark.read.option("mergeSchema", "true").parquet("/user/s2551055/NewsData/2016")
df.printSchema()

# RQ 1:
# TODO

# RQ 2:
# TODO

# RQ3: Does the month of the year influence the amount of articles published by langauge?
years = ["2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
combined_df = None

for year_folder in years:
    path = f"/user/s2551055/NewsData/{year_folder}/"
    df = spark.read.parquet(path)

    monthly_language_count = (
        df.select(
            month(col("published_date").cast("timestamp")).alias("month"),
            col("language"),
        )
        .groupBy("month", "language")
        .agg(count("*").alias("article_count"))
    )

    if combined_df is None:
        combined_df = monthly_language_count
    else:
        combined_df = combined_df.union(monthly_language_count)

result_df = (
    combined_df.groupBy("month", "language")
    .agg(sum("article_count").alias("total_article_count"))
    .orderBy("language", "month")
)
result_df.show(100)

# RQ 4:
# TODO

spark.stop()
