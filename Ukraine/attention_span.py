from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lower, month, year, date_format

# -------------------------------------- Setup -----------------------------------------
sc = SparkContext(appName="AttentionCurveAnalysis")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# ------------------------------------- Load data  --------------------------------------
path = "/user/s2551055/NewsData_full/*/*.parquet"
required_columns = ['plain_text', 'published_date', 'language', 'title']
df = spark.read.parquet(path).select(required_columns)


# ------------------------------------- Filtering ---------------------------------------
date_start = "2022-01-01"

keywords_dict = {
    "en": ["ukraine", "russia"],
    "fr": ["ukraine", "russie"],
    "es": ["ucrania", "rusia"],
    "ru": ["украина", "россия"],
    "cz": ["ukrajina", "rusko"],
    "ar": ["أوكرانيا", "روسيا"],
    "de": ["ukraine", "russland"],
    "nl": ["ukraine", "rusland"],
}


keyword_filters = [
    ((col("language") == lang) & (lower(col("title")).rlike(f"{kw[0]}.*?{kw[1]}")))
    for lang, kw in keywords_dict.items()
]


filter_condition = keyword_filters[0]
for condition in keyword_filters[1:]:
    filter_condition |= condition

filtered_df = df.filter(
    (col("published_date") >= date_start) &
    (col("language").isNotNull()) &
    filter_condition
).select("plain_text", "published_date", "language", "title")

# --------------------------------- Grouping and Counting -------------------------------
result_df = filtered_df.withColumn("month_year", date_format("published_date", "yyyy-MM"))
summary_df = result_df.groupBy("language", "month_year").count()

# --------------------------------- Printing results ------------------------------------

print(f"Filtered rows count: {filtered_df.count()}")

#summary_df.show(40)

#summary_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("mydata.csv")
summary_df.write.mode("overwrite").csv("collection")
#summary_df.write.mode("overwrite").parquet("collection.parquet")
