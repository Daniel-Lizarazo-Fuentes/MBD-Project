from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lower


# -------------------------------------- Setup -----------------------------------------
sc = SparkContext(appName="")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# ------------------------------------- Load data  --------------------------------------

path = "/user/s2551055/NewsData/*/*.parquet"
df = spark.read.parquet(path)

# ------------------------------------- Filtering ---------------------------------------

date_start = "2022-01-01"

keywords_dict = {
    "en": ["ukraine", "russia"],
    "fr": ["ukraine", "russie"],
    "es": ["ucrania", "rusia"],
    "ru": ["украина", "россия"],
    "cz": ["ukrajina", "rusko"],
    "ar": ["أوكرانيا", "روسيا"],
}

keyword_filters = [ 
    ((col("language") == lang) & (lower(col("plain_text")).rlike(f"{kw[0]}.*?{kw[1]}")))
    for lang, kw in keywords_dict.items()
]

# Leave |= as it needs to find only one of the languages (they're ofc never true for all at the same time)
filter_condition = keyword_filters[0]
for condition in keyword_filters[1:]:
    filter_condition |= condition

filtered_df = df.filter(
    (col("published_date") >= date_start)
    & (col("language").isNotNull())
    & filter_condition
).select("plain_text", "published_date", "language", "title")

# --------------------------------- Printing results ----------------------------------

print(df.count())
print(filtered_df.count())
