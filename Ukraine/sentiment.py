from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lower, udf, when, year, substring
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import BooleanType
from pyspark.sql import functions as F

# -------------------------------------- Setup -----------------------------------------
sc = SparkContext(appName="")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

base_path = "/user/s2551055/NewsData/"

# -------------------------------------- Keywords -----------------------------------------
keywords_dict = {
    "en": ["ukraine", "russia"],
    "es": ["ucrania", "rusia"],
    "ru": ["украина", "россия"],
    "ar": ["أوكرانيا", "روسيا"],
}

sentiment_keywords = {
    "en": {
        "positive": [
            "good",
            "great",
            "happy",
            "success",
            "win",
            "excellent",
            "positive",
            "joy",
        ],
        "negative": [
            "bad",
            "terrible",
            "sad",
            "fail",
            "lose",
            "horrible",
            "negative",
            "angry",
        ],
    },
    "es": {
        "positive": [
            "bueno",
            "excelente",
            "feliz",
            "éxito",
            "ganar",
            "positivo",
            "alegría",
        ],
        "negative": [
            "malo",
            "terrible",
            "triste",
            "fallar",
            "perder",
            "horrible",
            "negativo",
            "enojado",
        ],
    },
    "ru": {
        "positive": [
            "хороший",
            "отличный",
            "счастливый",
            "успех",
            "победа",
            "позитивный",
            "радость",
        ],
        "negative": [
            "плохой",
            "ужасный",
            "грустный",
            "провал",
            "потеря",
            "негативный",
            "злой",
        ],
    },
    "ar": {
        "positive": ["جيد", "عظيم", "سعيد", "نجاح", "فوز", "إيجابي", "فرح"],
        "negative": ["سيء", "فظيع", "حزين", "فشل", "خسارة", "سلبي", "غاضب"],
    },
}
# -------------------------------------- Functions -----------------------------------------
filter_condition = None
for keywords in keywords_dict.values():
    condition = col("title").contains(keywords[0]) | col("title").contains(keywords[1])
    filter_condition = (
        condition if filter_condition is None else filter_condition | condition
    )


def classify_sentiment(text, lang):
    keywords = broadcast_keywords.value.get(lang, {})
    positive_words = set(keywords.get("positive", []))
    negative_words = set(keywords.get("negative", []))

    words = text.split() 
    positive_count = sum(1 for word in words if word.lower() in positive_words)
    negative_count = sum(1 for word in words if word.lower() in negative_words)

    return positive_count >= negative_count


classify_sentiment_udf = udf(
    lambda text, lang: classify_sentiment(text, lang), BooleanType()
)

# ------------------------------------- Filtering --------------------------------------
df = spark.read.parquet("/user/s2551055/NewsData/*/*.parquet").filter(
    (col("published_date").isNotNull())
    & (col("published_date") >= "2022-01-01")
    & (col("language").isNotNull())
    & (col("title").isNotNull())
    # & (col("plain_text").isNotNull()) # This already crashes even at large numbers of executors
    & (col("language").isin("en", "es", "ru", "ar"))
)

df = df.filter(filter_condition)
df = df.withColumn("year", year(col("published_date")))
df = df.withColumn("plain_text", substring("plain_text", 0, 500))
df = df.withColumn("plain_text", lower(col("plain_text")))
size = df.count()
print("Rows before filtering: ", size)

# --------------------------------- Sentiment analysis ----------------------------------

# All executors should have access to all keywords
broadcast_keywords = spark.sparkContext.broadcast(sentiment_keywords)

result = spark.createDataFrame(
    [], "year STRING, language STRING, sentiment_positive BOOLEAN, count LONG"
)

df = df.repartition(20, "year", "language") 

chunk_size = 5000
offset = 0

while offset < size:
    chunk_df = df.limit(offset + chunk_size).subtract(df.limit(offset))

    df_with_sentiment = chunk_df.withColumn(
        "sentiment_positive", classify_sentiment_udf(col("plain_text"), col("language"))
    )

    sentiment_counts = df_with_sentiment.groupBy(
        "year", "language", "sentiment_positive"
    ).count()

    result = result.union(sentiment_counts)

    offset += chunk_size


final_result = (
    result.groupBy("year", "language", "sentiment_positive")
    .agg(F.sum("count").alias("count"))
    .orderBy("language", "year", "sentiment_positive")
)


final_result.show(100)
