from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    lower,
    udf,
    when,
    year,
    substring,
    pandas_udf,
)
from pyspark.sql.types import BooleanType
from pyspark.sql import functions as F
import pandas as pd

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
    positive_count = sum(words.count(word) for word in positive_words)
    negative_count = sum(words.count(word) for word in negative_words)

    return positive_count >= negative_count


classify_sentiment_udf = udf(
    lambda text, lang: classify_sentiment(text, lang), BooleanType()
)

# ------------------------------------- Filtering --------------------------------------
df = spark.read.parquet("/user/s2551055/NewsData_full/*/*.parquet").filter(
    (col("published_date").isNotNull())
    & (col("published_date") >= "2022-01-01")
    & (col("language").isNotNull())
    & (col("title").isNotNull())
    & (col("language").isin("en", "es", "ru", "ar"))
    & filter_condition
)

df = df.withColumn("year", year(col("published_date")))
# df = df.withColumn("plain_text", lower(substring(col("plain_text"), 0, 1000)))
df = df.repartition(20)

# size = df.count()
# print("Rows after filtering: ", size)

# --------------------------------- Sentiment analysis ----------------------------------

broadcast_keywords = spark.sparkContext.broadcast(sentiment_keywords)

df_with_sentiment = df.withColumn(
    "sentiment_positive", classify_sentiment_udf(col("title"), col("language"))
)

final_result = (
    df_with_sentiment.groupBy("language", "year", "sentiment_positive")
    .count()
    .orderBy("language", "year", "sentiment_positive")
)

final_result.show(100)
