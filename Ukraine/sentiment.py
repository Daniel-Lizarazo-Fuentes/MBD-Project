from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lower, udf, when
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

# -------------------------------------- Setup -----------------------------------------
sc = SparkContext(appName="")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# ------------------------------------- Load data  --------------------------------------

df = spark.read.parquet("/user/s2551055/NewsData_full/*/*.parquet").filter(
    (col("published_date").isNotNull())
    & (col("published_date") >= "2022-01-01")
    & (col("language").isNotNull())
    & (col("title").isNotNull())
    # & (col("plain_text").isNotNull()) # This already crashes even at large numbers of executors
    & (col("language").isin("en", "es", "ru", "ar"))
)

# ------------------------------------- Filtering ---------------------------------------

keywords_dict = {
    "en": ["ukraine", "russia"],
    "es": ["ucrania", "rusia"],
    "ru": ["украина", "россия"],
    "ar": ["أوكرانيا", "روسيا"],
}

filter_condition = None
for keywords in keywords_dict.values():
    condition = F.col("title").contains(keywords[0]) | F.col("title").contains(
        keywords[1]
    )
    filter_condition = (
        condition if filter_condition is None else filter_condition | condition
    )

filtered_df = df.filter(filter_condition)

# --------------------------------- Printing results ----------------------------------

# print("Rows before filtering: ",df.count())
# print("Rows after filtering: ",filtered_df.count())

# --------------------------------- Sentiment analysis ----------------------------------

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

# All executors should have access to the keywords
broadcast_keywords = spark.sparkContext.broadcast(sentiment_keywords)

def classify_sentiment(text, lang):
    keywords = broadcast_keywords.value.get(lang, {})
    positive_words = set(keywords.get('positive', []))
    negative_words = set(keywords.get('negative', []))

    # Tokenize the text and count occurrences of positive and negative words
    words = text.split()  # Use split to break the text into words
    positive_count = sum(1 for word in words if word.lower() in positive_words)
    negative_count = sum(1 for word in words if word.lower() in negative_words)

    return positive_count >= negative_count


classify_sentiment_udf = F.udf(lambda text, lang: classify_sentiment(text, lang), BooleanType())

df_with_sentiment = filtered_df.withColumn("sentiment_positive", classify_sentiment_udf(F.col("title"), F.col("language")))
sentiment_counts = (df_with_sentiment.groupBy("language", "sentiment_positive").count()).orderBy("language")


# --------------------------------- Printing results ----------------------------------
sentiment_counts.show()
