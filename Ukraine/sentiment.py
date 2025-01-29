from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    lower,
    udf,
    when,
    year,
    month,
    substring,
)
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
import pandas as pd
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from transformers import XLMRobertaConfig, XLMRobertaModel

# -------------------------------------- Setup -----------------------------------------
sc = SparkContext(appName="")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# -------------------------------------- Keywords -----------------------------------------
keywords_dict = {
    "en": ["ukraine", "russia"],
    "es": ["ucrania", "rusia"],
    "ru": ["украина", "россия"],
    "ar": ["أوكرانيا", "روسيا"],
}

# -------------------------------------- Functions -----------------------------------------
ukraine_condition = (
    col("title").rlike(rf"\b{keywords_dict['en'][0]}\b")
    | col("title").rlike(rf"\b{keywords_dict['es'][0]}\b")
    | col("title").rlike(rf"\b{keywords_dict['ru'][0]}\b")
    | col("title").rlike(rf"\b{keywords_dict['ar'][0]}\b")
)

russia_condition = (
    col("title").rlike(rf"\b{keywords_dict['en'][1]}\b")
    | col("title").rlike(rf"\b{keywords_dict['es'][1]}\b")
    | col("title").rlike(rf"\b{keywords_dict['ru'][1]}\b")
    | col("title").rlike(rf"\b{keywords_dict['ar'][1]}\b")
)

both_condition = ukraine_condition & russia_condition

# ------------------------------------- Filtering --------------------------------------
print("Starting filtering...")
df = spark.read.parquet("/user/s2551055/NewsData/*/*.parquet").filter(
    (col("published_date").isNotNull())
    & (col("published_date") >= "2014-01-01")
    & (col("language").isNotNull())
    & (col("title").isNotNull())
    & (col("language").isin("en", "es", "ru", "ar"))
)

df = df.withColumn("year", year(col("published_date")))
df = df.withColumn("month", month(col("published_date")))
df = df.withColumn("title", lower(col("title")))
df = df.select("title", "language", "year", "month")

# print("Rows after filtering: ",  df.count())

# -------------------------------------- Sentiment analysis --------------------------------------

print("Load model...")
configuration = XLMRobertaConfig()
model = XLMRobertaModel(configuration)

print("Broadcast model...")
broadcast_model = spark.sparkContext.broadcast(model)

def analyze_sentiment(text):
    if not text:
        return "neutral"
    
    model = broadcast_model.value 
    result = model(text)[0]  
    return result["label"].lower() 

sentiment_udf = udf(analyze_sentiment, StringType())

print("Running sentiment analysis...")
df = df.withColumn("sentiment", sentiment_udf(col("title")))
df.show(20)


df_ukraine = df.filter(
    ukraine_condition & ~russia_condition
)  # Only Ukraine, not Russia
df_russia = df.filter(russia_condition & ~ukraine_condition)  # Only Russia, not Ukraine
df_both = df.filter(both_condition)

df_names = ["ukraine", "russia", "both"]
for df_partial, name in zip([df_ukraine, df_russia, df_both], df_names):
    final_result = (
        df_with_sentiment.groupBy("language", "year", "month","sentiment")
        .count()
        .orderBy("language", "year","month", "sentiment")
    )

    result_data = final_result.collect()
    df_pandas = pd.DataFrame(
        result_data, columns=["language", "year","month", "sentiment", "count"]
    )

    df_pandas.to_csv(f"{name}_results.csv", index=False)


# temp = df_with_sentiment.select("title", "language", "year", "sentiment").limit(500)
# data = temp.collect()
# pan = pd.DataFrame(
#     data, columns=["title", "language", "year", "sentiment"]
# )
# pan.to_csv("test.csv", index=False)


# final_result.show(100)

