#do all imports for spark config
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, col,lit, count, sum, udf,desc
import csv
from collections import defaultdict
import pandas as pd



# -------------------------------------- Setup -----------------------------------------
sc = SparkContext(appName="ClimateChangeArticles")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


# languages=[
#     "BG", "CS", "DA", "DE", "EL", "ES", "ET", "FI", "FR", "GA", "HU", "IT",
#     "LT", "LV", "MT", "NL", "PL", "PT", "RO", "SK", "SL", "SV"
# ]
years = ["2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
results_list = []
# List of columns to load from the Parquet files
columns_to_load = ['plain_text', 'language']



#create the lookup dictionary
lookup_dict = defaultdict(list)

filename="climate_change_terms_translations.csv"

with open(filename, "r", newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=",", quotechar="|")
    next(reader)  # Skip the first line (header)
    for line in reader:
        lang_code, word=line
        lookup_dict[lang_code.lower()].append(word.lower())

lookup_dict = {
    lang: f"(?i){'|'.join(f'({phrase})' for phrase in words)}"
    for lang, words in lookup_dict.items()
}



for lang,words in lookup_dict.items():
    df_all = None
    for year_folder in years:
         path = f"/user/s2551055/NewsData/{year_folder}/"
         # df_tmp=spark.read.parquet(path).select(*columns_to_load).withColumn("year", lit(year_folder))
         # df_tmp_lang=df_tmp.filter(df_tmp.language==lang)
         df_tmp = (
             spark.read.parquet(path)
             .select(*columns_to_load)
             .withColumn("year", lit(year_folder))
             .filter(col("language") == lang)  # Filter by language directly
         )
         if df_all is None:
             df_all=df_tmp
         else:
             df_all=df_all.union(df_tmp)

#    df_all=df_all.repartition(80,"language")

    # Search for climate change terms in the 'plain_text' column
    df_search = df_all.filter(col("plain_text").rlike(words))
    # Group by year and count the number of articles
    df_search = df_search.groupBy(col("year")).agg(count("*").alias("no_climate_articles"))

    df_to_append=df_search.toPandas()
    df_to_append['language']=lang
    results_list.append(df_to_append)



# Concatenate all the Pandas DataFrames at once
final_results_df = pd.concat(results_list, ignore_index=True)

print(final_results_df)

# Save the results to a CSV file
final_results_df.to_csv('test_results_climate2.csv', index=False)



