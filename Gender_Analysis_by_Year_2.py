from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, split, count, regexp_extract, lit, when, round, year, to_date, regexp_replace
)

# Initialize SparkSession
spark = SparkSession.builder.appName("Gender Analysis by Year2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load gender dataset
gender_file_path = "hdfs:///user/s2857634/wgnd_langctry.csv"
gender_df = spark.read.csv(gender_file_path, header=True, inferSchema=True)
gender_df = gender_df.withColumn("name", lower(trim(col("name")))).dropDuplicates(["name"])

# Country mapping
country_mapping = {
    "it": "Italy", "de": "Germany", "fr": "France", "us": "United States",
    "uk": "United Kingdom", "ca": "Canada", "au": "Australia", "in": "India",
    "cn": "China", "jp": "Japan", "ru": "Russia", "br": "Brazil",
    "es": "Spain", "mx": "Mexico", "za": "South Africa", "kr": "South Korea",
    "nl": "Netherlands", "se": "Sweden", "no": "Norway", "fi": "Finland"
}

mapping_df = spark.createDataFrame(country_mapping.items(), ["domain", "country"])

# Function to process data for a single year
def process_year_with_country(current_year):
    year_path = f"hdfs:///user/s2551055/NewsData_full/{current_year}/*.parquet"
    cc_news_df = spark.read.parquet(year_path)

    # Standardize date format
    cc_news_df = cc_news_df.withColumn(
        "published_date",
        regexp_replace(col("published_date"), r"(\d{4}-\d{1,2}-)(\d{1})$", r"\g<1>0\g<2>")
    )
    cc_news_df = cc_news_df.withColumn(
        "published_date", to_date(col("published_date"), "yyyy-MM-dd")
    ).filter(col("published_date").isNotNull())
    cc_news_df = cc_news_df.withColumn("year", year(col("published_date")))

    # Filter authors and extract first names
    authors_df = cc_news_df.select("author", "publisher", "year").filter(col("author").isNotNull())
    authors_df = authors_df.withColumn("first_name", lower(trim(split(col("author"), " ").getItem(0))))
    authors_df = authors_df.withColumn("domain", regexp_extract(col("publisher"), r"([a-zA-Z]+)$", 1))

    # Map domains to countries
    authors_df = authors_df.join(mapping_df, "domain", "left").filter(col("country").isNotNull())

    # Join authors with gender data
    matched_df = authors_df.join(
        gender_df,
        authors_df["first_name"] == gender_df["name"],
        "inner"
    ).select(
        authors_df["country"],
        gender_df["gender"],
        authors_df["year"]
    )

    # Aggregate by gender, country, and year
    return matched_df.groupBy("country", "gender", "year").agg(count("*").alias("count"))

# Process data for each year
years = range(2016, 2025)
combined_gender_country_year_counts = None

for current_year in years:
    gender_country_year_counts = process_year_with_country(current_year)
    if combined_gender_country_year_counts is None:
        combined_gender_country_year_counts = gender_country_year_counts
    else:
        combined_gender_country_year_counts = combined_gender_country_year_counts.union(gender_country_year_counts)

# Normalize gender column
combined_gender_country_year_counts = combined_gender_country_year_counts.withColumn(
    "gender",
    when(col("gender") == "M", "male")
    .when(col("gender") == "F", "female")
    .otherwise(col("gender"))
)

# Filter years 2016 to 2024
combined_gender_country_year_counts = combined_gender_country_year_counts.filter(
    col("year").between(2016, 2024)
)

# Calculate male-to-female ratios by year and country
pivot_df = combined_gender_country_year_counts.groupBy("country", "year").pivot("gender").sum("count")
pivot_df = pivot_df.fillna(0)
pivot_df = pivot_df.withColumn(
    "male_female_ratio",
    when(col("female") == 0, lit(None))
    .otherwise(round(col("male") / col("female"), 2))
)

# Pivot to have years as columns
final_df = pivot_df.groupBy("country").pivot("year", list(range(2016, 2025))).agg({"male_female_ratio": "first"})
final_df = final_df.orderBy("country")
final_df.show(100, truncate=False)

# Stop SparkSession
spark.stop()
