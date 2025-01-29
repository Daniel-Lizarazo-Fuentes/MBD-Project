from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, split, count, regexp_extract, lit, when, round

# Initialize SparkSession
spark = SparkSession.builder.appName("Gender Analysis with Ratios").getOrCreate()
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
def process_year_with_country(year):
    year_path = f"hdfs:///user/s2551055/NewsData_full/{year}/*.parquet"
    cc_news_df = spark.read.parquet(year_path)

    # Filter authors and extract first names
    authors_df = cc_news_df.select("author", "publisher").filter(col("author").isNotNull())
    authors_df = authors_df.withColumn("first_name", lower(trim(split(col("author"), " ").getItem(0))))

    # Extract domain from publisher
    authors_df = authors_df.withColumn("domain", regexp_extract(col("publisher"), r"([a-zA-Z]+)$", 1))

    # Map domains to countries
    authors_df = authors_df.join(mapping_df, "domain", "left").filter(col("country").isNotNull())

    # Join authors with gender data
    matched_df = authors_df.join(
        gender_df,
        authors_df["first_name"] == gender_df["name"],
        "inner"
    ).select(
        authors_df["country"],  # Include the country column
        gender_df["gender"]  # Include the gender column
    )

    # Aggregate by gender and country
    gender_country_counts = matched_df.groupBy("country", "gender").agg(count("*").alias("count"))
    return gender_country_counts

# Process data for each year
years = range(2016, 2025)
combined_gender_country_counts = None

for year in years:
    print(f"Processing year: {year}")
    gender_country_counts = process_year_with_country(year)
    if combined_gender_country_counts is None:
        combined_gender_country_counts = gender_country_counts
    else:
        combined_gender_country_counts = combined_gender_country_counts.union(gender_country_counts)

# Normalize gender column to ensure consistency
combined_gender_country_counts = combined_gender_country_counts.withColumn(
    "gender",
    when(col("gender") == "M", "male")
    .when(col("gender") == "F", "female")
    .otherwise(col("gender"))
)

# Debug: Check unique values in the gender column
print("Unique gender values after normalization:")
combined_gender_country_counts.select("gender").distinct().show()

# Pivot to have separate columns for male and female counts
pivot_df = combined_gender_country_counts.groupBy("country").pivot("gender").sum("count")

# Fill null values with 0 (in case there are no male or female entries for a country)
pivot_df = pivot_df.fillna(0)

# Add a new column for the male-to-female ratio
pivot_df = pivot_df.withColumn(
    "male_female_ratio",
    when(col("female") == 0, lit(None))  # Avoid division by zero
    .otherwise(round(col("male") / col("female"), 2))
)

# Order by country and show the results
pivot_df = pivot_df.orderBy("country")  # Order by country alphabetically
pivot_df.show(100, truncate=False)

# Stop SparkSession
spark.stop()
