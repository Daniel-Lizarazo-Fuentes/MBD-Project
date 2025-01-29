from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, regexp_extract, row_number
from pyspark.sql.window import Window

# -------------------------------------- Setup -----------------------------------------
sc = SparkContext(appName="UniquePublishersByLanguage")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# -------------------------------------- Analyze Representation of Countries Over the Years -----------------------------------------

# Define country mapping
country_mapping = {
    "it": "Italy",
    "de": "Germany",
    "fr": "France",
    "us": "United States",
    "uk": "United Kingdom",
    "ca": "Canada",
    "au": "Australia",
    "in": "India",
    "cn": "China",
    "jp": "Japan",
    "ru": "Russia",
    "br": "Brazil",
    "es": "Spain",
    "mx": "Mexico",
    "za": "South Africa",
    "kr": "South Korea",
    "nl": "Netherlands",
    "se": "Sweden",
    "no": "Norway",
    "fi": "Finland"
    # Add more mappings as needed
}

# Create a mapping DataFrame
mapping_df = spark.createDataFrame(country_mapping.items(), ["domain", "country"])

combined_df = None

years = ["2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]

for year_folder in years:
    path = f"/user/s2551055/NewsData_full/{year_folder}/"
    df = spark.read.parquet(path)

    # Extract year from the folder name and add it as a column
    df = df.withColumn("year", col("published_date").substr(1, 4))

    # Extract the domain from the publisher name
    df = df.withColumn("domain", regexp_extract(col("publisher"), r"([a-zA-Z]+)$", 1))

    # Select relevant columns
    df = df.select("year", "domain", "publisher")

    # Combine data
    if combined_df is None:
        combined_df = df
    else:
        combined_df = combined_df.union(df)

# Map domains to countries
combined_df = combined_df.join(mapping_df, combined_df["domain"] == mapping_df["domain"], "left").drop("domain")

# Filter out null countries
combined_df = combined_df.filter(col("country").isNotNull())

# Analyze how the representation of countries has changed over the years
window_spec = Window.partitionBy("year").orderBy(col("count").desc())

country_representation = (
    combined_df.groupBy("year", "country")
    .count()
    .filter(col("count") > 0)  # Ensure valid counts
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") <= 5)  # Keep only the top 5 countries per year
    .drop("rank")  # Remove the rank column
    .orderBy(col("year").desc(), col("count").desc())  # Sort by year (descending) and count (descending)
)

# Show the results
country_representation.show(100)

# Stop the Spark session
spark.stop()


