from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, count, lower, trim

# Initializing SparkSession
spark = SparkSession.builder.appName("Author Gender Matching").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# Load gender dataset from HDFS
file_path = "hdfs:///user/s2857634/wgnd_2_0_name-gender-langcode.csv"
gender_df = spark.read.csv(file_path, header=True, inferSchema=True)

print("Gender Dataset Schema:")
gender_df.printSchema()

gender_df = gender_df.dropDuplicates(["name"])
gender_df = gender_df.withColumn("name", lower(trim(col("name"))))

# Process a single year of data
def process_year(year):
    year_path = f"hdfs:///user/s2551055/NewsData_full/{year}/*.parquet"
    cc_news_df = spark.read.parquet(year_path)
    
    # Filter out null authors
    authors_df = cc_news_df.select("author")
    authors_df = authors_df.filter(authors_df["author"].isNotNull())
    # print("Filtered authors with no nulls:")
    # authors_df.show()

    
    # Extract first names
    authors_df = authors_df.withColumn("first_name", split(col("author"), " ").getItem(0))
    print("Number of authors before join:", authors_df.count())

    # Make names lowercase and alphabetically order them 
    authors_df = authors_df.withColumn("first_name", lower(trim(col("first_name"))))
    authors_df = authors_df.orderBy("first_name")

    # Match authors with names in the gender dataset
    matched_df = authors_df.join(
        gender_df,
        authors_df["first_name"] == gender_df["name"],
        "inner"
    ).select(
        authors_df["author"],  
        authors_df["first_name"],  
        gender_df["gender"]  
    )

    # Show missing names, to check most real names are covered
    missing_names = authors_df.select("first_name").distinct().join(
    gender_df.select("name").distinct(),
    authors_df["first_name"] == gender_df["name"],
    "left_anti"
)
    
    matched_df = matched_df.orderBy("author")
    print("Joined authors:")
    matched_df.show()
    print("Missing Names:")
    missing_names.show()
    print("Number of authors after join:", matched_df.count())

    
    # count the number of male and female authors
    gender_counts = matched_df.groupBy("gender").agg(count("*").alias("count"))
    print(f"Gender counts for year {year}:")
    gender_counts.show()

    return matched_df, gender_counts

# dictionary to store results for each year
yearly_results = {}

# Range of years to process
years = range(2016, 2025)

for year in years:
    print(f"Processing year: {year}")
    yearly_matched_authors, yearly_gender_counts = process_year(year)
    
    # Store the matched authors dataframe and gender counts for each year
    yearly_results[year] = {
        "matched_authors": yearly_matched_authors,
        "gender_counts": yearly_gender_counts
    }



combined_gender_counts = {}

for year, result in yearly_results.items():
    gender_counts = result["gender_counts"].collect()
    
    # Update the combined counts dictionary
    for row in gender_counts:
        gender = row["gender"]
        count = row["count"] 
        if gender in combined_gender_counts:
            combined_gender_counts[gender] += count
        else:
            combined_gender_counts[gender] = count

# Print the final aggregated counts
print("Final Gender Counts Across All Years:")
for gender, count in combined_gender_counts.items():
    print(f"{gender}: {count}")


# Show combined gender counts
if combined_gender_counts:
    print("Final Gender Counts Across All Years:")
    combined_gender_counts.show()

# Stop the SparkSession
spark.stop()
