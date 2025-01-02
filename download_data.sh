# Variables
YEAR=2016

DATASET_URL="https://huggingface.co/api/datasets/stanford-oval/ccnews/parquet/$YEAR/train"
HDFS_DESTINATION="/user/s2551055/NewsData/$YEAR"

hdfs dfs -mkdir -p "$HDFS_DESTINATION"

echo "Downloading dataset metadata..."
curl -X GET "$DATASET_URL" -o ccnews_urls.json
PARQUET_URLS=$(grep -o '"https[^"]*.parquet"' ccnews_urls.json | sed 's/"//g')

echo "Extracting files..."
for PARQUET_URL in $PARQUET_URLS; do
    echo "Downloading $PARQUET_URL..."
    curl -X GET "$PARQUET_URL" -o "$PARQUET_URL.parquet"
    echo "Uploading $PARQUET_URL to hdfs dfs..."
    hdfs dfs -put "$PARQUET_URL.parquet" "$HDFS_DESTINATION"
done 


echo "All files downloaded and uploaded successfully!"
