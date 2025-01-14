# Variables
START_YEAR=2016
END_YEAR=2024

BASE_URL="https://huggingface.co/api/datasets/stanford-oval/ccnews/parquet"
HDFS_BASE_DESTINATION="/user/s2551055/NewsData/"

for YEAR in $(seq $START_YEAR $END_YEAR); do
    DATASET_URL="$BASE_URL/$YEAR/train"
    echo $DATASET_URL

    HDFS_DESTINATION="$HDFS_BASE_DESTINATION/$YEAR"

    echo "Creating HDFS directory: $HDFS_DESTINATION..."
    hdfs dfs -mkdir -p "$HDFS_DESTINATION"

    echo "Downloading dataset metadata..."
    curl -X GET "$DATASET_URL" -o ccnews_urls.json

    PARQUET_URLS=$(grep -o '"https[^"]*.parquet"' ccnews_urls.json | sed 's/"//g')

    echo "Fetching files..."
    for PARQUET_URL in $PARQUET_URLS; do
        FILE_NAME=$(basename "$PARQUET_URL")

        echo "Downloading $PARQUET_URL to NFS..."
        curl -L "$PARQUET_URL" -o "$FILE_NAME"

        echo "Uploading "$FILE_NAME" to hdfs dfs..."
        hdfs dfs -put ""$FILE_NAME"" "$HDFS_DESTINATION"

        echo "Removing file $FILE_NAME from NFS..."
        rm "$FILE_NAME"
    done

    echo "Completed processing for $YEAR."
done

rm ccnews_urls.json

echo "Upload and cleanup complete." 
