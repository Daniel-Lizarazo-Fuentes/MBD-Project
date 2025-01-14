# Variables
BASE_URL="https://huggingface.co/api/datasets/stanford-oval/ccnews"
DOWNLOAD_URL_BASE="https://huggingface.co/datasets/stanford-oval/ccnews/resolve/main"
HDFS_BASE_DESTINATION="/user/s2551055/NewsData_full"

echo "Fetching dataset file list..."
curl -X GET $BASE_URL -o dataset_metadata.json

PARQUET_FILES=$(grep -o '"rfilename": *"[^"]*.parquet"' dataset_metadata.json | sed -E 's/"rfilename": *"([^"]*)"/\1/')

for PARQUET_FILE in $PARQUET_FILES; do
    YEAR=$(echo "$PARQUET_FILE" | grep -o '^[0-9]\{4\}')
    HDFS_DESTINATION="$HDFS_BASE_DESTINATION/$YEAR"

    echo "Creating HDFS directory: $HDFS_DESTINATION..."
    hdfs dfs -mkdir -p "$HDFS_DESTINATION"

    DOWNLOAD_URL="$DOWNLOAD_URL_BASE/$PARQUET_FILE"
    FILE_NAME=$(basename "$PARQUET_FILE")

    echo "Downloading $DOWNLOAD_URL to NFS as $FILE_NAME..."
    curl -L "$DOWNLOAD_URL" -o "$FILE_NAME"

    echo "Uploading $FILE_NAME to HDFS..."
    hdfs dfs -put "$FILE_NAME" "$HDFS_DESTINATION"

    echo "Removing file $FILE_NAME from NFS..."
    rm "$FILE_NAME"
done

rm dataset_metadata.json

echo "Upload and cleanup complete." 