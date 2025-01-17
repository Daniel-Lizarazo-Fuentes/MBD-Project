# Variables
BASE_URL="https://huggingface.co/api/datasets/stanford-oval/ccnews"
DOWNLOAD_URL_BASE="https://huggingface.co/datasets/stanford-oval/ccnews/resolve/main"
HDFS_BASE_DESTINATION="/user/s2551055/NewsData_full"

IMPORT_SPECIFIC_FILES=true  
SPECIFIC_FILES=("2019_0037.parquet" "2019_0041.parquet" "2019_0049.parquet" "2019_0045.parquet" "2020_0026.parquet" "2020_0043.parquet" "2021_0038.parquet" "2021_0079.parquet" "2019_0049.parquet" "2023_0010.parquet")

echo "Fetching dataset file list..."
curl -X GET $BASE_URL -o dataset_metadata.json

PARQUET_FILES=$(grep -o '"rfilename": *"[^"]*.parquet"' dataset_metadata.json | sed -E 's/"rfilename": *"([^"]*)"/\1/')

# "2016_0000.parquet" for example
START_FILE="2016_0000.parquet"
STARTED=false

for PARQUET_FILE in $PARQUET_FILES; do
    if $IMPORT_SPECIFIC_FILES; then
        if [[ ! " ${SPECIFIC_FILES[@]} " =~ " ${PARQUET_FILE} " ]]; then
            echo "Skipping $PARQUET_FILE (not in the specific file list)..."
            continue
        fi
    else
        if [ "$STARTED" == false ] && [ "$PARQUET_FILE" != "$START_FILE" ]; then
            echo "Skipping $PARQUET_FILE..."
            continue
        fi
        STARTED=true
    fi

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