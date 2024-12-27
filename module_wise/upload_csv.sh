#!/bin/bash

# Directories
DATA_ENV="/root/workspace/" 
DATA_PATH="/root/workspace/module_wise"
RAW_DIR="${DATA_PATH}/raw_csv"
CLEANED_DIR="${DATA_PATH}/cleaned"
SCHEMA_DIR="${DATA_PATH}/schemas"
LOG_FILE="${DATA_PATH}/upload_log.txt"
DATASET="bakingo-a1694:analytics_writedb"

# Ensure directories exist
mkdir -p "$RAW_DIR" "$CLEANED_DIR"

# Start fresh log
echo "Log started at $(date)" > "$LOG_FILE"

# Process all CSV files in the raw directory
for raw_csv in "${RAW_DIR}"/*.csv; do
    # Extract base filename without extension
    base_name=$(basename "$raw_csv" .csv)
    
    # Map schema and table name
    schema_file="${SCHEMA_DIR}/schema_${base_name}.json"
    cleaned_csv="${CLEANED_DIR}/cleaned_${base_name}.csv"
    table_name="$base_name"

    echo "Processing $raw_csv..." | tee -a "$LOG_FILE"

    # Validate schema file exists
    if [ ! -f "$schema_file" ]; then
        echo "Error: Schema file $schema_file not found for $raw_csv." | tee -a "$LOG_FILE"
        continue
    fi

    # Run cleaning script

    if [ -f "${DATA_ENV}/env/bin/activate" ]; then
       
       source "${DATA_ENV}/env/bin/activate"
       echo "Environment activated successfully....."

    else
        echo "Error: Virtual environment not found at ${DATA_ENV}/env/bin/activate"
        exit 1
    fi

    python3 process_clean.py "$schema_file" "$raw_csv" "$cleaned_csv"
    if [ $? -ne 0 ]; then
        echo "Error cleaning $raw_csv. Check the log for details." | tee -a "$LOG_FILE"
        continue
    fi

    # Upload to BigQuery
    /bin/bash upload_bq.sh "$cleaned_csv" "$schema_file" "$table_name" "$DATASET"
    if [ $? -ne 0 ]; then
        echo "Error uploading $cleaned_csv to BigQuery." | tee -a "$LOG_FILE"
        continue
    fi

    echo "Successfully processed and uploaded $raw_csv." | tee -a "$LOG_FILE"
done

echo "Process completed at $(date)" | tee -a "$LOG_FILE"
