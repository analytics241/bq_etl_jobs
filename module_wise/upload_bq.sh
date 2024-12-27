#!/bin/bash

# Arguments
CLEANED_CSV=$1
SCHEMA_FILE=$2
TABLE_NAME=$3
DATASET=$4

if [ $# -ne 4 ]; then
    echo "Usage: $0 <cleaned_csv> <schema_file> <table_name> <dataset>"
    exit 1
fi

# Upload data to BigQuery
bq load --source_format=CSV \
        --quote "" \
        --field_delimiter='\t' \
        --allow_jagged_rows \
        --ignore_unknown_values \
        --allow_quoted_newlines \
        --max_bad_records 1 \
        --null_marker="\N" \
        --null_marker="NULL" \
        --null_marker="\\N" \
        --null_marker=NULL \
        --skip_leading_rows=1 \
        --schema="$SCHEMA_FILE" \
        -E UTF-8 \
         "$DATASET.$TABLE_NAME" "$CLEANED_CSV"
if [ $? -eq 0 ]; then
    echo "Uploaded $CLEANED_CSV to $DATASET.$TABLE_NAME successfully."
else
    echo "Failed to upload $CLEANED_CSV to $DATASET.$TABLE_NAME."
    exit 1
fi
