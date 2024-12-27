import pandas as pd
import numpy as np
import json
import chardet
import os
import sys

class DataCleaner:
    def __init__(self, schema_file):
        self.schema_file = schema_file
        self.df = None
        self.schema = None
        self.type_map = {
            "INTEGER": np.int64,
            "FLOAT": np.float64,
            "STRING": str,
            "TIMESTAMP": "datetime64[ns]",
            "DATE": "datetime64[ns]"
        }

    def load_schema(self):
        try:
            with open(self.schema_file, 'r') as file:
                self.schema = json.load(file)
        except Exception as e:
            raise Exception(f"Error loading schema file {self.schema_file}: {e}")

    def detect_encoding(self, csv_file):
        try:
            with open(csv_file, 'rb') as file:
                result = chardet.detect(file.read(10000))  # Read the first 10,000 bytes
                return result['encoding']
        except Exception as e:
            raise Exception(f"Error detecting encoding for file {csv_file}: {e}")

    def load_csv(self, csv_file):
        encoding = self.detect_encoding(csv_file)
        if not encoding:
            encoding = 'utf-8'

        print(f"Detected encoding for {csv_file}: {encoding}")
        try:
            
            with open(csv_file, 'rb') as f:
                clean_data = f.read().decode(encoding, errors='replace')

            from io import StringIO
            cleaned_csv = StringIO(clean_data)
            self.df = pd.read_csv(cleaned_csv)

        except Exception as e:
            raise Exception(f"Error loading CSV file {csv_file}: {e}")


    def clean_data(self):
        if self.df is None or self.schema is None:
            raise ValueError("CSV file or schema not loaded.")

        for column in self.schema:
            column_name = column['name']
            column_type = column['type']

            if column_name in self.df.columns:
                if column_type in self.type_map:
                    if column_type in ["INTEGER", "FLOAT"]:
                        self.df[column_name] = self.df[column_name].replace({None: 0})
                        self.df[column_name] = pd.to_numeric(self.df[column_name], errors='coerce').fillna(0).astype(self.type_map[column_type])
                    elif column_type in ["TIMESTAMP", "DATE"]:
                        self.df[column_name] = pd.to_datetime(self.df[column_name], errors='coerce')
                    else:
                        self.df[column_name] = self.df[column_name].astype(self.type_map[column_type])

        for column in self.schema:
            column_name = column['name']
            if column_name not in self.df.columns:
                if column['type'] == 'STRING':
                    self.df[column_name] = ""
                elif column['type'] == 'INTEGER':
                    self.df[column_name] = 0
                elif column['type'] == 'FLOAT':
                    self.df[column_name] = 0.0
                elif column['type'] in ['TIMESTAMP', 'DATE']:
                    self.df[column_name] = pd.NaT

    def save_cleaned_csv(self, output_csv):
        if self.df is None:
            raise ValueError("No data to save. Please clean data first.")

        try:
            self.df.to_csv(output_csv, index=False, sep='\t')
            print(f"Cleaned data has been saved to {output_csv}")
        except Exception as e:
            raise Exception(f"Error saving cleaned CSV to {output_csv}: {e}")


def clean_csv_using_schema(schema_file, raw_csv_file, cleaned_csv_file):
    cleaner = DataCleaner(schema_file)
    cleaner.load_schema()
    cleaner.load_csv(raw_csv_file)
    cleaner.clean_data()
    cleaner.save_cleaned_csv(cleaned_csv_file)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 process_clean.py <schema_file> <raw_csv_file> <cleaned_csv_file>")
        sys.exit(1)

    schema_file = sys.argv[1]
    raw_csv_file = sys.argv[2]
    cleaned_csv_file = sys.argv[3]

    try:
        clean_csv_using_schema(schema_file, raw_csv_file, cleaned_csv_file)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
