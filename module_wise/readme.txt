
Directory Layout
bash
Copy code
/root/workspace/module_wise/
├── raw_csv/                  # Raw CSV files
│   ├── orders_order.csv
│   ├── bakingo_swiggy_rating_feedback.csv
│   └── ...
├── cleaned/                  # Cleaned CSV files
│   ├── cleaned_orders_order.csv
│   ├── cleaned_bakingo_swiggy_rating_feedback.csv
│   └── ...
├── schemas/                  # Schema files
│   ├── schema_orders_order.json
│   ├── schema_bakingo_swiggy_rating_feedback.json
│   └── ...
├── upload_csvs.sh            # Main shell script
├── upload_bq.sh              # BigQuery upload script
├── process_clean.py          # Python cleaning script
└── upload_log.txt            # Log file for errors/successes



How It Works

Create env as per requirment.txt using virtual env

Place raw CSVs in /raw_csv/.

Run upload_csvs.sh:

bash

Copy code

bash upload_csvs.sh

The script:

Cleans each CSV using the schema in /schemas/.

Saves cleaned files in /cleaned/.

Uploads cleaned files to BigQuery.

Logs success/errors in upload_log.txt.

This setup is structured, automated, and easily extensible!

you can also use Docker also if required

#--------------DOCKER---------------------------#

# Base Image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir virtualenv \
    && virtualenv /app/env \
    && /app/env/bin/pip install -r requirements.txt

# Activate virtual environment in shell
RUN echo "source /app/env/bin/activate" >> ~/.bashrc

# Make scripts executable
RUN chmod +x upload_csvs.sh

# Define default command
CMD ["bash"]
