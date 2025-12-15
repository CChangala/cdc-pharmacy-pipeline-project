import pandas as pd
import os
import requests
from google.cloud import bigquery
from io import BytesIO


RAW_FILE_PATH = "data/sdud-2025-updated-dec2025.csv" # <-- NEW FILE NAME
TABLE_ID = "cms_utilization_raw"
DATASET_ID = "raw_data"
PROJECT_ID = "pharmacy-pipeline-2025"
CREDENTDIALS_PATH = "secrets/bigquery-editor-key.json"

"""MAP OF CONTENTS: 
1. Extract data from a URL
2. Load data into a raw file
3. Load data into BigQuery table
"""
def load_to_bigquery(df):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTDIALS_PATH
    client = bigquery.Client(project = PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    job_config = bigquery.LoadJobConfig(autodetect=True,
                                    write_disposition="WRITE_TRUNCATE")
    
    print(f"Loading data into BigQuery table {TABLE_ID}...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete.
    print("Data loaded successfully into BigQuery.")
    print(f"Successfully loaded {job.output_rows} rows to BigQuery.")


def extract_and_load_data():
    if not os.path.exists(RAW_FILE_PATH):
        print(f"!!! ERROR: Data file not found at {RAW_FILE_PATH}")
        print("Please ensure the downloaded CSV is named 'raw_sdud_data.csv' and is in the 'data/' folder.")
        return

    print(f"Starting data load from local file: {RAW_FILE_PATH}")

    # Read the CSV file into a DataFrame
    df = pd.read_csv(RAW_FILE_PATH, low_memory=False)
    
    # Load into BigQuery
    load_to_bigquery(df)

if __name__ == "__main__":
    os.makedirs("secrets", exist_ok=True)
    if PROJECT_ID == "YOUR_GCP_PROJECT_ID":
        print("!!! ERROR: Please replace 'YOUR_GCP_PROJECT_ID' in the config. !!!")
    else:
        extract_and_load_data()