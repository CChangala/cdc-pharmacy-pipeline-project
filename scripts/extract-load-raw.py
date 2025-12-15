import pandas as pd
import os
import requests
from google.cloud import bigquery
from io import BytesIO

DATA_URL = ""
RAW_FILE_PATH =""
TABLE_ID = ""
DATASET_ID = ""
PROJECT_ID = ""
CREDENTDIALS_PATH = ""

"""MAP OF CONTENTS: 
1. Extract data from a URL
2. Load data into a raw file
3. Load data into BigQuery table
"""
def load_to_bigquery(df):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTDIALS_PATH
    client = bigquery.Client(project = PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    job_config = bigquery.LoadJobConfig(auto_detect =True,
                                        write_disposition="WRITE_TRUNCATE",
                                        null_marker = "")
    print(f"Loading data into BigQuery table {TABLE_ID}...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete.
    print("Data loaded successfully into BigQuery.")
    print(f"Successfully loaded {job.output_rows} rows to BigQuery.")


def extract_and_load_data():
    print("Starting data extraction from {DATA_URL}...")
    os.makedirs("data", exist_ok=True)

    response = requests.get(DATA_URL,stream=True, timeout=300)
    response.raise_for_status()

    with open(RAW_FILE_PATH, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Successfully downloaded file to {RAW_FILE_PATH}")

    df = pd.read_csv(RAW_FILE_PATH, low_memory=False)
    
    # Load into BigQuery
    load_to_bigquery(df)

if __name__ == "__main__":
    os.makedirs("secrets", exist_ok=True)
    if PROJECT_ID == "YOUR_GCP_PROJECT_ID":
        print("!!! ERROR: Please replace 'YOUR_GCP_PROJECT_ID' in the config. !!!")
    else:
        extract_and_load_data()