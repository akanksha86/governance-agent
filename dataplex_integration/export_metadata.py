import os
from google.cloud import dataplex_v1
from google.cloud import bigquery
import pandas as pd

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
LOCATION = "europe-west1"
ENTRY_GROUP_ID = "retail-governance-group"
EXPORT_DATASET_ID = "dataplex_metadata_export"

def list_entries_and_aspects():
    client = dataplex_v1.CatalogServiceClient()
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP_ID}"
    
    entries = client.list_entries(parent=parent)
    
    data = []
    for entry in entries:
        for aspect_type, aspect_data in entry.aspects.items():
            data.append({
                "entry_name": entry.name,
                "aspect_type": aspect_type,
                "owner": aspect_data.data.get("owner"),
                "contains_pii": aspect_data.data.get("contains_pii")
            })
    return pd.DataFrame(data)

def create_export_dataset():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{EXPORT_DATASET_ID}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU" # Keep in same region if possible, or match BQ
    try:
        client.get_dataset(dataset_id)
    except Exception:
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")

def load_to_bigquery(df):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{EXPORT_DATASET_ID}.metadata_export"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows to {table_id}")

if __name__ == "__main__":
    if not PROJECT_ID:
        print("Please set GOOGLE_CLOUD_PROJECT environment variable.")
        exit(1)
        
    print("Listing entries and aspects...")
    # In a real run, this would fetch from Dataplex
    # For now, we simulate the data if Dataplex is not accessible or empty
    try:
        df = list_entries_and_aspects()
    except Exception as e:
        print(f"Could not fetch from Dataplex: {e}. Using simulated data for demo.")
        df = pd.DataFrame([
            {"entry_name": "bq-customers", "aspect_type": "governance", "owner": "Data Privacy Team", "contains_pii": True},
            {"entry_name": "bq-products", "aspect_type": "governance", "owner": "Merchandising Team", "contains_pii": False},
        ])
    
    if len(df) > 0:
        create_export_dataset()
        load_to_bigquery(df)
    else:
        print("No metadata found to export.")
