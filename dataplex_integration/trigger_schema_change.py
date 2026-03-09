import os
import time
from google.cloud import bigquery

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
DATASET_ID = "retail_synthetic_data"
TABLE_ID = "customers"
# Dynamic column name to avoid 'already exists' error on repeated runs
NEW_COLUMN_NAME = f"evolution_check_{int(time.time())}"

def add_column_to_table():
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    table = client.get_table(table_ref)
    
    # Add a new column to trigger a schema change in Dataplex
    new_schema = list(table.schema)
    new_schema.append(bigquery.SchemaField(NEW_COLUMN_NAME, "STRING", mode="NULLABLE"))
    
    table.schema = new_schema
    client.update_table(table, ["schema"])
    
    print(f"Added column '{NEW_COLUMN_NAME}' to {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print("Dataplex should detect this change during the next harvest/scan and publish a notification.")

if __name__ == "__main__":
    add_column_to_table()
