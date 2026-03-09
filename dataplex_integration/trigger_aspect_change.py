import os
from google.cloud import dataplex_v1
from google.protobuf import struct_pb2

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
LOCATION = "europe-west1"
DATASET_ID = "retail_synthetic_data"
TABLE_ID = "customers"
ASPECT_TYPE_ID = "data-governance-aspect"

def update_aspect():
    client = dataplex_v1.CatalogServiceClient()
    
    # Construct entry name
    entry_id = f"bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
    entry_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/@bigquery/entries/{entry_id}"
    
    aspect_type_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/aspectTypes/{ASPECT_TYPE_ID}"
    
    # Parse aspect type name to get the correct key format for the aspects map
    # The key is project.location.aspect_type_id
    aspect_key = f"{PROJECT_ID}.{LOCATION}.{ASPECT_TYPE_ID}"
    
    # Update some data in the aspect
    aspect_data = struct_pb2.Struct()
    aspect_data.update({
        "owner": "Data Security Team (Updated)",
        "contains_pii": True,
        "layer": "raw",
        "environment": "Prod"
    })
    
    entry = dataplex_v1.Entry()
    entry.name = entry_name
    entry.aspects = {
        aspect_key: dataplex_v1.Aspect(
            aspect_type=aspect_type_name,
            data=aspect_data
        )
    }
    
    # Update only the aspects field
    update_mask = {"paths": ["aspects"]}
    
    try:
        client.update_entry(entry=entry, update_mask=update_mask)
        print(f"Updated aspect '{ASPECT_TYPE_ID}' for table {TABLE_ID}")
        print("Dataplex should trigger an 'UPDATED' notification immediately for this aspect change.")
    except Exception as e:
        print(f"Error updating aspect: {e}")

if __name__ == "__main__":
    update_aspect()
