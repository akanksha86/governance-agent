import os
import time
import json
import requests
import google.auth
import google.auth.transport.requests
from google.cloud import dataplex_v1
from google.cloud import bigquery
from google.cloud import storage

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
LOCATION = "europe-west1"
EXPORT_DATASET_ID = "governance_export"
EXPORT_TABLE_ID = "metadata_export"
GCS_BUCKET_NAME = f"{PROJECT_ID}-dataplex-export"

def get_access_token():
    credentials, project = google.auth.default()
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)
    return credentials.token

def create_gcs_bucket():
    client = storage.Client(project=PROJECT_ID)
    bucket_name = GCS_BUCKET_NAME
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket {bucket_name} already exists.")
    except Exception:
        bucket = client.create_bucket(bucket_name, location=LOCATION)
        print(f"Created bucket {bucket_name} in {LOCATION}")
    return bucket_name

def run_metadata_export():
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Generate a unique job ID
    job_id = f"export-{int(time.time())}"
    
    data = {
        "type": "EXPORT",
        "exportSpec": {
            "outputPath": f"gs://{GCS_BUCKET_NAME}/",
            "scope": {
                "projects": [
                    f"projects/{PROJECT_ID}"
                ]
            }
        }
    }
    
    print(f"Starting metadata export job {job_id} for project {PROJECT_ID}...")
    url = f"https://dataplex.googleapis.com/v1/projects/{PROJECT_ID}/locations/{LOCATION}/metadataJobs"
    response = requests.post(
        f"{url}?metadataJobId={job_id}",
        headers=headers,
        json=data
    )
    
    response.raise_for_status()
    job_info = response.json()
    print(f"Job started: {job_info.get('name')}")
    return job_info

def wait_for_job(job_info):
    if not job_info:
        return
    
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    
    operation_name = job_info.get("name")
    if not operation_name:
        return
    
    print(f"Waiting for job {operation_name} to complete...")
    url = f"https://dataplex.googleapis.com/v1/{operation_name}"
    
    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        status = response.json()
        
        done = status.get("done", False)
        metadata = status.get("metadata", {})
        
        # When an operation is done, the actual MetadataJob is in the 'response' field
        state = None
        if done:
            job_response = status.get("response", {})
            job_status = job_response.get("status", {})
            state = job_status.get("state")
        else:
            state = metadata.get("state") # During operation, state might be in metadata
        
        print(f"Operation done: {done}, Job state: {state}")
        
        if done:
            if state == "SUCCEEDED":
                print(f"Job completed successfully.")
            elif "error" in status:
                print(f"Job failed with operation error: {status['error']}")
            else:
                print(f"Job finished with state: {state}")
            break

        print("Job still running, waiting 10 seconds...")
        time.sleep(10)
    
    return status

def create_bigquery_external_table():
    client = bigquery.Client(project=PROJECT_ID)
    
    # Create dataset if not exists
    dataset_id = f"{PROJECT_ID}.{EXPORT_DATASET_ID}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = LOCATION
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")
    
    table_id = f"{dataset_id}.{EXPORT_TABLE_ID}"
    
    # Define schema for Dataplex metadata export
    schema = [
        bigquery.SchemaField("entry", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("entryType", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("createTime", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("updateTime", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("aspects", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("parentEntry", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fullyQualifiedName", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("entrySource", "RECORD", mode="NULLABLE", fields=[
                bigquery.SchemaField("resource", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("system", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("platform", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("displayName", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("labels", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("ancestors", "RECORD", mode="REPEATED", fields=[
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
                ]),
                bigquery.SchemaField("createTime", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("updateTime", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
            ]),
        ]),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
    external_config.source_uris = [f"gs://{GCS_BUCKET_NAME}/*"]
    
    # Enable Hive Partitioning to handle the nested directory structure
    hive_partitioning = bigquery.HivePartitioningOptions()
    hive_partitioning.mode = "AUTO"
    hive_partitioning.source_uri_prefix = f"gs://{GCS_BUCKET_NAME}/"
    external_config.hive_partitioning = hive_partitioning
    
    table.external_data_configuration = external_config
    
    try:
        client.delete_table(table_id)
        print(f"Deleted old table {table_id}")
    except Exception:
        pass
        
    client.create_table(table)
    print(f"Created external table {table_id} pointing to gs://{GCS_BUCKET_NAME}/*")

def create_native_table():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{EXPORT_DATASET_ID}"
    external_table_id = f"{dataset_id}.{EXPORT_TABLE_ID}"
    native_table_id = f"{dataset_id}.{EXPORT_TABLE_ID}_native"

    sql = f"""
        CREATE OR REPLACE TABLE `{native_table_id}` AS
        SELECT 
            entry.*
        FROM `{external_table_id}`
    """
    print(f"Creating native table {native_table_id} from {external_table_id}...")
    query_job = client.query(sql)
    query_job.result()  # Wait for job to complete
    print(f"Native table {native_table_id} created.")

if __name__ == "__main__":
    if not PROJECT_ID:
        print("Please set GOOGLE_CLOUD_PROJECT environment variable.")
        exit(1)
    
    print("Starting Dataplex Metadata Export process...")
    
    # 1. Create GCS bucket
    create_gcs_bucket()
    
    # 2. Run Metadata Export Job
    op_status = run_metadata_export()
    if op_status:
        wait_for_job(op_status)
    
    # Check if we actually have files in GCS before proceeding with BigQuery
    # This prevents the 'BadRequest' error if no metadata was harvested yet.
    storage_client = storage.Client(project=PROJECT_ID)
    blobs = list(storage_client.list_blobs(GCS_BUCKET_NAME, max_results=1))
    
    if not blobs:
        print("\nWARNING: No metadata files found in the export bucket.")
        print("This usually happens if Dataplex hasn't finished harvesting your BigQuery tables yet.")
        print("Wait a few minutes and try running this script again.")
    else:
        # 3. Create BigQuery External Table
        create_bigquery_external_table()
        
        # 4. Create Native BigQuery Table
        create_native_table()

