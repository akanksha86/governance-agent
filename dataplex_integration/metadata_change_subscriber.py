import os
import json
import requests
from google.cloud import pubsub_v1
from google.cloud import bigquery
import google.auth
import google.auth.transport.requests

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
SUBSCRIPTION_ID = "dataplex-metadata-changes-sub"
BQ_DATASET = "governance_export"
BQ_TABLE = "metadata_changes"

def get_access_token():
    credentials, project = google.auth.default()
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)
    return credentials.token

def fetch_dataplex_entry(entry_name):
    """Fetch the current state of an entry from Dataplex."""
    access_token = get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://dataplex.googleapis.com/v1/{entry_name}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            print(f"Entry {entry_name} not found (might have been deleted).")
            return None
        else:
            print(f"Error fetching entry {entry_name}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Exception fetching entry: {e}")
        return None

def log_to_bigquery(event_data):
    """Insert the event record into BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    
    # Ensure all fields are strings or correct types for BQ insert
    # metadata_snapshot should be a string or JSON type
    # changed_aspects should be an array of strings
    
    errors = client.insert_rows_json(table_id, [event_data])
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print(f"Logged {event_data.get('change_type')} event for {event_data.get('entry_fqn')} to BigQuery.")

def callback(message):
    """Pub/Sub message callback."""
    print(f"\n--- New Notification Received ---")
    
    try:
        attributes = dict(message.attributes)
        print(f"Attributes: {attributes}")
        
        # The data payload contains aspect change details
        data_json = message.data.decode("utf-8")
        print(f"Data Payload: {data_json}")
        data = json.loads(data_json) if data_json else {}
        
        # Dataplex uses various names for entry info across attributes and payload
        entry_name = (attributes.get("entry_name") or 
                     data.get("entryName") or 
                     data.get("entry_name"))
        
        change_type = (attributes.get("entry_change_type") or 
                      data.get("entry_change_type") or 
                      "UNKNOWN")
        
        timestamp = (attributes.get("timestamp") or 
                    data.get("timestamp"))
        
        # Try multiple variations for FQN
        entry_fqn = (attributes.get("entry_fqn") or 
                    attributes.get("entryFqn") or
                    data.get("full_qualified_name") or 
                    data.get("fullyQualifiedName") or 
                    data.get("entry_fqn"))
        
        entry_type = (attributes.get("entry_type") or 
                     data.get("entry_type") or 
                     data.get("entryType"))
        
        print(f"Change Type: {change_type}")
        print(f"Entry Name (internal): {entry_name}")
        print(f"Entry FQN: {entry_fqn}")
        
        changed_aspects = []
        if data:
            changed_aspects.extend(data.get("createdAspects", []))
            changed_aspects.extend(data.get("updatedAspects", []))
            changed_aspects.extend(data.get("deletedAspects", []))
            
        # Fetch current state of the entry (if not deleted and entry_name is present)
        entry_snapshot = None
        if change_type != "DELETED" and entry_name:
            entry_snapshot = fetch_dataplex_entry(entry_name)
            if entry_snapshot and not entry_fqn:
                # Last resort: try to get FQN from snapshot
                entry_fqn = entry_snapshot.get("fullyQualifiedName")
        
        # Human readable summary
        summary_name = entry_fqn or entry_name or "Unknown Entry"
        summary = f"Metadata {change_type} for {summary_name}"
        if changed_aspects:
            summary += f" (Aspects: {', '.join([a.split('/')[-1] for a in changed_aspects])})"
        
        # Prepare for BQ
        event_record = {
            "event_timestamp": timestamp,
            "entry_name": entry_name,
            "entry_fqn": entry_fqn,
            "change_type": change_type,
            "entry_type": entry_type,
            "changed_aspects": changed_aspects,
            "metadata_snapshot": json.dumps(entry_snapshot) if entry_snapshot else None,
            "summary": summary
        }
        
        log_to_bigquery(event_record)
        message.ack()
        
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

def listen_for_changes():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for metadata changes on {subscription_path}...\n")
    
    with subscriber:
        try:
            streaming_pull_future.result()
        except Exception as e:
            print(f"Listening for messages stopped: {e}")
            streaming_pull_future.cancel()

if __name__ == "__main__":
    listen_for_changes()
