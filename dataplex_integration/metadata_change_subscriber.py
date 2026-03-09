import os
import json
import requests
from google.cloud import pubsub_v1
from google.cloud import bigquery
import google.auth
import google.auth.transport.requests
from google.cloud import logging as cloud_logging
from datetime import datetime, timedelta

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
SUBSCRIPTION_ID = os.environ.get("METADATA_SUBSCRIPTION_ID", "dataplex-metadata-changes-sub")
BQ_DATASET = os.environ.get("METADATA_BQ_DATASET", "governance_export")
BQ_TABLE = os.environ.get("METADATA_BQ_TABLE", "metadata_changes")

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
            print(f"Entry {entry_name} not found (might have been deleted).", flush=True)
            return None
        else:
            print(f"Error fetching entry {entry_name}: {response.status_code} - {response.text}", flush=True)
            return None
    except Exception as e:
        print(f"Exception fetching entry: {e}", flush=True)
        return None

def fetch_actor_from_audit_logs(resource_name, event_timestamp_str, entry_fqn=None):
    """Correlate metadata event with Cloud Audit Logs to find the principalEmail."""
    logging_client = cloud_logging.Client(project=PROJECT_ID)
    
    # event_timestamp_str is usually in RFC3339 format, e.g., '2026-03-09T14:23:04.507350Z'
    # Notifications can be delayed by several minutes, so we search backwards a bit more.
    try:
        # Parse timestamp safely
        event_dt = datetime.fromisoformat(event_timestamp_str.replace("Z", "+00:00"))
        # Looking back 10 minutes to account for notification latency
        start_time = (event_dt - timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%SZ')
        # Include a small buffer after the event just in case clocks are slightly off
        end_time = (event_dt + timedelta(seconds=15)).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        entry_id = resource_name.split("/")[-1]
        
        # Build filter with proper precedence and multiple resource hints
        # We search for the full resource name, the short ID, or the FQN
        resource_match = [f'protoPayload.resourceName:"{resource_name}"', f'protoPayload.resourceName:"{entry_id}"']
        if entry_fqn:
            resource_match.append(f'protoPayload.resourceName:"{entry_fqn}"')
            
        resource_filter = " OR ".join(resource_match)
        
        filter_str = (
            f'(resource.type="dataplex.googleapis.com" OR resource.type="bigquery_resource" OR resource.type="audited_resource") '
            f'AND timestamp >= "{start_time}" '
            f'AND timestamp <= "{end_time}" '
            f'AND ({resource_filter})'
        )
        
        print(f"Searching audit logs with filter: {filter_str}", flush=True)
        entries = logging_client.list_entries(filter_=filter_str, order_by=cloud_logging.DESCENDING, page_size=10)
        
        for entry in entries:
            auth_info = entry.payload.get("authenticationInfo", {})
            principal = auth_info.get("principalEmail")
            if principal:
                print(f"Found actor in audit logs: {principal}", flush=True)
                return principal
                
        return "system-harvest" # Default if not found (likely background harvest)
    except Exception as e:
        print(f"Error fetching actor from logs: {e}", flush=True)
        return "unknown"

def log_to_bigquery(event_data):
    """Insert the event record into BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    
    # Ensure all fields are strings or correct types for BQ insert
    # metadata_snapshot should be a string or JSON type
    # changed_aspects should be an array of strings
    
    errors = client.insert_rows_json(table_id, [event_data])
    if errors:
        print(f"Encountered errors while inserting rows: {errors}", flush=True)
    else:
        print(f"Logged {event_data.get('change_type')} event for {event_data.get('entry_fqn')} to BigQuery.", flush=True)

def callback(message):
    """Pub/Sub message callback."""
    print(f"\n--- New Notification Received ---", flush=True)
    
    try:
        attributes = dict(message.attributes)
        print(f"Attributes: {attributes}", flush=True)
        
        # The data payload contains aspect change details
        data_json = message.data.decode("utf-8")
        print(f"Data Payload: {data_json}", flush=True)
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
        
        print(f"Change Type: {change_type}", flush=True)
        print(f"Entry Name (internal): {entry_name}", flush=True)
        print(f"Entry FQN: {entry_fqn}", flush=True)
        
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
        
        # Fetch actor from Audit Logs
        user_email = fetch_actor_from_audit_logs(entry_name, timestamp, entry_fqn)
        
        # Prepare for BQ
        event_record = {
            "event_timestamp": timestamp,
            "entry_name": entry_name,
            "entry_fqn": entry_fqn,
            "change_type": change_type,
            "entry_type": entry_type,
            "changed_aspects": changed_aspects,
            "metadata_snapshot": json.dumps(entry_snapshot) if entry_snapshot else None,
            "user_email": user_email,
            "summary": summary
        }
        
        log_to_bigquery(event_record)
        message.ack()
        
    except Exception as e:
        print(f"Error processing message: {e}", flush=True)
        message.nack()

def listen_for_changes():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for metadata changes on {subscription_path}...\n", flush=True)
    
    with subscriber:
        try:
            streaming_pull_future.result()
        except Exception as e:
            print(f"Listening for messages stopped: {e}")
            streaming_pull_future.cancel()

if __name__ == "__main__":
    listen_for_changes()
