import os
import requests
import google.auth
import google.auth.transport.requests
from google.cloud import pubsub_v1
from google.api_core import exceptions

# Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
LOCATION = "europe-west1"
TOPIC_ID = "dataplex-metadata-changes"
SUBSCRIPTION_ID = "dataplex-metadata-changes-sub"
FEED_ID = "retail-metadata-feed"

def get_access_token():
    credentials, project = google.auth.default()
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)
    return credentials.token

def create_pubsub_infra():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    
    try:
        publisher.create_topic(name=topic_path)
        print(f"Created Pub/Sub topic: {topic_path}")
    except exceptions.AlreadyExists:
        print(f"Pub/Sub topic already exists: {topic_path}")
    except Exception as e:
        print(f"Error creating Pub/Sub topic: {e}")
        
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    
    try:
        subscriber.create_subscription(name=subscription_path, topic=topic_path)
        print(f"Created Pub/Sub subscription: {subscription_path}")
    except exceptions.AlreadyExists:
        print(f"Pub/Sub subscription already exists: {subscription_path}")
    except Exception as e:
        print(f"Error creating Pub/Sub subscription: {e}")
        
    return topic_path

def create_metadata_change_feed(topic_path):
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    url = f"https://dataplex.googleapis.com/v1/projects/{PROJECT_ID}/locations/{LOCATION}/metadataFeeds"
    
    data = {
        "scope": {
            "projects": [f"projects/{PROJECT_ID}"]
        },
        "pubsubTopic": topic_path
    }
    
    print(f"Creating Metadata Change Feed {FEED_ID} via REST API...")
    response = requests.post(
        f"{url}?metadataFeedId={FEED_ID}",
        headers=headers,
        json=data
    )
    
    if response.status_code == 409:
        print(f"Metadata Change Feed {FEED_ID} already exists.")
    elif response.status_code >= 200 and response.status_code < 300:
        op_info = response.json()
        print(f"Metadata Change Feed creation started: {op_info.get('name')}")
    else:
        print(f"Error creating Metadata Change Feed: {response.status_code} - {response.text}")

if __name__ == "__main__":
    if not PROJECT_ID:
        print("Please set GOOGLE_CLOUD_PROJECT environment variable.")
        exit(1)
        
    topic_path = create_pubsub_infra()
    create_metadata_change_feed(topic_path)
