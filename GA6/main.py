import functions_framework
from google.cloud import pubsub_v1


project_id = "intro-to-big-data-439410"
topic_id = "ibd-ga6-topic"

# Initialize Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def process_file(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    publish_to_pubsub(bucket, name)


# Function to publish file name to Pub/Sub topic
def publish_to_pubsub(bucket_name, file_name):
    message_data = file_name.encode("utf-8")
    future = publisher.publish(topic_path, data=message_data)
    print(f"Published message {future.result()} for file {file_name} to Pub/Sub topic.")