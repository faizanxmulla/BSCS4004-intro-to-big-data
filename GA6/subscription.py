from google.cloud import pubsub_v1, storage
import time


def callback(message):
    """Callback function to handle incoming messages."""
    filename = message.data.decode("utf-8")
    print(f"Received message for file: {filename}")
    try:
        # Adding a delay to ensure the file is available in storage
        time.sleep(5)
        lines = lines_counter(filename)
        print(f"The number of lines in {filename} are {lines}")
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
    message.ack()


def lines_counter(filename):
    """Count the number of lines in a file stored in Google Cloud Storage."""
    client = storage.Client()
    bucket = client.get_bucket("iitm-ibd-ga6")
    blob = bucket.blob(filename)
    with blob.open("r") as file:
        lines = len(file.readlines())
    return lines


def subscribe_pub_sub(project_id, subscription_name):
    """Subscribe to a Pub/Sub subscription and listen for messages."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    print(f"Subscribing to {subscription_path}...")

    # Listen for messages with an attached callback
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print("Listening for messages... Press Ctrl+C to exit.")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        print("Subscriber script terminated by user.")
        streaming_pull_future.cancel()
    except Exception as e:
        print(f"Error while listening for messages: {e}")
        streaming_pull_future.cancel()


if __name__ == "__main__":
    project_id = "intro-to-big-data-439410"
    subscription_name = "ibd-ga6-subscription"
    subscribe_pub_sub(project_id, subscription_name)