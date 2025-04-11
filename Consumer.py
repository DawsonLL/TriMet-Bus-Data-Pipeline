from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import datetime

project_id = "data-eng-456119"
subscription_id = "Trimet_IHS-sub"
# Number of seconds the subscriber should listen for messages
timeout = 1000
count = 0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global count
    count +=1 
    print(f"Received {message}. {count}")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")
start_time = datetime.datetime.now()
# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except KeyboardInterrupt:
        print("Received shutdown signal. Stopping subscriber...")
        end_time = datetime.datetime.now()
        run_time = end_time - start_time
        print(f"{count} Messages Recieved in {run_time}")
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.
    except TimeoutError:
        print("Received shutdown signal. Stopping subscriber...")
        end_time = datetime.datetime.now()
        run_time = end_time - start_time
        print(f"{count} Messages Recieved in {run_time}")
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.