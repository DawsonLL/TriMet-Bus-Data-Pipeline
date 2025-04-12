from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import datetime
import os
import json

# we need a while true so that the script runs indefinitely within systemd
while True:
    # timestamps to track program
    with open("timestamp.txt", "a") as f:
        f.write("The current timestamp is: " + str(datetime.now()))
        f.close()
    time.sleep(10)

    project_id = "data-eng-456119"
    subscription_id = "Trimet_IHS-sub"
    timeout = 100  # seconds
    count = 0
    path = "./Received_Data/"
    log_file = os.path.join(path, f"{datetime.date.today()}.json")
    messages = []

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        global count, messages
        count += 1

        msg_data = {
            "message_id": message.message_id,
            "data": message.data.decode("utf-8"),
            "attributes": dict(message.attributes),
            "publish_time": message.publish_time.isoformat()
        }

        messages.append(msg_data)
        message.ack()

    start_time = datetime.datetime.now()

    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    # Load existing messages if the file exists
    if os.path.exists(log_file):
        with open(log_file, "r", encoding="utf-8") as f:
            try:
                messages = json.load(f)
                if not isinstance(messages, list):
                    messages = []
            except json.JSONDecodeError:
                messages = []

    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
        try:
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
            streaming_pull_future.result(timeout=timeout)
        except KeyboardInterrupt:
            print("Received shutdown signal. Stopping subscriber...")
        except TimeoutError:
            print("Subscription timed out.")
        finally:
            # Save all messages to a single JSON file
            with open(log_file, "w", encoding="utf-8") as f:
                json.dump(messages, f, indent=2)

            end_time = datetime.datetime.now()
            run_time = end_time - start_time
            print(f"{count} messages received in {run_time}")
            streaming_pull_future.cancel()
            streaming_pull_future.result()
