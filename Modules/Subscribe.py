from google.cloud import pubsub_v1
import datetime
import logging

class Sub:
    """
    A class to subscribe and consume messages from a Google Cloud Pub/Sub topic.

    Attributes:
        project_id (str): Google Cloud project ID.
        topic_id (str): Pub/Sub topic or subscription name.
        timeout (int): Number of seconds to listen for messages.
        count (int): Number of messages received in the current session.
        messages (list): List of decoded messages received.
        subscriber (pubsub_v1.SubscriberClient): The Pub/Sub subscriber client.
        subscription_path (str): Full path of the subscription.
    """

    def __init__(self, pid, tid, time):
        """
        Initializes the subscription object.

        Args:
            pid (str): Google Cloud project ID.
            tid (str): Pub/Sub subscription name.
            time (int): Listening duration in seconds.
        """
        self.project_id = pid
        self.topic_id = tid
        self.timeout = time  # seconds
        self.count = 0
        self.messages = []

        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.topic_id)

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        """
        Callback function executed for each received message.

        Decodes the message, adds metadata, and acknowledges receipt.

        Args:
            message (pubsub_v1.subscriber.message.Message): The Pub/Sub message received.
        """
        self.count += 1

        msg_data = {
            "message_id": message.message_id,
            "data": message.data.decode("utf-8"),
            "attributes": dict(message.attributes),
            "publish_time": message.publish_time.isoformat()
        }

        self.messages.append(msg_data)
        message.ack()

    def consumer(self):
        """
        Starts listening for messages from the subscription.

        Resets counters and begins consuming messages for the specified timeout.
        Logs the number of messages received and the total run time.
        """
        self.count = 0
        self.messages = []
        start_time = datetime.datetime.now()

        print(f"Listening for messages on {self.subscription_path}..\n")

        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.topic_id)

        with self.subscriber:
            try:
                streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
                streaming_pull_future.result(timeout=self.timeout)
            except Exception as e:
                logging.error(f"An error occurred: {e}")
            finally:
                end_time = datetime.datetime.now()
                run_time = end_time - start_time
                logging.error(f"{self.count} messages received in {run_time}")
                streaming_pull_future.cancel()
                streaming_pull_future.result()

