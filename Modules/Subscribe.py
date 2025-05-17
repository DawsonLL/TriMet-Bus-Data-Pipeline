from google.cloud import pubsub_v1
import datetime
import logging

class Sub:

    def __init__(self, pid, tid, time):
        self.project_id = pid
        self.topic_id = tid
        self.timeout = time  # seconds
        self.count = 0
        self.messages = []

        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.topic_id)

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
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
            #reset the values of count and message
            self.count = 0 
            self.messages = []
            start_time = datetime.datetime.now()

            print(f"Listening for messages on {self.subscription_path}..\n")

            with self.subscriber:
                try:
                    streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
                    streaming_pull_future.result(timeout=self.timeout)
                except Exception as e:
                    logging.error(f"An error occurred: {e}")
                finally:
                    #transform the data
                    end_time = datetime.datetime.now()
                    run_time = end_time - start_time
                    logging.error(f"{self.count} messages received in {run_time}")
                    streaming_pull_future.cancel()
                    streaming_pull_future.result()

