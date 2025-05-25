from google.cloud import pubsub_v1
import json
import datetime
from concurrent import futures
import logging

class Pub:
    """
    A class for publishing messages to a Google Cloud Pub/Sub topic.

    Attributes:
        project_id (str): Google Cloud project ID.
        topic_id (str): Pub/Sub topic ID.
        pubCount (int): Counter for successfully published messages.
        publisher (PublisherClient): Pub/Sub publisher client.
    """

    def __init__(self, pid, tid):
        """
        Initializes the Pub class with project and topic IDs.

        Args:
            pid (str): Google Cloud project ID.
            tid (str): Pub/Sub topic ID.
        """
        self.project_id = pid
        self.topic_id = tid
        self.pubCount = 0
        self.publisher = pubsub_v1.PublisherClient()

    def future_callback(self, future):
        try:
            # Wait for the result of the publish operation.
            future.result() 
            self.pubCount += 1 
        except Exception as e:
            logging.error(f"An error occurred: {e}")


    #publishes to the Trimet_IHS topic
    def Publish_PubSub(self, data):
        """
        Publishes a list of data items to a Pub/Sub topic.

        Args:
            data (list): A list of dictionaries to be published as messages.
        """
        start_time = datetime.datetime.now()

        future_list = []

        
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_id}`
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        for item in data:
            # Data must be a bytestring
            datastr = json.dumps(item).encode("utf-8")
            future = self.publisher.publish(topic_path, datastr)
            future.add_done_callback(self.future_callback)
            #add it to the list of futures
            future_list.append(future)

        end_time = datetime.datetime.now()
        run_time = end_time - start_time

        #wait until the futures are finished
        for future in futures.as_completed(future_list):
            continue

        #print(f"Published {publish_count} messages to {topic_path} in {run_time}.")