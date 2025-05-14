from google.cloud import pubsub_v1
import os
import json
import datetime
from concurrent import futures

class Pub:

    def __init__(self, pid, tid):
        self.project_id = pid
        self.topic_id = tid

    def future_callback(self, future):
        try:
            # Wait for the result of the publish operation.
            future.result()  
        except Exception as e:
            pass

    #takes the date and opens the folder corosponding to that date and reads all of the jsons in that folder
    #then publishes them to the Trimet_IHS topic
    def Publish_PubSub(self, data):
        start_time = datetime.datetime.now()

        publish_count = 0
        future_list = []

        publisher = pubsub_v1.PublisherClient()
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_id}`
        topic_path = publisher.topic_path(self.project_id, self.topic_id)
        for item in data:
            # Data must be a bytestring
            datastr = json.dumps(item).encode("utf-8")

            future = publisher.publish(topic_path, datastr)
            future.add_done_callback(self.future_callback)
            #add it to the list of futures
            future_list.append(future)

            publish_count += 1

        end_time = datetime.datetime.now()
        run_time = end_time - start_time

        #wait until the futures are finished
        for future in futures.as_completed(future_list):
            continue

        #print(f"Published {publish_count} messages to {topic_path} in {run_time}.")
        return int(publish_count)