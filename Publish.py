from google.cloud import pubsub_v1
import os
import json
import datetime
from concurrent import futures

def future_callback(future):
    try:
        # Wait for the result of the publish operation.
        future.result()  
    except Exception as e:
        print(f"An error occurred in future_callback: {e}")

#takes the date and opens the folder corosponding to that date and reads all of the jsons in that folder
#then publishes them to the Trimet_IHS topic
def Publish_PubSub(data):
    start_time = datetime.datetime.now()
    #topic
    project_id = "data-eng-456119"
    topic_id = "Trimet_IHS"
    publish_count = 0
    future_list = []

    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)
    for item in data:
        # Data must be a bytestring
        datastr = json.dumps(item).encode("utf-8")

        future = publisher.publish(topic_path, datastr)
        future.add_done_callback(future_callback)
        #add it to the list of futures
        future_list.append(future)

        publish_count+= 1

    end_time = datetime.datetime.now()
    run_time = end_time - start_time

    #wait until the futures are finished
    for future in futures.as_completed(future_list):
        continue

    #print(f"Published {publish_count} messages to {topic_path} in {run_time}.")
    return int(publish_count)