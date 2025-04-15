from google.cloud import pubsub_v1
import os
import json
import datetime

#takes the date and opens the folder corosponding to that date and reads all of the jsons in that folder
#then publishes them to the Trimet_IHS topic
def Publish_PubSub(date):
    start_time = datetime.datetime.now()
    #topic
    project_id = "data-eng-456119"
    topic_id = "Trimet_IHS"
    publish_count = 0
    futures = []

    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)

    for filename in os.listdir(f"{os.getcwd()}/Data/{date}"):
        with open(os.path.join(f"{os.getcwd()}/Data/{date}", filename), 'r') as file:
                data = json.load(file)
                for item in data:
                    # Data must be a bytestring
                    datastr = json.dumps(item).encode("utf-8")
                    future = publisher.publish(topic_path, datastr)
                    publish_count+= 1
                    futures.append(future)

    end_time = datetime.datetime.now()
    run_time = end_time - start_time

    #wait until the futures are finished
    for future in futures:
        try:
            future.result(timeout=30)
        except TimeoutError:
            print("Publishing timed out.")

    print(f"Published {publish_count} messages to {topic_path} in {run_time}.")
    return publish_count