from google.cloud import pubsub_v1
import datetime
import os
import json
import pandas as pd
import time
import Modules.dataLogging as log
import Modules.Load as load
import Modules.Validate_Transform as vt
import logging

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


# we need a while true so that the script runs indefinitely within systemd
while True:
    # timestamps to track program
    with open("timestamp.txt", "a") as f:
        f.write(f"The current timestamp is: {str(datetime.datetime.now())}\n")
        f.close()
    time.sleep(10)

    project_id = "data-eng-456119"
    subscription_id = "Trimet_IHS-sub"
    timeout = 2000  # seconds
    count = 0
    path = "./Received_Data/"
    messages = []

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    start_time = datetime.datetime.now()

    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
        try:
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
            streaming_pull_future.result(timeout=timeout)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        finally:
            #transform the data
            end_time = datetime.datetime.now()
            run_time = end_time - start_time
            print(f"{count} messages received in {run_time}")
            streaming_pull_future.cancel()
            streaming_pull_future.result()

            messages = pd.DataFrame(messages)
            #pulls the datastring out of the json
            messages = pd.json_normalize(messages['data'].apply(json.loads))
            messages = vt.Transform(messages)

            #load the data into the database
            conn = load.dbconnect()
            load.createTables(conn)
            load_count = load.load_data(conn, messages, f"{datetime.date.today()}")

            #add datalog here
            log.consumerLog(count)
            log.dataSaved(messages.memory_usage(index=True).sum()/1024)

            #COUNT(pubsub recieved)
            
