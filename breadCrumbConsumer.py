from google.cloud import pubsub_v1
import datetime
import json
import pandas as pd
import time
import Modules.Load as load
import Modules.dataLogging as log
import Modules.Subscribe as Subscribe
import Modules.Validate_Transform as vt
import logging

def safe_json_load(s):
    try:
        return json.loads(s)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None


#configures the error logging
logging.basicConfig(filename=f'.\Logs\{datetime.date.today()}_error.log', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

project_id = "data-eng-456119"
subscription_id = "my-sub"
timeout = 2000  # seconds

trimetSubscriber = Subscribe.Sub(project_id, subscription_id , timeout)

# we need a while true so that the script runs indefinitely within systemd
while True:
    try:
        # timestamps to track program
        with open("timestamp.txt", "a") as f:
            f.write(f"The current timestamp is: {str(datetime.datetime.now())}\n")
            f.close()
            
        time.sleep(10)

        trimetSubscriber.consumer()

        #grab the data from the subscriber and pulls the datastring out of the json
        transformedMessages = pd.DataFrame(trimetSubscriber.messages)
        transformedMessages = transformedMessages['data'].apply(safe_json_load).dropna()
        transformedMessages = pd.json_normalize(transformedMessages['data'].apply(json.loads))
        transformedMessages = vt.Transform(transformedMessages)

        conn = load.dbconnect()
        load.createTables(conn)
        load_count = load.load_data(conn, transformedMessages, f"{datetime.date.today()}")

        #add datalog here
        log.consumerLog(trimetSubscriber.count)
        log.dataSaved(trimetSubscriber.messages.memory_usage(index=True).sum()/1024)

    except Exception as e:
        logging.error(f"An error occurred: {e}")     

