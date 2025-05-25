from google.cloud import pubsub_v1
import datetime
import json
import os
import pandas as pd
import time
import logging
import Modules.Load as load
import Modules.dataLogging as log
import Modules.Subscribe as Subscribe
import Modules.Validate_Transform as vt



#configures the error logging
logging.basicConfig(filename=f'./Logs/{datetime.date.today()}_error.log', level=logging.ERROR, 
                    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s')

project_id = os.getenv("PROJECTID")
subscription_id = os.getenv("STOPSUB")
timeout = 1000  # seconds
trimetSubscriber = Subscribe.Sub(project_id, subscription_id, timeout)
dBConn = load.dBConnect(os.getenv("DBNAME"), os.getenv("DBUSERNAME"), os.getenv("DBPASSWORD"), os.getenv("HOSTNAME"))
trimetDB = load.TripDataLoader(dBConn)

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
        if not transformedMessages.empty:
            transformedMessages = transformedMessages.dropna()
            transformedMessages = pd.json_normalize(transformedMessages['data'].apply(json.loads))
            transformedMessages['direction'] = transformedMessages['direction'].astype(str)
            transformedMessages = vt.Transform(transformedMessages)

            trimetDB.create_tables()
            load_count = trimetDB.load_data_trips(transformedMessages, f"{datetime.date.today()}")

            #add datalog here
            log.consumerLog(trimetSubscriber.count)
            log.dataSaved(transformedMessages.memory_usage(index=True).sum()/1024)

    except Exception as e:
        logging.error(f"An error occurred: {e}")     