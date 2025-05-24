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

project_id = os.getenv("PROJECTID")
subscription_id = os.getenv("STOPSUB")
timeout = 2000  # seconds
trimetSubscriber = Subscribe.Sub(project_id, subscription_id, timeout)
dBConn = load.dBConnect(os.getenv("DBNAME"), os.getenv("DBUSERNAME"), os.getenv("DBPASSWORD"), os.getenv("HOSTNAME"))
trimetDB = load.TripDataLoader(dBConn)


df = pd.read_json('StopEvents/2025-05-18/2025-05-18_3002.json')
df =vt.Transform(df)
print(df)
loadcount = trimetDB.load_data_trips(df, f"{datetime.date.today()}")