import sys
import os
import logging
import datetime
import pandas as pd

#append the root folder so we can access modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

import Modules.dataLogging as log
import Modules.Publish as Publish

#configures the error logging
logging.basicConfig(filename=f'.\Logs\{datetime.date.today()}_error.log', level=logging.ERROR, 
                    format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s')

if not os.path.exists("ManualLog.csv"):
    print("No ManualLog.csv detected.")
    df = pd.DataFrame(columns=["date", "tripcount"])
    df.to_csv("ManualLog.csv", index=False)


stopPublisher = Publish.Pub(os.getenv("PROJECTID"), os.getenv("STOPTOPIC"))       
folder_path = os.path.join(project_root, "StopEvents")
sensorReadings = 0
if os.path.exists(folder_path):
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for dirname in dirnames:
            sensorReadings = 0
            directpath = os.path.join(dirpath, dirname)
            for filename in os.listdir(directpath):
                if filename.endswith('.json'):
                    file_path = os.path.join(directpath, filename)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            messages = pd.read_json(file_path)
                            sensorReadings += len(messages)
                            data = messages.to_dict(orient='records')
                            stopPublisher.Publish_PubSub(data)
                    except Exception as e:
                        print(f"Error reading {file_path}: {e}")
            new_df = pd.DataFrame([{"date": dirname, "tripcount":sensorReadings}])
            new_df.to_csv("ManualLog.csv", mode='a', header=False, index=False)