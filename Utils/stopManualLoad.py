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
                    format='%(asctime)s - %(levelname)s - %(message)s')


stopPublisher = Publish.Pub(os.getenv("PROJECTID"), os.getenv("STOPTOPIC"))       
folder_path = "./StopEvents"
sensorReadings = 0
if os.path.exists(folder_path):
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.endswith('.json'):
                file_path = os.path.join(dirpath, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        messages = pd.read_json(file_path)
                        sensorReadings += len(messages)
                        stopPublisher.Publish_PubSub(messages)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
