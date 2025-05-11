import os
import json
import pandas as pd
import Validate_Transform as vt
import Load as load
import logging
import datetime

logging.basicConfig(filename='time.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

load_time_start = datetime.datetime.now()
read_count = 1

folder_path = './Received_Data'
if os.path.exists(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r', encoding='utf-8') as f:

                transform_time_start = datetime.datetime.now()

                data = json.load(f)
                messages = vt.Transform(pd.DataFrame(data))

                read_count = len(messages)
                
                transform_time_end = datetime.datetime.now()
                
                
                #load the data into the database
                conn = load.dbconnect()
                load.createTablesIfNeeded(conn)
                load_count = load.load_data(conn, messages)

                logging.info(f"Transformed {filename}, {read_count}, {load_count}, in {(transform_time_end-transform_time_start).total_seconds()} at a rate of {read_count/((transform_time_end-transform_time_start).total_seconds())} messages per second")

folder_path = '/Received_Data'
if os.path.exists(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r', encoding='utf-8') as f:

                transform_time_start = datetime.datetime.now()

                data = json.load(f)
                messages = vt.Transform(pd.DataFrame(data))

                read_count = len(messages)
                
                transform_time_end = datetime.datetime.now()
                
                
                #load the data into the database
                conn = load.dbconnect()
                load.createTablesIfNeeded(conn)
                load_count = load.load_data(conn, messages)

                logging.info(f"Transformed {filename}, {read_count}, {load_count}, in {(transform_time_end-transform_time_start).total_seconds()} at a rate of {read_count/((transform_time_end-transform_time_start).total_seconds())} messages per second")

