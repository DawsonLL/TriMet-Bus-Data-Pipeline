import os
import json
import pandas as pd
import Validate_Transform as vt
import Load as load
import logging
import datetime

logging.basicConfig(filename='time.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
read_count = 0
completed = []


for folder_path in ['./Received_Data', '/Received_Data']:
    if os.path.exists(folder_path):
        print(f"{folder_path} exists")
        for filename in os.listdir(folder_path):
            if filename.endswith('.json') and filename not in completed:
                print(f"LOADING {filename}")

                read_count = 0
                load_time_start = datetime.datetime.now()

                file_path = os.path.join(folder_path, filename)
                with open(file_path, 'r', encoding='utf-8') as f:

                    transform_time_start = datetime.datetime.now()

                    messages = json.load(f)
                    messages = vt.Transform(pd.DataFrame(messages))

                    read_count = len(messages)
                    
                    transform_time_end = datetime.datetime.now()
                    
                    #load the data into the database
                    conn = load.dbconnect()
                    load.createTables(conn)
                    load_count = load.load_data(conn, messages)

                    logging.info(f"Transformed {filename}, {read_count}, {0}, in {(transform_time_end-transform_time_start).total_seconds()} at a rate of {read_count/((transform_time_end-transform_time_start).total_seconds())} messages per second")