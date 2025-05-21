import os
import pandas as pd
import Validate_Transform as vt
import Load as load
import logging
import datetime
import json


read_count = 0
completed = []

#patches the data to pull out all of the datastrings into their own json
for folder_path in ['./Received_Data', '/Received_Data']:
    if os.path.exists(folder_path):
        print(f"{folder_path} exists")
        for filename in os.listdir(folder_path):
            if filename.endswith('.json') and filename not in completed:
                print(f"LOADING {filename}")
                file_path = os.path.join(folder_path, filename)

                messages = pd.read_json(file_path)

                # Optionally, expand the parsed data into separate columns
                messages = pd.json_normalize(messages['data'].apply(json.loads))

                print(messages)

                file_path = os.path.join("./Patched_Data", filename)
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(messages.to_json(orient='records'))


                