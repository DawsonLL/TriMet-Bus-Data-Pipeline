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



df = pd.read_json('StopEvents/2025-05-18/2025-05-18_3002.json')
print(df.columns)