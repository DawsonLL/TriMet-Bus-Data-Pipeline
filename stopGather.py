import requests
import os
import datetime
import logging
import Modules.dataLogging as log
import Modules.Publish as Publish
from bs4 import BeautifulSoup
import pandas as pd



#configures the error logging
logging.basicConfig(filename=f'./Logs/{datetime.date.today()}_error.log', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

#our base Url
baseUrl = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="
#reads vehicleids.txt and parses into a list for each id entry
vehicleIds = [line.strip() for line in open("vehicleids.txt","r").readlines()]

#Current Date, Weekday, Time of Collection Start
currDate = datetime.date.today()
day = currDate.strftime("%A")
time = datetime.datetime.now().strftime("%H:%M:%S")
sensorReadings = 0
pubCount = 0
subCount = 0
#logData = {"date": currDate, "day_of_week": day, "time_accessed": time, "#_sensor_readings": sensorReadings, "total_data_saved_(KBs)": 0.0, "#_pub_message_published": pubCount, "#_sub_message_received" : subCount}
#log.preLoad(logData)


stopPublisher = Publish.Pub(os.getenv("PROJECTID"), os.getenv("STOPTOPIC"))        

for id in vehicleIds:
    
    resp = requests.get(f'{baseUrl}{id}')

    if resp.status_code == 200:
        soup = BeautifulSoup(resp.text, 'html.parser')
        tables = soup.find_all("table")
        headers = None

        all_rows = []
        for table in tables:
            rows = table.find_all("tr")
            if not headers:
                headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]

            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if cells:
                    all_rows.append(cells)
        stops_df = pd.DataFrame(all_rows, columns=headers)
        
        try:
            data = stops_df.to_dict(orient='records')
            sensorReadings += len(data)
            stopPublisher.Publish_PubSub(data)
        except Exception as e:
            logging.error(f"An error occurred: {e}")


#TO BE ADDED
"""
#Logs Data After Collection
try:
    logData = {"#_sensor_readings": sensorReadings, "#_pub_message_published": trimetPublisher.pubCount}
    log.dataLog(logData)
    print("Data Logging Successful")
except Exception as e:
    logging.error(f"An error occurred: {e}")
"""
