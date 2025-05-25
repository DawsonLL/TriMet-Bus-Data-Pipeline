import requests
import os
import datetime
import logging
import Modules.dataLogging as log
import Modules.Publish as Publish

#configures the error logging
logging.basicConfig(filename=f'./Logs/{datetime.date.today()}_error.log', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

#our base Url
baseUrl = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
#reads vehicleids.txt and parses into a list for each id entry
vehicleIds = [line.strip() for line in open("vehicleids.txt","r").readlines()]

#Current Date, Weekday, Time of Collection Start
currDate = datetime.date.today()
day = currDate.strftime("%A")
time = datetime.datetime.now().strftime("%H:%M:%S")
sensorReadings = 0
pubCount = 0
subCount = 0
logData = {"date": currDate, "day_of_week": day, "time_accessed": time, "#_sensor_readings": sensorReadings, "total_data_saved_(KBs)": 0.0, "#_pub_message_published": pubCount, "#_sub_message_received" : subCount}
log.preLoad(logData)


trimetPublisher = Publish.Pub(os.getenv("PROJECTID"), os.getenv("BREADCRUMBTOPIC"))        

for id in vehicleIds:
    url = baseUrl + id
    data = requests.get(url)
    if data.status_code == 200:
        try:
            data = data.json()
            sensorReadings += len(data)
            trimetPublisher.Publish_PubSub(data)
        except Exception as e:
            logging.error(f"An error occurred: {e}")


#Logs Data After Collection
try:
    logData = {"#_sensor_readings": sensorReadings, "#_pub_message_published": trimetPublisher.pubCount}
    log.dataLog(logData)
    print("Data Logging Successful")
except Exception as e:
    logging.error(f"An error occurred: {e}")

