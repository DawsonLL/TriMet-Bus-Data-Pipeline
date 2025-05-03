import requests
import datetime
import logging
import dataLogging as log
import Publish as pub

#configures the error logging
logging.basicConfig(filename='error.log', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

#our base Url
baseUrl = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
#reads vehicleids.txt and parses into a list for each id entry
vehicleIds = [line.strip() for line in open("vehicleids.txt","r").readlines()]


#Error Counter
errorCount = 0

#Current Date, Weekday, Time of Collection Start
currDate = datetime.date.today()
day = currDate.strftime("%A")
time = datetime.datetime.now().strftime("%H:%M:%S")
sensorReadings = 0
pubCount = 0
subCount = 0
logData = {"date": currDate, "day_of_week": day, "time_accessed": time, "#_sensor_readings": sensorReadings, "total_data_saved_(KBs)": 0.0, "#_pub_message_published": pubCount, "#_sub_message_received" : subCount}
log.preLoad(logData)

'''
#Progress Tracker
totalIds = len(vehicleIds)
countIds = 0
'''


for id in vehicleIds:
    url = baseUrl + id
    data = requests.get(url)
    if data.status_code == 200:
        '''
        countIds += 1
        print(f"Progress: {countIds}/{totalIds}", end='\r')
        '''
        try:
            data = data.json()
            sensorReadings += len(data)
            pubCount += pub.Publish_PubSub(data)
        except Exception as e:
            errorCount += 1 #Adds to Error Count
    '''
    else:
        totalIds -= 1
        print(f"Progress: {countIds}/{totalIds}", end='\r')
    '''

#Logs Data After Collection
try:
    logData = {"#_sensor_readings": sensorReadings, "#_pub_message_published": pubCount}
    log.dataLog(logData)
    print("Data Logging Successful")
except Exception as e:
    logging.error(f"An error occurred: {e}")

