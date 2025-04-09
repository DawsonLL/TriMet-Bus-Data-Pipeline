import urllib.request as request
import urllib.error as error
import datetime
import os
import dataLogging as log


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
sensorReadings = "n/a"
totalData = "n/a"
pubsubCount = "n/a"


path = "./Data/" + str(datetime.date.today())
if not os.path.exists(path):

    for id in vehicleIds:
        url = baseUrl + id
        try:
            #retrieves the data for the assocaited ID and then download it into the Data Folder
            os.makedirs(path, exist_ok=True)
            request.urlretrieve(url, f"Data/{datetime.date.today()}/{datetime.date.today()}_{id}.json")
        except error.HTTPError as e:
            print(f"{e} FOR {id}")
            errorCount += 1 #Adds to Error Count
    
    #Logs Data After Collection
    data = [{"date": currDate, "day_of_week": day, "time_accessed": time, "#_sensor_readings": sensorReadings, "total_data_saved_(KBs)": totalData, "#_pub/sub_message_published/recieved": pubsubCount}]
    log.dataLog(data)
    print("Data Logging Successful")


else:
    print("Data Aready Collected for: " + str(datetime.date.today()))

