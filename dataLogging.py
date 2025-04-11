import csv
import os

def dataLog(data):

    if not os.path.exists("dataLog.csv"):
        print("Creating dataLog.csv")
        field_headers = ["date","day_of_week", "time_accessed", "#_sensor_readings", "total_data_saved_(KBs)", "#_pub/sub_message_published/recieved"]
        with open("dataLog.csv", 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=field_headers)
            writer.writeheader()
            writer.writerows(data)
    else:
        print("Updating dataLog.csv")
        field_headers = ["date","day_of_week", "time_accessed", "#_sensor_readings", "total_data_saved_(KBs)", "#_pub/sub_message_published/recieved"]
        with open("dataLog.csv", "a") as file:
            writer = csv.DictWriter(file, fieldnames=field_headers)
            writer.writerows(data)