import csv
import os

def dataLog(data):
    """
    Logs sensor data to a CSV file named 'dataLog.csv'.
    
    If the file doesn't exist, it creates the file and writes the header and data.
    If the file already exists, it appends the new data to the existing file.

    Parameters:
    data (list of dict): A list of dictionaries, where each dictionary represents
                         a row of sensor data with the following keys:
                         - "date"
                         - "day_of_week"
                         - "time_accessed"
                         - "#_sensor_readings"
                         - "total_data_saved_(KBs)"
                         - "#_pub/sub_message_published/recieved"
    """
    # Define the headers to be used in the CSV
    field_headers = [
        "date",
        "day_of_week", 
        "time_accessed", 
        "#_sensor_readings", 
        "total_data_saved_(KBs)", 
        "#_pub/sub_message_published/recieved"
        ]
    
    # Check if the log file exists
    if not os.path.exists("dataLog.csv"):
        print("Creating dataLog.csv")
        # Open the file in write mode and write header and data
        with open("dataLog.csv", 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=field_headers)
            writer.writeheader()
            writer.writerows(data)
    else:
        print("Updating dataLog.csv")
        # Open the file in append mode and add new data rows
        with open("dataLog.csv", "a") as file:
            writer = csv.DictWriter(file, fieldnames=field_headers)
            writer.writerows(data)