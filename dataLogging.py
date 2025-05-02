import pandas as pd
import os


def preLoad(data):
    
    if not os.path.exists("dataLog.csv"):
        print("No dataLog.csv detected.")
        createLog(data)
        print("dataLog.csv Created.")
        return

    print("Preloading dataLog.csv")
    # Turn into DataFrame
    new_df = pd.DataFrame([data])

    # Append the new Data without writing the header again
    new_df.to_csv('dataLog.csv', mode='a', header=False, index=False)
    print("Preloading Finished")


def dataLog(data):
    df = pd.read_csv("dataLog.csv")

    # Increment specific columns in the last row
    df.at[df.index[-1], '#_pub_message_published'] += data['#_pub_message_published']
    df.at[df.index[-1], '#_sensor_readings'] += data['#_sensor_readings']

    # Save back to CSV (overwrite)
    df.to_csv("dataLog.csv", index=False)

def createLog(data):
    
    # Define the headers to be used in the CSV
    columns = [
        "date",
        "day_of_week", 
        "time_accessed", 
        "#_sensor_readings", 
        "total_data_saved_(KBs)", 
        "#_pub_message_published",
        "#_sub_message_received"
        ]
    
    print("Creating dataLog.csv...")

    # Create an empty DataFrame with those columns
    df = pd.DataFrame(columns=columns)

    # Save to CSV
    df.to_csv("dataLog.csv", index=False)

    # Turn into DataFrame
    new_df = pd.DataFrame([data])

    # Append the new Data without writing the header again
    new_df.to_csv("dataLog.csv", mode='a', header=False, index=False)



def dataSaved(size):
    # Read the CSV
    df = pd.read_csv('dataLog.csv')

    
    # Update the value in the last row
    df.at[df.index[-1], "total_data_saved_(KBs)"] += size
    
    # Save it back
    df.to_csv("dataLog.csv", index=False)
        

def consumerLog(data):
    # Read the CSV
    df = pd.read_csv('dataLog.csv')

    
    # Update the value in the last row
    df.at[df.index[-1], "#_sub_message_received"] += data
    
    # Save it back
    df.to_csv("dataLog.csv", index=False)
