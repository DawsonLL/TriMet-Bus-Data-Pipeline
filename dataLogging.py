import pandas as pd
import os

def dataLog(data):

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
    
    # Check if the log file exists
    if not os.path.exists("dataLog.csv"):
        print("Creating dataLog.csv")

        # Create an empty DataFrame with those columns
        df = pd.DataFrame(columns=columns)

        # Save to CSV
        df.to_csv("dataLog.csv", index=False)

        # Turn into DataFrame
        new_df = pd.DataFrame([data])

        # Append the new Data without writing the header again
        new_df.to_csv("dataLog.csv", mode='a', header=False, index=False)
        

    else:
        print("Updating dataLog.csv")
        # Turn into DataFrame
        new_df = pd.DataFrame([data])

        # Append the new Data without writing the header again
        new_df.to_csv('dataLog.csv', mode='a', header=False, index=False)


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
