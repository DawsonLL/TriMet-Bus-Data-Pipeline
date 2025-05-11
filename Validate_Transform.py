import pandas as pd
import ast
import logging
import datetime
import Load as load


#conversts the string literal to a proper panda dataframe
def eval_to_df(x):
    try:
        x = x.replace("null", "0")
        return pd.json_normalize(ast.literal_eval(x))
    except Exception as e:
        logging.error(f"An error occurred: {e} {x}")


#Validation for indvidual validation
def Indvidual_Validation(message_df):

    #existence validation for each column
    for column_name in ["EVENT_NO_TRIP", "EVENT_NO_STOP", "OPD_DATE", "VEHICLE_ID", 
            "METERS", "ACT_TIME", "GPS_LONGITUDE", "GPS_LATITUDE", 
            "GPS_SATELLITES", "GPS_HDOP"]:
        if column_name not in message_df.columns:
            message_df[column_name] = 0

    #GPS_LATITUDE should be between 42째N (42) to 46째 15'N (46.25)  (the rough borders of oregon)
    message_df['GPS_LATITUDE'] = message_df['GPS_LATITUDE'].clip(lower=42, upper=46)
    #GPS_LONGITUDE should be between 116째 45'W (-116.75) to 124째 30'W (-124.5) (the rough borders of oregon)
    message_df['GPS_LONGITUDE'] = message_df['GPS_LONGITUDE'].clip(lower=-124.5, upper=-116.75)
    #ACT_TIME does not exceed 24 hours
    message_df['ACT_TIME'] = message_df['ACT_TIME'].clip(upper=2073600)
    return message_df

def Transform(messages):

    #we need to pull out the nested data string from the json
    messages = pd.concat([eval_to_df(item) for item in messages['data']], ignore_index=True)

    #There are no messages that are exact duplicates of eachother    
    messages.drop_duplicates(inplace=True)


    #Each EVENT_NO_TRIP is assigned to only one VEHICLE_ID 
    messages["VEHICLE_ID"] = messages.groupby('EVENT_NO_TRIP')['VEHICLE_ID'].transform(lambda x: x.mode()[0])

    messages = Indvidual_Validation(messages)

    #drops the unused columns
    messages = messages.drop(columns=["GPS_SATELLITES", "GPS_HDOP", "EVENT_NO_STOP"])

    #create the timestamp
    messages['TIMESTAMP'] = messages.apply(lambda row: pd.to_datetime(row['OPD_DATE'], format="%d%b%Y:%H:%M:%S") + pd.to_timedelta(row['ACT_TIME'], unit='s'), axis=1)
    messages = messages.drop(columns=["OPD_DATE", "ACT_TIME"])
    messages.sort_values(by=['EVENT_NO_TRIP', 'TIMESTAMP'], inplace=True)

    #calculates the speed
    messages['dMETERS'] = messages.groupby("EVENT_NO_TRIP")['METERS'].diff()
    messages['dTIMESTAMP'] =  messages.groupby("EVENT_NO_TRIP")['TIMESTAMP'].diff().dt.total_seconds()
    messages['SPEED'] = messages['dMETERS'] / messages['dTIMESTAMP']
    messages = messages.drop(columns=['dMETERS', 'dTIMESTAMP', 'METERS'])

    #sets the the first row speed equal to the second

    messages['SPEED'] = messages.groupby('EVENT_NO_TRIP')['SPEED'].transform(lambda x: x.bfill())
    
    #create the columns we dont have values for
    messages['route_id'] = 0
    messages['service_key'] = 0
    messages['direction'] = 0

    #Speed does not exceed 120 MPH (53.6448 Meters per Second)
    messages['SPEED'] = messages['SPEED'].clip(upper=53.6448)

    #fill the speed values that don't get filled due to issues with trips with only single messages
    messages['SPEED'] = messages['SPEED'].fillna(0)

    #rename the column to match the schema
    messages = messages.rename(columns={'EVENT_NO_TRIP': 'trip_id', 'GPS_LONGITUDE': 'longitude', 'GPS_LATITUDE': 'latitude', 'SPEED': 'speed', 'TIMESTAMP': 'tstamp', 'VEHICLE_ID': 'vehicle_id'})

    return messages

'''
logging.basicConfig(filename='time.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

log_file = "./Received_Data/2025-04-11.json"

load_time_start = datetime.datetime.now()
length = 1

with open(log_file, "r", encoding="utf-8") as f:
    messages = pd.read_json(log_file)
    length = len(messages)

load_time_end = datetime.datetime.now()

logging.info(f"Loaded in {(load_time_end-load_time_start).total_seconds()}, {length} messages loaded")

transform_time_start = datetime.datetime.now()
messages = Transform(messages)
print(messages)
transform_time_end = datetime.datetime.now()
logging.info(f"Loaded in {(transform_time_end-transform_time_start).total_seconds()}, {length/((transform_time_end-transform_time_start).total_seconds())}")

conn = load.dbconnect()
load.createTablesIfNeeded(conn)
load.load_data(conn, messages)
'''