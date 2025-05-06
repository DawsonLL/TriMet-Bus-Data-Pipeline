import pandas as pd
import ast

#conversts the string literal to a proper panda dataframe
def eval_to_df(x):
    try:
        return pd.json_normalize(ast.literal_eval(x))
    except Exception as e:
        pass


#Validation for indvidual validation, if not satisfy  return False
def Indvidual_Validation(message_data):

    column_array = ["EVENT_NO_TRIP", "EVENT_NO_STOP", "OPD_DATE", "VEHICLE_ID", "METERS", "ACT_TIME", "GPS_LONGITUDE", "GPS_LATITUDE", "GPS_SATELLITES", "GPS_HDOP"]

    try:
        #techincally literal_eval encounters an error with the null values already which should be caught by the error but adding each check should be more thorough 
        message_dic = ast.literal_eval(message_data)
    except Exception as e:
        return False

    #existence validation for each column
    for column_name in column_array:
        if message_dic.get(column_name, False) == None:
            return False
    
    #GPS_LATITUDE should be between 42°N (42) to 46° 15'N (46.25)  (the rough borders of oregon)
    if message_dic["GPS_LATITUDE"] > 46.25 or message_dic["GPS_LATITUDE"] < 42:
        return False
    #GPS_LATITUDE should be between 116° 45'W (-116.75) to 124° 30'W (-124.5) (the rough borders of oregon)
    if  message_dic["GPS_LONGITUDE"] > -116.75 or message_dic["GPS_LONGITUDE"] < -124.5:
        return False
    
    #GPS_SATELLITES is always greater than 4
    if message_dic["GPS_SATELLITES"] < 4.0:
        return False
    
    #ACT_TIME does not exceed 24 hours
    if message_dic["ACT_TIME"] > 2073600:
        return False
        

def Transform(messages):
    #for index, row in messages.iterrows():
    error_count = 0

    #split the dataframe into multiples based off ID
    trip_groups = {trip_id: group for trip_id, group in messages.groupby('EVENT_NO_TRIP')}
    for trip_id in trip_groups:
        for index, row in trip_groups[trip_id].iterrows():
            Indvidual_Validation(row)

        #drops the unused columns
        trip_groups[trip_id] = trip_groups[trip_id].drop("GPS_SATELLITES", axis='columns')
        trip_groups[trip_id] = trip_groups[trip_id].drop("GPS_HDOP", axis='columns')

        #create the timestamp
        trip_groups[trip_id]['TIMESTAMP'] = trip_groups[trip_id].apply(lambda row: pd.to_datetime(row['OPD_DATE'], format="%d%b%Y:%H:%M:%S") + pd.to_timedelta(row['ACT_TIME'], unit='s'), axis=1)
        trip_groups[trip_id] = trip_groups[trip_id].drop('OPD_DATE', axis='columns')
        trip_groups[trip_id] = trip_groups[trip_id].drop('ACT_TIME', axis='columns')

        trip_groups[trip_id].sort_values(by='TIMESTAMP', inplace=True)

        #calculates the time
        trip_groups[trip_id]['dMETERS'] = trip_groups[trip_id].groupby("EVENT_NO_TRIP")['METERS'].diff()
        trip_groups[trip_id]['dTIMESTAMP'] =  trip_groups[trip_id].groupby("EVENT_NO_TRIP")['TIMESTAMP'].diff().dt.total_seconds()
        trip_groups[trip_id]['SPEED'] = trip_groups[trip_id].apply(lambda row: row['dMETERS'] / row['dTIMESTAMP'], axis=1)
        trip_groups[trip_id] = trip_groups[trip_id].drop(columns=['dMETERS', 'dTIMESTAMP'])

        #sets the the first row speed equal to the second
        if len(trip_groups[trip_id]) > 1:
            trip_groups[trip_id].iloc[0, trip_groups[trip_id].columns.get_loc('SPEED')] = trip_groups[trip_id].iloc[1]['SPEED']

    return trip_groups

log_file = "./Received_Data/2025-04-11.json"
with open(log_file, "r", encoding="utf-8") as f:

    #we need to pull out the nested data string from the json
    df = pd.read_json(log_file)
    expanded = df['data'].apply(eval_to_df)
    messages = pd.concat(expanded.tolist(), ignore_index=True)

trip_groups = Transform(messages)

if trip_groups:
    for trip_id in trip_groups:
        print(trip_groups[trip_id])