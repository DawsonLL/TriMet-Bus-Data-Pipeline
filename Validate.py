import pandas as pd
import ast

#convers 
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
        
#def Validation(messages):
    #for index, row in messages.iterrows():

error_count = 0

log_file = "./Received_Data/2025-04-11.json"
with open(log_file, "r", encoding="utf-8") as f:

    df = pd.read_json(log_file)
    expanded = df['data'].apply(eval_to_df)
    messages = pd.concat(expanded.tolist(), ignore_index=True)

    '''

    sorted_messages = messages.sort_values(by=["VEHICLE_ID", "ACT_TIME"])

    for index, row in sorted_messages.iterrows():
        print(row["VEHICLE_ID"], row["ACT_TIME"] )

    '''

    #split the dataframe into multiples based off ID
    vehicle_groups = {vehicle_id: group for vehicle_id, group in messages.groupby('VEHICLE_ID')}
    for vehicle_id in vehicle_groups:
        vehicle_groups[vehicle_id].sort_values(by='ACT_TIME', inplace=True)
        for index, row in vehicle_groups[vehicle_id].iterrows():
            print(row["VEHICLE_ID"], row["ACT_TIME"], row["EVENT_NO_TRIP"] )