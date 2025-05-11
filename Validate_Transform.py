import pandas as pd
import ast
import logging
import datetime
import Load as load

# Converts the string literal to a proper pandas DataFrame
def eval_to_df(x):
    try:
        x = x.replace("null", "0")
        return pd.json_normalize(ast.literal_eval(x))
    except Exception as e:
        logging.error(f"An error occurred: {e} {x}")

def process_messages_in_chunks(messages, chunk_size=1000):
    # Use a generator to process chunks and avoid holding all data in memory at once
    def chunked_generator():
        start_idx = 0
        while start_idx < len(messages['data']):
            end_idx = min(start_idx + chunk_size, len(messages['data']))
            chunk = messages['data'][start_idx:end_idx]
            yield pd.concat([eval_to_df(item) for item in chunk], ignore_index=True)
            start_idx = end_idx
    
    # Concatenate the chunks incrementally
    result = pd.DataFrame()  # Initialize an empty DataFrame to accumulate results
    for chunk_df in chunked_generator():
        result = pd.concat([result, chunk_df], ignore_index=True)
    
    return result


# Validation for individual validation
def Indvidual_Validation(message_df):

    # Existence validation for each column
    for column_name in ["EVENT_NO_TRIP", "EVENT_NO_STOP", "OPD_DATE", "VEHICLE_ID", 
            "METERS", "ACT_TIME", "GPS_LONGITUDE", "GPS_LATITUDE", 
            "GPS_SATELLITES", "GPS_HDOP"]:
        if column_name not in message_df.columns:
            message_df[column_name] = 0

    # GPS_LATITUDE should be between 42N (42) to 4615'N (46.25) (the rough borders of Oregon)
    message_df['GPS_LATITUDE'] = message_df['GPS_LATITUDE'].clip(lower=42, upper=46)
    # GPS_LONGITUDE should be between 116 45'W (-116.75) to 124 30'W (-124.5) (the rough borders of Oregon)
    message_df['GPS_LONGITUDE'] = message_df['GPS_LONGITUDE'].clip(lower=-124.5, upper=-116.75)
    # ACT_TIME does not exceed 24 hours
    message_df['ACT_TIME'] = message_df['ACT_TIME'].clip(upper=2073600)
    return message_df

def Transform(messages):

    # We need to pull out the nested data string from the JSON
    messages = process_messages_in_chunks(messages)

    # Remove exact duplicates in place
    messages.drop_duplicates(inplace=True)

    # Each EVENT_NO_TRIP is assigned to only one VEHICLE_ID
    messages["VEHICLE_ID"] = messages.groupby('EVENT_NO_TRIP')['VEHICLE_ID'].transform(lambda x: x.mode()[0])

    # Perform individual validation in place
    Indvidual_Validation(messages)

    # Drop the unused columns in place
    messages.drop(columns=["GPS_SATELLITES", "GPS_HDOP", "EVENT_NO_STOP"], inplace=True)

    # Create the timestamp in place
    messages['TIMESTAMP'] = messages.apply(lambda row: pd.to_datetime(row['OPD_DATE'], format="%d%b%Y:%H:%M:%S") + pd.to_timedelta(row['ACT_TIME'], unit='s'), axis=1)
    messages.drop(columns=["OPD_DATE", "ACT_TIME"], inplace=True)

    # Sort values in place
    messages.sort_values(by=['EVENT_NO_TRIP', 'TIMESTAMP'], inplace=True)

    # Calculate the speed in place
    messages['dMETERS'] = messages.groupby("EVENT_NO_TRIP")['METERS'].diff()
    messages['dTIMESTAMP'] = messages.groupby("EVENT_NO_TRIP")['TIMESTAMP'].diff().dt.total_seconds()
    messages['SPEED'] = messages['dMETERS'] / messages['dTIMESTAMP']
    messages.drop(columns=['dMETERS', 'dTIMESTAMP', 'METERS'], inplace=True)

    # Set the first row speed equal to the second in place
    messages['SPEED'] = messages.groupby('EVENT_NO_TRIP')['SPEED'].transform(lambda x: x.bfill())
    
    # Create the columns we don't have values for in place
    messages['route_id'] = 0
    messages['service_key'] = 0
    messages['direction'] = 0

    # Speed should not exceed 120 MPH (53.6448 Meters per Second) in place
    messages['SPEED'] = messages['SPEED'].clip(upper=53.6448)

    # Fill the speed values that don't get filled due to issues with trips with only single messages
    messages['SPEED'] = messages['SPEED'].fillna(0)

    # Rename the column to match the schema in place
    messages.rename(columns={'EVENT_NO_TRIP': 'trip_id', 'GPS_LONGITUDE': 'longitude', 'GPS_LATITUDE': 'latitude', 'SPEED': 'speed', 'TIMESTAMP': 'tstamp', 'VEHICLE_ID': 'vehicle_id'}, inplace=True)

    return messages