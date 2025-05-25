import pandas as pd
import ast
import logging
import datetime
import Modules.Load as load

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
    message_df = assert_nulls(message_df)
    # GPS_LATITUDE should be between 42N (42) to 4615'N (46.25) (the rough borders of Oregon)
    message_df = assert_lat_range(message_df)
    # GPS_LONGITUDE should be between 116 45'W (-116.75) to 124 30'W (-124.5) (the rough borders of Oregon)
    message_df= assert_long_range(message_df)
    # ACT_TIME does not exceed 48 hours
    message_df = assert_acttime_48(message_df)
    #Each Trip_ID should have more than one breadcrumb associated with it
    message_df = assert_one_breadcrumb(message_df)

    return message_df

def Transform(messages):

    columns_bc = ["EVENT_NO_TRIP", 
            "EVENT_NO_STOP", 
            "OPD_DATE", 
            "VEHICLE_ID", 
            "METERS", 
            "ACT_TIME", 
            "GPS_LONGITUDE", 
            "GPS_LATITUDE", 
            "GPS_SATELLITES", 
            "GPS_HDOP"]
    
    columns_trips = ['vehicle_number', 'leave_time', 'train', 'route_number', 'direction',
       'service_key', 'trip_number', 'stop_time', 'arrive_time', 'dwell',
       'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load',
       'maximum_speed', 'train_mileage', 'pattern_distance',
       'location_distance', 'x_coordinate', 'y_coordinate', 'data_source',
       'schedule_status']

    if messages.columns.tolist() == columns_bc:
        # We need to pull out the nested data string from the JSON
        #messages = process_messages_in_chunks(messages)

        # Remove exact duplicates in place
        messages.drop_duplicates(inplace=True)

        # Each EVENT_NO_TRIP is assigned to only one VEHICLE_ID
        # messages["VEHICLE_ID"] = messages.groupby('EVENT_NO_TRIP')['VEHICLE_ID'].transform(lambda x: x.mode()[0])
        messages = assert_unique_vehicle_per_trip(messages)

        # Perform individual validation in place
        messages = Indvidual_Validation(messages)

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
        messages['service_key'] = ''
        messages['direction'] = ''

        # Speed should not exceed 120 MPH (53.6448 Meters per Second) in place
        messages = assert_speed_cap(messages)

        # Rename the column to match the schema in place
        messages.rename(columns={'EVENT_NO_TRIP': 'trip_id', 'GPS_LONGITUDE': 'longitude', 'GPS_LATITUDE': 'latitude', 'SPEED': 'speed', 'TIMESTAMP': 'tstamp', 'VEHICLE_ID': 'vehicle_id'}, inplace=True)

        #reapply the null validations
        messages = assert_nulls(messages)

        return messages
    

    elif messages.columns.tolist() == columns_trips:

        messages.drop_duplicates(inplace=True)

        messages = messages[['vehicle_number', 'route_number', 'trip_number', 'service_key', 'direction']]

        messages = assert_nulls(messages)
        messages.rename(columns={'trip_number': 'trip_id', 'route_number': 'route_id', 'vehicle_number': 'vehicle_id'}, inplace=True)
        messages['direction'] = messages['direction'].replace({'1' : 'Out', '0' : 'Back'})
        messages['service_key'] = messages['service_key'].replace({'W' : 'Weekday', 'M': 'Weekday', 'S' : 'Saturday', 'U': 'Sunday'})
        return messages
    else:
        print(f"COLUMNS DO NOT MATCH, {messages.columns.tolist()}")
#-----------------------------------------------------------------Assertions---------------------------------------------------------------------------

# Check for null values in specified columns and drop rows with nulls if found
def assert_nulls(df):

    for column in df.columns.tolist():
        # Count non-null values and total values in the column
        idNullCount = df[df[column] != 'nan'][column].notnull().sum()
        totalIdCount = len(df)
    
        try:
            # Assert there are no nulls
            assert idNullCount == totalIdCount
        except AssertionError as e:
            # Print how many nulls were found and drop them
            print(f"Found {totalIdCount - idNullCount} null values in {column}!")
            df = df[df[column] != 'nan']
            df = df.dropna(subset=[column])
            
        
    return df

# Ensure GPS_LATITUDE values are within the range [42, 46]
def assert_lat_range(df):
    # Count values outside the valid latitude range
    lower = (df['GPS_LATITUDE'] < 42 ).sum()
    upper = (df['GPS_LATITUDE'] > 46).sum()
    total = lower + upper
    try:
        # Assert all latitudes are within range
        assert total == 0
    except AssertionError as e:
        print(f"Found {total} values not in range of 42 and 46!")
        # Clip values to bring them within the valid range
        df['GPS_LATITUDE'] = df['GPS_LATITUDE'].clip(lower=42, upper=46)
        return df
    return df

# Ensure GPS_LONGITUDE values are within the range [-124.5, -116.75]
def assert_long_range(df):
    # Count values outside the valid longitude range
    lower = (df['GPS_LONGITUDE'] < -124.5 ).sum()
    upper = (df['GPS_LONGITUDE'] > -116.75).sum()
    total = lower + upper
    try:
        # Assert all longitudes are within range
        assert total == 0
    except AssertionError as e:
        print(f"Found {total} values not in range of -124.5 and -116.75!")
        # Clip values to bring them within the valid range
        df['GPS_LONGITUDE'] = df['GPS_LONGITUDE'].clip(lower=-124.5, upper=-116.75)
        return df
    return df

# Ensure ACT_TIME values do not exceed 48 hours (in seconds)
def assert_acttime_48(df):
    # Count values that exceed the 48-hour threshold (4147200 seconds)
    greater = (df['ACT_TIME'] > 4147200 ).sum()
    try:
        assert greater == 0
    except AssertionError as e:
        print(f"Found {greater} trip(s) longer than 48 hours!")
        # Clip any excessive ACT_TIME values to 48 hours
        df['ACT_TIME'] = df['ACT_TIME'].clip(upper=4147200)
        return df
    return df

# Ensure each trip (EVENT_NO_TRIP) has more than one breadcrumb
def assert_one_breadcrumb(df):
    # Count the number of trips that have only 1 breadcrumb
    unique_rows = len(df[df['EVENT_NO_TRIP'].map(df['EVENT_NO_TRIP'].value_counts()) == 1])
    try:
        assert unique_rows == 0
    except AssertionError as e:
        print(f"Found {unique_rows} trips with 1 breadcrumb!")
        # Filter out trips with only 1 breadcrumb
        df = df[df['EVENT_NO_TRIP'].map(df['EVENT_NO_TRIP'].value_counts()) > 1].copy()
        return df
    return df

# Cap SPEED values at a maximum of 53.6448
def assert_speed_cap(df):
    # Count speed values over the threshold
    speed = (df['SPEED'] > 53.6448 ).sum()
    try:
        assert speed == 0
    except AssertionError as e:
        # Clip speed values above the threshold
        df['SPEED'] = df['SPEED'].clip(upper=53.6448)
        return df
    return df

# Ensure each trip (EVENT_NO_TRIP) is associated with only one VEHICLE_ID
def assert_unique_vehicle_per_trip(df):
    try:
        # Group by trip and check for multiple vehicle IDs
        assert df.groupby('EVENT_NO_TRIP')['VEHICLE_ID'].nunique().le(1).all()
    except AssertionError as e:
        print("Some EVENT_NO_TRIP values are associated with multiple VEHICLE_IDs")
        # Fix by replacing with the most frequent (mode) VEHICLE_ID for each trip
        df["VEHICLE_ID"] = df.groupby('EVENT_NO_TRIP')['VEHICLE_ID'].transform(lambda x: x.mode()[0])
        return df
    return df