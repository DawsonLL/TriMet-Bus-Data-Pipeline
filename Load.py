import time
import psycopg2
import datetime
import io
import dataLogging as log

DBname = "pipeline_db"
DBuser = "postgres"
DBpwd = "coJBU@6uv4U4Hq"


def dbconnect():
    conn = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    conn.autocommit = True
    return conn

def createTables(conn):
    with conn.cursor() as cursor:
        # Only check for one table's existence, assuming both are created together
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables WHERE table_name = 'trip'
            );
        """)
        if cursor.fetchone()[0]:
            print("Tables already exist — skipping creation.")
            return

        cursor.execute("""
            DROP TYPE IF EXISTS service_type CASCADE;
            DROP TYPE IF EXISTS tripdir_type CASCADE;

            CREATE TYPE service_type AS ENUM ('Weekday', 'Saturday', 'Sunday', '0');
            CREATE TYPE tripdir_type AS ENUM ('Out', 'Back', '0');

            CREATE TABLE Trip (
                trip_id integer PRIMARY KEY,
                route_id integer,
                vehicle_id integer,
                service_key service_type,
                direction tripdir_type
            );

            CREATE TABLE BreadCrumb (
                tstamp timestamp,
                latitude float,
                longitude float,
                speed float,
                trip_id integer REFERENCES Trip
            );
        """)
        print("Created Trip and BreadCrumb tables.")

def load_data(conn, data):

    startDate = datetime.date.today()
    day = startDate.strftime("%A")
    count = 0

    with conn.cursor() as cursor:
        trip_seen = set()
        trip_buf = io.StringIO()
        bc_buf = io.StringIO()
        trip_count = 0
        bc_count = 0
        skipped_count = 0
          
        for index, row in data.iterrows():
            try:
                
                trip_id = int(row['trip_id'])
                # Trip: Avoid duplicates
                if trip_id not in trip_seen:
                    trip_seen.add(trip_id)
                    trip_buf.write(f"{trip_id},{int(row['route_id'])},{int(row['vehicle_id'])},{row['service_key']},{row['direction']}\n")
                    trip_count += 1

                # BreadCrumb
                tstamp = row['tstamp']
                bc_buf.write(f"{tstamp},{float(row['latitude'])},{float(row['longitude'])},{float(row['speed'])},{trip_id}\n")
                bc_count += 1
                count+=1
            except Exception as e:
                print(f"Skipping row due to error: {row} {e}")
                skipped_count += 1

        trip_buf.seek(0)
        bc_buf.seek(0)

        # COPY Trip
        try:
            cursor.copy_from(trip_buf, 'trip', sep=',', columns=('trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction'))
            print(f"Copied rows into Trip")
        except Exception as e:
            print(f"Trip copy error: {e}")

        # COPY BreadCrumb
        try:
            cursor.copy_from(bc_buf, 'breadcrumb', sep=',', columns=('tstamp', 'latitude', 'longitude', 'speed', 'trip_id'))
            print(f"Copied rows into BreadCrumb")
        except Exception as e:
            print(f"BreadCrumb copy error: {e}")

        conn.commit()
        #elapsed = time.perf_counter() - startDate
        #print(f"Finished Loading. Elapsed Time: {elapsed:0.4f} seconds")
        dbData = {"date": startDate, "day_of_week": day, "#_sensor_readings": len(data), "#_rows_added_trip" : trip_count, "#_rows_added_bread": bc_count, "total_rows_added": trip_count+bc_count, "#_rows_skipped": skipped_count}
        log.updateDBLog(dbData)
        return count
