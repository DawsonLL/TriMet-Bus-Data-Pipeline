import psycopg2
import datetime
import io
import logging
import Modules.dataLogging as log


class dBConnect:
    """
    A class to manage connection to a PostgreSQL database using psycopg2.

    Attributes:
        dbname (str): Database name.
        user (str): Username for authentication.
        password (str): Password for authentication.
        host (str): Database host address.
        conn (psycopg2.connection): The database connection object.
    """
    def __init__(self, dbname, user, password, host):
        """
        Initializes the dBConnect instance with connection parameters.
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.conn = None

    def connect(self):
        """
        Establishes a connection to the PostgreSQL database.
        """
        self.conn = psycopg2.connect(
            host=self.host,
            database=self.dbname,
            user=self.user,
            password=self.password
        )
        self.conn.autocommit = True

    def get_cursor(self):
        """
        Returns a cursor object for executing SQL commands.

        Automatically connects if no connection exists.
        """       
        if self.conn is None:
            self.connect()
        return self.conn.cursor()

    def commit(self):
        """
        Commits the current transaction to the database.
        """
        if self.conn:
            self.conn.commit()

    def close(self):
        """
        Closes the database connection.
        """
        if self.conn:
            self.conn.close()


class TripDataLoader:
    """
    A class responsible for creating database tables and bulk loading trip and breadcrumb data.

    Attributes:
        db (dBConnect): An instance of the dBConnect class used to access the database.
    """

    def __init__(self, dbConnection):
        """
        Initializes the TripDataLoader with a database connection object.

        Args:
            dbConnection (dBConnect): A connected dBConnect instance.
        """
        self.db = dbConnection

    def create_tables(self):
        """
        Creates the Trip and BreadCrumb tables (and required enum types) in the database
        if they don't already exist.
        """
        with self.db.get_cursor() as cursor:
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
                    trip_id integer
                );
            """)
            print("Created Trip and BreadCrumb tables.")

    def load_data(self, data, filename):
        """
        Loads trip and breadcrumb data into the PostgreSQL database using efficient bulk insert.

        Args:
            data (DataFrame): Transformed pandas DataFrame containing sensor data.
            filename (str): Name of the original file (for logging purposes).

        Returns:
            int: Total number of rows inserted into the database.
        """
        start_date = datetime.date.today()
        day = start_date.strftime("%A")
        trip_seen = set()
        trip_buf = io.StringIO()
        bc_buf = io.StringIO()
        trip_count = 0
        bc_count = 0
        skipped_count = 0

        for index, row in data.iterrows():
            try:
                trip_id = int(row['trip_id'])

                if trip_id not in trip_seen:
                    trip_seen.add(trip_id)
                    trip_buf.write(f"{trip_id},{int(row['route_id'])},{int(row['vehicle_id'])},{row['service_key']},{row['direction']}\n")
                    trip_count += 1

                tstamp = row['tstamp']
                bc_buf.write(f"{tstamp},{float(row['latitude'])},{float(row['longitude'])},{float(row['speed'])},{trip_id}\n")
                bc_count += 1

            except Exception as e:
                logging.error(f"Skipping row due to error: {row} {e}")
                skipped_count += 1

        trip_buf.seek(0)
        bc_buf.seek(0)

        with self.db.get_cursor() as cursor:
            try:
                cursor.copy_from(trip_buf, 'trip', sep=',', null='', columns=('trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction'))
                print(f"Copied rows into Trip")
            except Exception as e:
                print(f"Trip copy error: {e}")

            try:
                cursor.copy_from(bc_buf, 'breadcrumb', sep=',', null='', columns=('tstamp', 'latitude', 'longitude', 'speed', 'trip_id'))
                print(f"Copied rows into BreadCrumb")
            except Exception as e:
                logging.error(f"BreadCrumb copy error: {e}")  

        self.db.commit()

        dbData = {
            "file_name": filename,
            "date": start_date,
            "day_of_week": day,
            "#_sensor_readings": len(data),
            "#_rows_added_trip": trip_count,
            "#_rows_added_bread": bc_count,
            "total_rows_added": trip_count + bc_count,
            "#_rows_skipped": skipped_count
        }

        log.updateDBLog(dbData)
        return trip_count + bc_count
