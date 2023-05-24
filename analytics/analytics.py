import os
from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import logging
from geopy.distance import geodesic
import pandas as pd
from datetime import datetime

print("Waiting for the data generator...")
sleep(20)
print("ETL Starting...")

while True:
    try:
        psql_engine = create_engine(
            environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10
        )
        break
    except OperationalError:
        sleep(0.1)
print("Connection to PostgresSQL successful.")

# Write the solution here
# Create log directory if it doesn't exist
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)


def create_log_file():
    """
    This function creates a log file
    """
    # logfile name with timestamp
    log_filename = "etl_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"
    log_file_path = os.path.join(log_dir, log_filename)
    # set up logging
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(message)s",
    )
    print(f"Log file created and is located at: {os.getcwd()}\logs")


def log_decorator(func):
    """
    decorator function
    """

    def decorator_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.exception(e)
            raise

    return decorator_wrapper


def mysql_connection():
    """
    This function is to make a connection to MYSQL database
    """
    logging.info(f"inside function {mysql_connection.__name__}")
    mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
    return mysql_engine


def distance_calculator(row):
    """
    This function calculates the distance between co-ordinates. Uses geopy library
    """
    starting_coordinates = (33.05096, 15.21229)  # This values are assumed
    ending_coordinates = (row["location"]["latitude"], row["location"]["longitude"])
    distance = geodesic(starting_coordinates, ending_coordinates).km
    return distance


def transform_data(df):
    """
    This function uses pandas dataframe and performs transformation
    """
    logging.info(f"inside function {transform_data.__name__}")
    # Convert unix timestamp column to DateTime type
    df["timestamp"] = pd.to_datetime(df["time"], unit="s")
    # create a new column with calculated distance values
    df["distance_km"] = df.apply(distance_calculator, axis=1)
    # Group data by device_id and hour component of timestamp
    # calculate maximum temperature
    # amount of data points
    # total distance
    result = (
        df.groupby(["device_id", pd.Grouper(key="timestamp", freq="H")])
        .aggregate({"temperature": "max", "device_id": "count", "distance_km": "sum"})
        .rename(
            columns={
                "temperature": "max_temperature",
                "device_id": "no_of_occurences",
                "distance_km": "total_distance",
            }
        )
        .reset_index()
    )
    return df


@log_decorator
def etl(postgresql_table, mysql_table):
    """
    This function combines the other functions and performs ETL

    Args:
    postgresql_table (str): Table name in postgresql database
    mysql_table (str): Table name in MYSQL database
    """
    try:
        create_log_file()

        # Pull data from PostgreSQL
        query = f"SELECT * FROM {postgresql_table}"
        df = pd.read_sql_query(query, psql_engine)
        logging.info("Data pulled from PostgreSQL.")

        # Perform transformations on the data
        transformed_df = transform_data(df)
        logging.info("Data transformation completed.")

        # Connect to MySQL
        mysql_engine = mysql_connection()
        logging.info("Connected to MySQL.")

        # Insert data into MySQL
        transformed_df.to_sql(
            mysql_table, mysql_engine, if_exists="append", index=False
        )
        logging.info("Data inserted into MySQL.")
        print("ETL process completed")

    except Exception as e:
        raise


# call the main function `etl` by passing the table names
etl("devices", "agg_devices")

logging.info("Data transformation and insertion completed successfully!")
