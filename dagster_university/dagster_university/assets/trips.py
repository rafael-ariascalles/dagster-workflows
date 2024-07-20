import requests
from . import constants
from dagster import asset
import duckdb
import os
from dagster_duckdb import DuckDBResource

@asset
def taxi_trips_file() -> None:
    """
        Raw parquet file
    """
    month_to_fetch = '2023-03'
    raw_trips = requests.get(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet")
    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)

@asset
def taxi_zone_file() -> None:
    """
        Taxi Zones (Parquet)
    """
    taxi_zones = requests.get("https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD")
    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(taxi_zones.content)

@asset(deps=["taxi_trips_file"])
def taxi_trips(database: DuckDBResource) -> None:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    sql_query = """
        create or replace table trips as (
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          from 'data/raw/taxi_trips_2023-03.parquet'
        );
    """
    #conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    #conn.execute(sql_query)
    with database.get_connection() as conn:
      conn.execute(sql_query)



@asset(deps=["taxi_zone_file"])
def taxi_zones(database: DuckDBResource) -> None:
    """
      The taxi zone dataset, loaded into a DuckDB database
    """
    sql_query = """
        create or replace table zones as (
          select
            LocationID as zone_id,
            zone,
            borough,
            the_geom as geometry
          from 'data/raw/taxi_zones.csv'
        );
    """
    #conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    #conn.execute(sql_query)
    with database.get_connection() as conn:
      conn.execute(sql_query)
