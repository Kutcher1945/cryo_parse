import psycopg2
import json
import datetime
import requests
from tqdm import tqdm
import schedule
import time

# Function to fetch and insert data for the previous day
def fetch_and_insert_data():
    # Get the date for the previous day
    previous_date = datetime.datetime.now() - datetime.timedelta(days=1)
    
    # Define the URL
    url = "https://gis.kgp.kz/arcgis/rest/services/KPSSU/crime/FeatureServer/1/query"
    
    # Define the query parameters
    params = {
        'f': 'json',
        'returnIdsOnly': False,
        'returnGeometry': True,
        'spatialRel': 'esriSpatialRelIntersects',
        'outSR': 4326,
        'outFields': '*',
        'where': f"dat_sover>=timestamp '{previous_date.strftime('%Y-%m-%d')} 00:00:00' AND dat_sover<=timestamp '{previous_date.strftime('%Y-%m-%d')} 23:59:59' AND (city_code='1975')"
    }
    
    try:
        # Send GET request to fetch data
        response = requests.get(url, params=params, timeout=10)  # Set timeout to 10 seconds
        
        # Check if request was successful
        if response.status_code == 200:
            data = response.json()
            
            # Check if response contains 'features' key
            if 'features' in data:
                # Iterate over features and insert them into the database
                for feature in data['features']:
                    attributes = feature['attributes']
                    geometry = feature['geometry']
                    
                    # Convert milliseconds timestamp to datetime object if it exists
                    for key, value in attributes.items():
                        if isinstance(value, int) and key.endswith('_date'):
                            attributes[key] = datetime.datetime.fromtimestamp(value / 1000.0)
                    
                    # Convert dat_sover to timestamp if it exists
                    if 'dat_sover' in attributes:
                        attributes['dat_sover'] = datetime.datetime.fromtimestamp(attributes['dat_sover'] / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Define your INSERT statement
                    insert_query = """
                    INSERT INTO kgp_crimes (objectid, yr, period, crime_code, time_period, hard_code,
                                          ud, organ, dat_vozb, dat_sover, stat, dat_vozb_str, dat_sover_str,
                                          tz1id, reg_code, city_code, org_code, fz1r18p5, fz1r18p6, transgression,
                                          organ_kz, fe1r29p1_id, fe1r32p1, weekday, globalid, geom)
                    VALUES (%(objectid)s, %(yr)s, %(period)s, %(crime_code)s, %(time_period)s,
                            %(hard_code)s, %(ud)s, %(organ)s, %(dat_vozb)s, %(dat_sover)s, %(stat)s,
                            %(dat_vozb_str)s, %(dat_sover_str)s, %(tz1id)s, %(reg_code)s, %(city_code)s,
                            %(org_code)s, %(fz1r18p5)s, %(fz1r18p6)s, %(transgression)s, %(organ_kz)s,
                            %(fe1r29p1_id)s, %(fe1r32p1)s, %(weekday)s, %(globalid)s,
                            ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326));
                    """
                    
                    # Add geometry coordinates to attributes dictionary
                    attributes['x'] = geometry['x']
                    attributes['y'] = geometry['y']
                    
                    # Execute the INSERT statement
                    cur.execute(insert_query, attributes)
            
            else:
                print(f"No features found for {previous_date.strftime('%Y-%m-%d')}.")
        else:
            print(f"Failed to fetch data for {previous_date.strftime('%Y-%m-%d')}. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching data for {previous_date.strftime('%Y-%m-%d')}: {e}")

# Function to print time left until next parse
def print_time_left():
    # Get the current time
    now = datetime.datetime.now()
    # Calculate the time until the next parsing (2:00 AM)
    next_parse = now.replace(hour=2, minute=0, second=0, microsecond=0)
    if now > next_parse:
        next_parse += datetime.timedelta(days=1)
    time_left = next_parse - now
    # Print the time left
    print(f"Time left until next parse: {time_left}")

# Connect to your PostgreSQL/PostGIS database
conn = psycopg2.connect(
    dbname="rwh_datalake",
    user="rwh_analytics",
    password="4HPzQt2HyU@",
    host="172.30.227.205",
    port="5439"
)

# Create a cursor object
cur = conn.cursor()

# Create table if it does not exist
create_table_query = """
CREATE TABLE IF NOT EXISTS kgp_crimes (
    id SERIAL PRIMARY KEY,
    objectid INT,
    yr INT,
    period INT,
    crime_code VARCHAR(10),
    time_period INT,
    hard_code VARCHAR(5),
    ud VARCHAR(100),
    organ VARCHAR(200),
    dat_vozb VARCHAR(100),  -- Change to VARCHAR
    dat_sover TIMESTAMP,
    stat VARCHAR(100),
    dat_vozb_str VARCHAR(100),
    dat_sover_str VARCHAR(100),
    tz1id VARCHAR(100),
    reg_code VARCHAR(40),
    city_code VARCHAR(20),
    org_code VARCHAR(40),
    fz1r18p5 VARCHAR(200),
    fz1r18p6 VARCHAR(200),
    transgression VARCHAR(1),
    organ_kz VARCHAR(300),
    fe1r29p1_id VARCHAR(3),
    fe1r32p1 VARCHAR(100),
    weekday INT,
    globalid VARCHAR(38),
    geom GEOMETRY(Point, 4326)
);
"""
cur.execute(create_table_query)

# Commit the table creation
conn.commit()

# Define a job to fetch and insert data daily
def job():
    fetch_and_insert_data()
    conn.commit()
    print("Parsing done.")

# Schedule the job to run daily at 2:00 AM
schedule.every().day.at("02:00").do(job)

# Schedule the printing of time left every minute
schedule.every().minute.do(print_time_left)

# Run the scheduler loop indefinitely
while True:
    schedule.run_pending()
    time.sleep(60)  # Sleep for 60 seconds before checking again
