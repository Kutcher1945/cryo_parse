import psycopg2
import json
import datetime
import requests
from tqdm import tqdm

# Function to fetch and insert data for a given year, month, and start day
def fetch_and_insert_data(year, month, start_day):
    # Define the URL
    url = f"https://gis.kgp.kz/arcgis/rest/services/KPSSU/crime/FeatureServer/1/query"
    
    # Define the query parameters
    params = {
        'f': 'json',
        'returnIdsOnly': False,
        'returnGeometry': True,
        'spatialRel': 'esriSpatialRelIntersects',
        'outSR': 4326,
        'outFields': '*',
        'where': f"dat_sover>=timestamp '{year}-{month:02}-{start_day:02} 00:00:00' AND dat_sover<=timestamp '{year}-{month:02}-{start_day + 19:02} 23:59:59' AND (city_code='1975')"
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
                    INSERT INTO z_crimes (objectid, yr, period, crime_code, time_period, hard_code,
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
                print(f"No features found for {year}-{month:02}.")
        else:
            print(f"Failed to fetch data for {year}-{month:02}. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching data for {year}-{month:02}: {e}")

# Connect to your PostgreSQL/PostGIS database
conn = psycopg2.connect(
    dbname="rwh_gis_database",
    user="rwh_analytics",
    password="4HPzQt2HyU@",
    host="172.30.227.205",
    port="5439"
)

# Create a cursor object
cur = conn.cursor()

# Create table if it does not exist
create_table_query = """
CREATE TABLE IF NOT EXISTS z_crimes (
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

# Set starting year and month
start_year = 2015
start_month = 1

# Iterate over years and months
for year in tqdm(range(start_year, 2025), desc="Years"):
    # Set start month for each year
    if year == start_year:
        months_range = range(start_month, 13)
    else:
        months_range = range(1, 13)
        
    for month in tqdm(months_range, desc="Months"):
        # Iterate over each 20-day period in the month
        for start_day in range(1, 32, 20):
            fetch_and_insert_data(year, month, start_day)
            conn.commit()  # Commit after each batch of data

# Close the cursor and connection
cur.close()
conn.close()

print("Data has been inserted into the database.")
