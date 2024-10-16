import psycopg2
import json
import datetime
import requests

# Function to fetch and insert data for a specific date range
def fetch_and_insert_data(start_date, end_date):
    # Connect to your PostgreSQL/PostGIS database
    conn = psycopg2.connect(
        dbname="sitcenter_postgis_datalake",
        user="la_noche_estrellada",
        password="Cfq,thNb13@",
        host="10.100.200.150",
        port="5439"
    )
    cur = conn.cursor()

    # Create table if it does not exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS crimes (
        id SERIAL PRIMARY KEY,
        objectid INT,
        year INT,
        period INT,
        crime_code_id INT,
        time_period INT,
        hard_code_id INT,
        ud VARCHAR(100),
        organ VARCHAR(200),
        dat_vozb VARCHAR(100),
        dat_sover TIMESTAMP,
        stat VARCHAR(100),
        dat_vozb_str VARCHAR(100),
        dat_sover_str VARCHAR(100),
        tz1id VARCHAR(100),
        reg_id INT,
        city_id INT,
        org_code VARCHAR(40),
        fz1r18p5 VARCHAR(200),
        fz1r18p6 VARCHAR(200),
        transgression_id INT,
        organ_kz VARCHAR(300),
        crime_place_id INT,
        fe1r32p1 VARCHAR(100),
        weekday INT,
        globalid VARCHAR(38),
        geom GEOMETRY(Point, 4326),
        CONSTRAINT fk_crime_place_id FOREIGN KEY (crime_place_id) REFERENCES crime_place_code (id)
    );
    """
    cur.execute(create_table_query)
    conn.commit()

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
        'where': f"dat_sover>=timestamp '{start_date.strftime('%Y-%m-%d')} 00:00:00' AND dat_sover<=timestamp '{end_date.strftime('%Y-%m-%d')} 23:59:59'"
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
                    
                    # Perform lookup for crime_code_id
                    crime_code_query = "SELECT id FROM crime_code WHERE code = %s"
                    cur.execute(crime_code_query, (attributes['crime_code'],))
                    crime_code_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                    # Perform lookup for crime_place_id
                    place_code_query = "SELECT id FROM crime_place_code WHERE code = %s"
                    cur.execute(place_code_query, (attributes['fe1r29p1_id'],))
                    crime_place_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                    # Perform lookup for city_id
                    city_code_query = "SELECT id FROM crime_city_code WHERE code = %s"
                    cur.execute(city_code_query, (attributes['city_code'],))
                    city_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                    # Perform lookup for reg_id
                    reg_code_query = "SELECT id FROM crime_reg_code WHERE code = %s"
                    cur.execute(reg_code_query, (attributes['reg_code'],))
                    reg_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                    # Perform lookup for hard_code_id
                    hard_code_query = "SELECT id FROM crime_hard_code WHERE code = %s"
                    cur.execute(hard_code_query, (attributes['hard_code'],))
                    hard_code_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                    # Perform lookup for transgression_id
                    transgression_code_query = "SELECT id FROM crime_transgression_code WHERE code = %s"
                    cur.execute(transgression_code_query, (attributes['transgression'],))
                    transgression_id = cur.fetchone()[0] if cur.rowcount > 0 else None
                    
                    # Define your INSERT statement
                    insert_query = """
                    INSERT INTO crimes (objectid, year, period, crime_code_id, time_period, hard_code_id,
                                          ud, organ, dat_vozb, dat_sover, stat, dat_vozb_str, dat_sover_str,
                                          tz1id, reg_id, city_id, org_code, fz1r18p5, fz1r18p6, transgression_id,
                                          organ_kz, crime_place_id, fe1r32p1, weekday, globalid, geom)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));
                    """
                    
                    # Add geometry coordinates to attributes dictionary
                    attributes['x'] = geometry['x']
                    attributes['y'] = geometry['y']
                    
                    # Execute the INSERT statement
                    cur.execute(insert_query, (attributes['objectid'], attributes['yr'], attributes['period'], crime_code_id,
                                                attributes['time_period'], hard_code_id, attributes['ud'], attributes['organ'],
                                                attributes['dat_vozb'], attributes['dat_sover'], attributes['stat'],
                                                attributes['dat_vozb_str'], attributes['dat_sover_str'], attributes['tz1id'],
                                                reg_id, city_id, attributes['org_code'], attributes['fz1r18p5'], attributes['fz1r18p6'],
                                                transgression_id, attributes['organ_kz'], crime_place_id, attributes['fe1r32p1'],
                                                attributes['weekday'], attributes['globalid'], attributes['x'], attributes['y']))
            
            else:
                print(f"No features found for the specified date range.")
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching data: {e}")
    finally:
        # Execute the UPDATE statements
        update_address_id_query = """
        UPDATE crimes AS r
        SET address_id = (
            SELECT s.id
            FROM address_buildings AS s
            ORDER BY r.geom <-> s.marker
            LIMIT 1
        );
        """
        cur.execute(update_address_id_query)

        update_full_address_query = """
        UPDATE crimes
        SET full_address = address_buildings.full_address
        FROM address_buildings
        WHERE crimes.address_id = address_buildings.id;
        """
        cur.execute(update_full_address_query)

        update_district_id_query = """
        UPDATE crimes
        SET district_id = address_buildings.district_id
        FROM address_buildings
        WHERE crimes.address_id = address_buildings.id;
        """
        cur.execute(update_district_id_query)

        conn.commit()
        cur.close()
        conn.close()

# Define the start and end dates for January 2015
start_date = datetime.datetime(2015, 1, 1)
end_date = datetime.datetime(2015, 1, 31)

# Fetch and insert data for January 2015
fetch_and_insert_data(start_date, end_date)
print("Data inserted successfully.")