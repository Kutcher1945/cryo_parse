import requests
import psycopg2
from tqdm import tqdm
import datetime
import time
import schedule

# Define the URL for the API request
url = "https://gis.kgp.kz/server/rest/services/KPSSU/DTP/FeatureServer/0/query"

# Define query parameters
params = {
    "f": "geojson",
    "where": "area_code='1975'",
    "returnGeometry": "true",
    "spatialRel": "esriSpatialRelIntersects",
    "outFields": "*",
    "outSR": "4326",
    "resultOffset": 0,
    "resultRecordCount": 2000  # Set the initial batch size
}

# Database connection parameters
db_params = {
    "dbname": "sitcenter_postgis_datalake",
    "user": "la_noche_estrellada",
    "password": "Cfq,thNb13@",
    "host": "172.30.227.205",
    "port": "5439"
}

# Define insert query
insert_query = """
INSERT INTO road_traffic_incident (
    objectid, fd1r08p1, rta_date, fd1id, yr, period, area_code_id, fd1r05p1,
    fd1r01p1, fd1r07p2, fd1r07p3, fd1r07p4, fd1r071p1_id, fd1r071p1,
    fd1r09p1, fd1r14p1, fd1r141p1_id, fd1r141p1, fd1r13p1, fd1r13p2,
    fd1r06p1, fd1r06p3, vehicle_category, is_public_transport,
    fd1r041p1, fd1r072p1, fd1r073p1, fd1r074p1, fd1r10p1, fd1r142p6,
    fd1r17, type_dtp, fd1r17_descrip, load_date, fd1r07p1, fd1r061p0,
    fd1r061p1, fd1r061p2, globalid, reg_code_id, geometry
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)
);
"""

# Define update queries
update_queries = [
    """
    UPDATE road_traffic_incident AS r
    SET address_id = (
        SELECT s.id
        FROM address_buildings AS s
        ORDER BY r.geometry <-> s.marker
        LIMIT 1
    )
    WHERE address_id IS NULL;
    """,
    """
    UPDATE road_traffic_incident
    SET full_address = address_buildings.full_address
    FROM address_buildings
    WHERE road_traffic_incident.address_id = address_buildings.id
    AND road_traffic_incident.full_address IS NULL;
    """,
    """
    UPDATE road_traffic_incident
    SET district_id = address_buildings.district_id
    FROM address_buildings
    WHERE road_traffic_incident.address_id = address_buildings.id
    AND road_traffic_incident.district_id IS NULL;
    """,
    """
    UPDATE road_traffic_incident
    SET
      latitude = ST_Y(geometry::geometry),
      longitude = ST_X(geometry::geometry)
    WHERE latitude IS NULL AND longitude IS NULL
    """,
    """
    UPDATE road_traffic_incident
    SET quarter_id = 
        CASE 
            WHEN period BETWEEN 1 AND 3 THEN 1
            WHEN period BETWEEN 4 AND 6 THEN 2
            WHEN period BETWEEN 7 AND 9 THEN 3
            WHEN period BETWEEN 10 AND 12 THEN 4
            ELSE quarter_id
        END
    WHERE quarter_id IS NULL;
    """
]

def insert_data(conn, query, values):
    cur = conn.cursor()
    cur.execute(query, values)
    cur.close()

def update_data(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    cur.close()

def fetch_and_process_data():
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(**db_params)

        # Fetch existing objectids from the database to avoid duplicates
        cur = conn.cursor()
        cur.execute("SELECT objectid FROM road_traffic_incident")
        existing_objectids = set(row[0] for row in cur.fetchall())

        # Process data in batches
        while True:
            # Send a GET request to the API
            response = requests.get(url, params=params)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse the GeoJSON response
                data = response.json()
                features = data.get("features", [])
                if not features:
                    break  # No more records to fetch
                
                total_records = len(features)

                for feature in tqdm(features, desc="Inserting records", unit="record"):
                    if 'properties' in feature:
                        objectid = feature['properties']['objectid']
                        # Skip insertion if objectid already exists
                        if objectid in existing_objectids:
                            continue

                        rta_date_timestamp = feature['properties']['rta_date'] / 1000  # Convert milliseconds to seconds
                        rta_date = datetime.datetime.fromtimestamp(rta_date_timestamp)
                        
                        load_date_timestamp = feature['properties']['load_date'] / 1000  # Convert milliseconds to seconds
                        load_date = datetime.datetime.fromtimestamp(load_date_timestamp)

                        # Perform lookup for area_code_id
                        area_code_query = "SELECT id FROM crime_city_code WHERE code = %s"
                        cur.execute(area_code_query, (feature['properties']['area_code'],))
                        area_code_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                        # Perform lookup for reg_code_id
                        reg_code_query = "SELECT id FROM crime_reg_code WHERE code = %s"
                        cur.execute(reg_code_query, (feature['properties']['fd1r06p2'],))
                        reg_code_id = cur.fetchone()[0] if cur.rowcount > 0 else None
                
                        # Convert timestamps to strings
                        rta_date_str = rta_date.strftime('%Y-%m-%d %H:%M:%S')
                        load_date_str = load_date.strftime('%Y-%m-%d %H:%M:%S')

                        # Extract geometry coordinates
                        geom_x, geom_y = feature['geometry']['coordinates']
                
                        # Extract values from the feature
                        values = (
                            objectid,
                            feature['properties']['fd1r08p1'],
                            rta_date_str,  # Convert to string
                            feature['properties']['fd1id'],
                            feature['properties']['yr'],
                            feature['properties']['period'],
                            area_code_id,
                            feature['properties']['fd1r05p1'],
                            feature['properties']['fd1r01p1'],
                            feature['properties']['fd1r07p2'],
                            feature['properties']['fd1r07p3'],
                            feature['properties']['fd1r07p4'],
                            feature['properties']['fd1r071p1_id'],
                            feature['properties']['fd1r071p1'],
                            feature['properties']['fd1r09p1'],
                            feature['properties']['fd1r14p1'],
                            feature['properties']['fd1r141p1_id'],
                            feature['properties']['fd1r141p1'],
                            feature['properties']['fd1r13p1'],
                            feature['properties']['fd1r13p2'],
                            feature['properties']['fd1r06p1'],
                            feature['properties']['fd1r06p3'],
                            feature['properties']['vehicle_category'],
                            feature['properties']['is_public_transport'],
                            feature['properties']['fd1r041p1'],
                            feature['properties']['fd1r072p1'],
                            feature['properties']['fd1r073p1'],
                            feature['properties']['fd1r074p1'],
                            feature['properties']['fd1r10p1'],
                            feature['properties']['fd1r142p6'],
                            feature['properties']['fd1r17'],
                            feature['properties']['type_dtp'],
                            feature['properties']['fd1r17_descrip'],
                            load_date_str,  # Convert to string
                            feature['properties']['fd1r07p1'],
                            feature['properties']['fd1r061p0'],
                            feature['properties']['fd1r061p1'],
                            feature['properties']['fd1r061p2'],
                            feature['properties']['globalid'],
                            reg_code_id,
                            geom_x,
                            geom_y
                        )
                        insert_data(conn, insert_query, values)
                        existing_objectids.add(objectid)  # Update existing objectids set
                
                # Increment the result offset for the next batch
                params["resultOffset"] += params["resultRecordCount"]
            else:
                print("Error:", response.status_code)
                break

        # Execute the UPDATE statements
        for query in update_queries:
            update_data(conn, query)

    except Exception as e:
        print("An error occurred:", e)

    finally:
        if 'conn' in locals():
            # Commit the transaction
            conn.commit()

            # Close the connection
            conn.close()

def print_time_until_next_parse():
    now = datetime.datetime.now()
    next_run = now.replace(hour=2, minute=50, second=0, microsecond=0)
    if now > next_run:
        next_run += datetime.timedelta(days=1)
    time_until_next_parse = next_run - now
    print("Time until next parse:", time_until_next_parse)
    print("Next parse at:", next_run)

# Schedule the job to fetch and process data
schedule.every().day.at("02:50").do(fetch_and_process_data)

# Schedule the job to print time until next parse
schedule.every().minute.do(print_time_until_next_parse)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)  # Wait for 1 second before checking the schedule again
