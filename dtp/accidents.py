import requests
import psycopg2
from tqdm import tqdm
import datetime

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

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    dbname="*",
    user="*",
    password="*",
    host="*",
    port="*"
)

# Create a cursor object
cur = conn.cursor()

# Define SQL query to create table if not exists
create_table_query = """
CREATE TABLE IF NOT EXISTS accidents (
    objectid INT PRIMARY KEY,
    fd1r08p1 VARCHAR(2),
    rta_date TIMESTAMP,
    fd1id VARCHAR(64),
    yr SMALLINT,
    period SMALLINT,
    area_code VARCHAR(32),
    fd1r05p1 VARCHAR(5),
    fd1r01p1 DOUBLE PRECISION,
    fd1r07p2 VARCHAR(200),
    fd1r07p3 VARCHAR(200),
    fd1r07p4 VARCHAR(200),
    fd1r071p1_id VARCHAR(100),
    fd1r071p1 VARCHAR(4000),
    fd1r09p1 VARCHAR(4000),
    fd1r14p1 VARCHAR(4000),
    fd1r141p1_id VARCHAR(1),
    fd1r141p1 VARCHAR(200),
    fd1r13p1 SMALLINT,
    fd1r13p2 SMALLINT,
    fd1r06p1 VARCHAR(200),
    fd1r06p2 VARCHAR(500),
    fd1r06p3 VARCHAR(200),
    vehicle_category VARCHAR(300),
    is_public_transport SMALLINT,
    fd1r041p1 SMALLINT,
    fd1r072p1 VARCHAR(50),
    fd1r073p1 SMALLINT,
    fd1r074p1 SMALLINT,
    fd1r10p1 SMALLINT,
    fd1r142p6 SMALLINT,
    fd1r17 VARCHAR(30),
    type_dtp SMALLINT,
    fd1r17_descrip VARCHAR(600),
    load_date TIMESTAMP,
    fd1r07p1 VARCHAR(4000),
    fd1r061p0 VARCHAR(3),
    fd1r061p1 VARCHAR(150),
    fd1r061p2 DOUBLE PRECISION,
    globalid VARCHAR(38),
    geometry GEOMETRY(Point, 4326)
);
"""
# Execute the create table query
cur.execute(create_table_query)

# Commit the transaction
conn.commit()

# Insert data into the table
insert_query = """
INSERT INTO accidents (
    objectid, fd1r08p1, rta_date, fd1id, yr, period, area_code, fd1r05p1,
    fd1r01p1, fd1r07p2, fd1r07p3, fd1r07p4, fd1r071p1_id, fd1r071p1,
    fd1r09p1, fd1r14p1, fd1r141p1_id, fd1r141p1, fd1r13p1, fd1r13p2,
    fd1r06p1, fd1r06p2, fd1r06p3, vehicle_category, is_public_transport,
    fd1r041p1, fd1r072p1, fd1r073p1, fd1r074p1, fd1r10p1, fd1r142p6,
    fd1r17, type_dtp, fd1r17_descrip, load_date, fd1r07p1, fd1r061p0,
    fd1r061p1, fd1r061p2, globalid, geometry
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)
);
"""

total_records = 0

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
        
        total_records += len(features)

        for feature in tqdm(features, desc="Inserting records", unit="record"):
            if 'properties' in feature:
                rta_date_timestamp = feature['properties']['rta_date'] / 1000  # Convert milliseconds to seconds
                rta_date = datetime.datetime.fromtimestamp(rta_date_timestamp)
                
                load_date_timestamp = feature['properties']['load_date'] / 1000  # Convert milliseconds to seconds
                load_date = datetime.datetime.fromtimestamp(load_date_timestamp)
        
                # Extract values from the feature
                values = (
                    feature['properties']['objectid'],
                    feature['properties']['fd1r08p1'],
                    rta_date,
                    feature['properties']['fd1id'],
                    feature['properties']['yr'],
                    feature['properties']['period'],
                    feature['properties']['area_code'],
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
                    feature['properties']['fd1r06p2'],
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
                    load_date,  # Use the converted load_date value
                    feature['properties']['fd1r07p1'],
                    feature['properties']['fd1r061p0'],
                    feature['properties']['fd1r061p1'],
                    feature['properties']['fd1r061p2'],
                    feature['properties']['globalid'],
                    feature['geometry']['coordinates'][0],
                    feature['geometry']['coordinates'][1]
                )
        
                # Execute the insert query
                cur.execute(insert_query, values)

        # Increment the result offset for the next batch
        params["resultOffset"] += params["resultRecordCount"]
    else:
        print("Error:", response.status_code)
        break

# Commit the transaction
conn.commit()

# Close the cursor and the connection
cur.close()
conn.close()

print(f"Total number of records inserted: {total_records}")
print("Data insertion completed.")
