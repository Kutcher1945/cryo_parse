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
    dbname="sitcenter_postgis_datalake",
    user="la_noche_estrellada",
    password="Cfq,thNb13@",
    host="172.30.227.205",
    port="5439"
)

# Create a cursor object
cur = conn.cursor()

# Insert data into the table
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

total_records = 0
existing_objectids = set()

try:
    # Fetch existing objectids from the database to avoid duplicates
    cur.execute("SELECT objectid FROM road_traffic_incident")
    existing_objectids.update(row[0] for row in cur.fetchall())

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
                    # Debugging: Print out the length of values tuple and placeholders in the insert query
                    # print("Length of values tuple:", len(values))
                    # print("Number of placeholders:", insert_query.count("%s"))
                    # Execute the insert query
                    cur.execute(insert_query, values)
                    existing_objectids.add(objectid)  # Update existing objectids set
            
            # Increment the result offset for the next batch
            params["resultOffset"] += params["resultRecordCount"]
        else:
            print("Error:", response.status_code)
            break

    # Execute the UPDATE statements
    update_address_id_query = """
    UPDATE road_traffic_incident AS r
    SET address_id = (
        SELECT s.id
        FROM address_buildings AS s
        ORDER BY r.geometry <-> s.marker
        LIMIT 1
    )
    WHERE address_id IS NULL;
    """
    cur.execute(update_address_id_query)

    update_full_address_query = """
    UPDATE road_traffic_incident
    SET full_address = address_buildings.full_address
    FROM address_buildings
    WHERE road_traffic_incident.address_id = address_buildings.id
    AND road_traffic_incident.full_address IS NULL;
    """
    cur.execute(update_full_address_query)

    update_district_id_query = """
    UPDATE road_traffic_incident
    SET district_id = address_buildings.district_id
    FROM address_buildings
    WHERE road_traffic_incident.address_id = address_buildings.id
    AND road_traffic_incident.district_id IS NULL;
    """
    cur.execute(update_district_id_query)

    update_latitude_and_longitude_query = """
    UPDATE road_traffic_incident
    SET
      latitude = ST_Y(geometry::geometry),
      longitude = ST_X(geometry::geometry)
    WHERE latitude IS NULL AND longitude IS NULL
    """
    cur.execute(update_latitude_and_longitude_query)

    update_quarter_query = """
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
    cur.execute(update_quarter_query)

finally:
    # Commit the transaction
    conn.commit()

    # Close the cursor and the connection
    cur.close()
    conn.close()

    print(f"Total number of records inserted: {total_records}")
    print("Data insertion and update completed.")
