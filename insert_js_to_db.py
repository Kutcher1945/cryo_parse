import psycopg2
import json

# Database connection parameters
DB_NAME = 'sitcenter_postgis_datalake'
DB_USER = 'la_noche_estrellada'
DB_PASSWORD = 'Cfq,thNb13@'
DB_HOST = '10.100.200.150'
DB_PORT = '5439'

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

# Load your JSON data
with open('fd1r08p1_data.json', 'r') as file:
    data = json.load(file)

# SQL statement to insert data into the table
insert_query = """
    INSERT INTO road_traffic_incident_type_code (name, code)
    VALUES (%s, %s)
"""

# Create a cursor object
cur = conn.cursor()

try:
    # Iterate over the JSON data and insert into the table
    for item in data[0]['domain']['codedValues']:
        name = item['name']
        code = item['code']
        cur.execute(insert_query, (name, code))
    
    # Commit the transaction
    conn.commit()
    print("Data inserted successfully!")

except (Exception, psycopg2.DatabaseError) as error:
    print("Error:", error)
    conn.rollback()

finally:
    # Close the cursor and connection
    cur.close()
    conn.close()
