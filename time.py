import requests
import psycopg2
from tqdm import tqdm
import datetime
from datetime import timedelta

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

# Function to convert milliseconds to human-readable date format
def convert_milliseconds_to_date(milliseconds):
    return datetime.datetime.fromtimestamp(milliseconds / 1000).strftime('%Y-%m-%d %H:%M:%S')

try:
    # Get the date for the previous day
    previous_day = datetime.datetime.now() - timedelta(days=1)

    # Set the date filter in the query parameters
    start_date = int(previous_day.timestamp()) * 1000
    end_date = start_date + 86399999  # Adding milliseconds for one day
    params["where"] = f"area_code='1975' AND rta_date >= {start_date} AND rta_date < {end_date}"

    # Print the URL for debugging
    print("URL:", url)
    print("Params:", params)

    # Send a GET request to the API
    response = requests.get(url, params=params)

    # Print the response content for debugging
    print("Response content:", response.content)

except Exception as e:
    print("An error occurred:", str(e))
