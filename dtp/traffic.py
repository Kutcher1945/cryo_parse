import requests
import json
import datetime

# Define the URL for the API request
url = "https://gis.kgp.kz/server/rest/services/KPSSU/DTP/FeatureServer/0/query"

# Define query parameters
params = {
    "f": "json",
    "where": "area_code='1975'",
    "returnGeometry": "true",
    "spatialRel": "esriSpatialRelIntersects",
    "outFields": "*",
    "outSR": "4326"
}

# Send a GET request to the API
response = requests.get(url, params=params)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()

    # Filter data based on rta_date and convert it to human-readable format
    filtered_features = []
    for feature in data["features"]:
        rta_date_timestamp = feature['attributes']['rta_date'] / 1000  # Convert milliseconds to seconds
        rta_date = datetime.datetime.fromtimestamp(rta_date_timestamp).strftime('%Y-%m-%d')
        
        # Filter data between the specified date range (2015-01-01 to 2015-01-29)
        if '2013-01-01' <= rta_date <= '2020-02-25':
            filtered_features.append(feature)

    # Create a GeoJSON feature collection
    features = []

    for feature in filtered_features:
        feature_geojson = {
            "type": "Feature",
            "geometry": feature["geometry"],
            "properties": feature["attributes"]
        }
        features.append(feature_geojson)

    feature_collection = {
        "type": "FeatureCollection",
        "features": features
    }

    # Write GeoJSON data to a file with UTF-8 encoding
    with open("data_2015-01-01_to_2015-01-29.geojson", "w", encoding="utf-8") as outfile:
        json.dump(feature_collection, outfile, ensure_ascii=False)

    print("GeoJSON file created successfully.")
else:
    print("Error:", response.status_code)
