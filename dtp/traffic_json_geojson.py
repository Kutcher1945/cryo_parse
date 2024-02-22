import requests
import json

url = "https://gis.kgp.kz/server/rest/services/KPSSU/DTP/FeatureServer/0/query"
params = {
    "f": "json",
    "where": "1=1",
    "returnGeometry": "true",
    "spatialRel": "esriSpatialRelIntersects",
    "outFields": "*",
    "outSR": "4326"
}

response = requests.get(url, params=params)
data = json.loads(response.content.decode('utf-8'))  # Manually decode response content using UTF-8

# Write JSON data to a file with UTF-8 encoding
with open("traffic_accidents.json", "w", encoding="utf-8") as outfile:
    json.dump(data, outfile, ensure_ascii=False)

print("JSON file created successfully.")


import requests
import json

url = "https://gis.kgp.kz/server/rest/services/KPSSU/DTP/FeatureServer/0/query"
params = {
    "f": "json",
    "where": "1=1",
    "returnGeometry": "true",
    "spatialRel": "esriSpatialRelIntersects",
    "outFields": "*",
    "outSR": "4326"
}

response = requests.get(url, params=params)
data = response.json()

# Extract relevant features from the response
features = data.get("features", [])

# Create a GeoJSON structure
geojson_data = {
    "type": "FeatureCollection",
    "features": features
}

# Write GeoJSON data to a file with UTF-8 encoding
with open("traffic_accidents.geojson", "w", encoding="utf-8") as outfile:
    json.dump(geojson_data, outfile, ensure_ascii=False)

print("GeoJSON file created successfully.")

