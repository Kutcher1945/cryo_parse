import requests
import json

# Define the API endpoint
url = "https://gis.kgp.kz/server/rest/services/KPSSU/DTP/FeatureServer/0/query"

# Define query parameters
params = {
    "f": "json",
    'returnIdsOnly': False,
    'returnGeometry': True,
    "spatialRel": "esriSpatialRelIntersects",
    "outFields": "*",  
    "outSR": "4326",
    "where": "fd1r06p3='1975' AND rta_date>=timestamp '2024-01-01 18:00:00' AND rta_date<=timestamp '2024-01-24 17:59:59'",
}

# Send a GET request to the API
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    
    # Check if the response contains an error
    if "error" in data:
        print("Error:", data["error"]["message"])
    else:
        # Write JSON data to a file with UTF-8 encoding
        with open("traffic_accidents_filtered.json", "w", encoding="utf-8") as outfile:
            json.dump(data, outfile, ensure_ascii=False)
        print("JSON file created successfully.")
else:
    print("Error:", response.status_code)
