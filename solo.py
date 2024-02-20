import requests
import json

# Define the URL
url = "https://gis.kgp.kz/arcgis/rest/services/KPSSU/DTP/FeatureServer/0/query"

# Define the query parameters
params = {
    'f': 'json',
    'geometry': '{"xmin":7514065.628541023,"ymin":5322463.153554989,"xmax":7827151.696397021,"ymax":5635549.22141099}',
    'orderByFields': 'objectid',
    'outFields': 'fd1r08p1,objectid,rta_date',
    'outSR': 102100,
    'quantizationParameters': '{"extent":{"spatialReference":{"latestWkid":3857,"wkid":102100},"xmin":7514065.628541023,"ymin":5009377.085698988,"xmax":8140237.764253024,"ymax":5635549.22141099},"mode":"view","originPosition":"upperLeft","tolerance":1222.992452562501}',
    'resultType': 'tile',
    'returnExceededLimitFeatures': False,
    'spatialRel': 'esriSpatialRelIntersects',
    'where': '1=1',
    'geometryType': 'esriGeometryEnvelope',
    'inSR': 102100
}

# Send the request
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    # Parse JSON response
    data = response.json()
    
    # Save data to a JSON file with UTF-8 encoding
    with open('traffic_accidents_data.json', 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False)
        
    print("Data saved to traffic_accidents_data.json")
else:
    print("Failed to retrieve data:", response.text)
