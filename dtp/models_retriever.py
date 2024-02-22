import requests
import json

# URL of the ArcGIS REST API endpoint
url = "https://gis.kgp.kz/arcgis/rest/services/KPSSU/DTP/FeatureServer/1?f=json"

try:
    # Sending a GET request to fetch data
    response = requests.get(url)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse JSON response
        data = response.json()
        
        # Print the structure of the JSON response
        print(json.dumps(data, indent=4))
        
        # Extract features
        features = data.get("fields", [])
        
        # Create a dictionary to hold features for each modelName
        features_dict = {}
        
        # Iterate over features
        for feature in features:
            # Extract modelName
            modelName = feature.get("modelName")
            
            if modelName:
                # Check if the modelName key exists in the dictionary
                if modelName not in features_dict:
                    features_dict[modelName] = []
                
                # Append the whole feature to the list of features for the modelName
                features_dict[modelName].append(feature)
        
        # Iterate over features and save them into separate JSON files
        for modelName, feature_list in features_dict.items():
            with open(f"{modelName}_data.json", "w", encoding="utf-8") as json_file:
                json.dump(feature_list, json_file, indent=4, ensure_ascii=False)
                print(f"Data for {modelName} has been successfully written to '{modelName}_data.json' file.")
        
        print("All data has been processed.")
    else:
        print("Failed to retrieve data. Status code:", response.status_code)

except requests.exceptions.RequestException as e:
    print("Error fetching data:", e)
