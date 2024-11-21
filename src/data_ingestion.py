import os
from datetime import datetime

import requests

# Ingestion : Paris

def get_paris_realtime_bicycle_data():
    
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    
    response = requests.request("GET", url)
    
    serialize_data(response.text, "paris_realtime_bicycle_data.json")

def serialize_data(raw_json: str, file_name: str):

    today_date = datetime.now().strftime("%Y-%m-%d")
    
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)

# Ingestion : Nantes

def get_nantes_realtime_bicycle_data():
    
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json?lang=fr&timezone=Europe%2FBerlin"
    
    response = requests.request("GET", url)
    
    serialize_data(response.text, "nantes_realtime_bicycle_data.json")

def serialize_data(raw_json: str, file_name: str):

    today_date = datetime.now().strftime("%Y-%m-%d")
    
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)


def get_all_communes_data():
    """
    Function to fetch data for all communes in France from `geo.api.gouv.fr` 
    and save it into a JSON file.
    """
    # API URL for all communes
    url = "https://geo.api.gouv.fr/communes?fields=nom,code,population,codeDepartement&format=json"
    
    # Perform the HTTP GET request
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        print("All communes data retrieved successfully.")
        
        # Name the file with today's date
        today_date = datetime.now().strftime("%Y-%m-%d")
        file_path = f"data/raw_data/{today_date}/all_communes_data.json"
        
        # Create the directory if it does not exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Save the data into a JSON file
        with open(file_path, "w") as file:
            file.write(response.text)
        
        print(f"Data saved in {file_path}")
    else:
        print(f"Error while fetching data: {response.status_code}")

