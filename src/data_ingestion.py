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


# Ingestion : Toulouse

def get_toulouse_realtime_bicycle_data():
    
    url = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json?lang=fr&timezone=Europe%2FParis"
    
    response = requests.request("GET", url)
    
    serialize_data(response.text, "toulouse_realtime_bicycle_data.json")

def serialize_data(raw_json: str, file_name: str):

    today_date = datetime.now().strftime("%Y-%m-%d")
    
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)


# Ingestion : communes

def get_all_communes_data():
    """
    Fonction pour récupérer les données de toutes les communes de France depuis `geo.api.gouv.fr` 
    et les sauvegarder dans un fichier JSON.
    """
    url = "https://geo.api.gouv.fr/communes?fields=nom,code,population,codeDepartement&format=json"

    response = requests.get(url)
   
    if response.status_code == 200:
        print("All communes data retrieved successfully.")
        today_date = datetime.now().strftime("%Y-%m-%d")
        file_path = f"data/raw_data/{today_date}/all_communes_data.json"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            file.write(response.text)
        print(f"Data saved in {file_path}")
    else:
        print(f"Error while fetching data: {response.status_code}")