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


def get_nantes_communes_data():
    """
    Fonction pour ingérer les données des communes de Nantes depuis l'API `geo.api.gouv.fr`
    et les sauvegarder dans un fichier JSON.
    """
    url = "https://geo.api.gouv.fr/communes?codeDepartement=44&fields=nom,code,population,codeDepartement&format=json"
    
    # Effectuer la requête HTTP GET
    response = requests.get(url)
    
    # Vérifier si la requête a réussi
    if response.status_code == 200:
        print("Données des communes de Nantes récupérées avec succès.")
        
        # Nommer le fichier avec la date du jour
        today_date = datetime.now().strftime("%Y-%m-%d")
        file_path = f"data/raw_data/{today_date}/nantes_communes_data.json"
        
        # Créer le répertoire s'il n'existe pas
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Sauvegarder les données dans un fichier JSON
        with open(file_path, "w") as file:
            file.write(response.text)
        
        print(f"Données sauvegardées dans {file_path}")
    else:
        print(f"Erreur lors de la récupération des données : {response.status_code}")
