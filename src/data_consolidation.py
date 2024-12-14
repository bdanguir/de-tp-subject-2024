import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3


def create_consolidate_tables():
    """
    Cette fonction établit une connexion à une base de données DuckDB, lit un fichier contenant des requêtes SQL 
    pour créer et consolider des tables, puis exécute ces requêtes une par une.
    """
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)


#======================================================================================================================================================


def consolidate_station_data(city):
    """
    Cette fonction consolide les données des stations de vélos en temps réel pour une ville donnée.
    Elle traite les données spécifiques à chaque ville (Paris, Nantes ou Toulouse), 
    puis insère les résultats consolidés dans une base de données DuckDB.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    try:
        with open(f"data/raw_data/{today_date}/{city}_realtime_bicycle_data.json") as fd:
            data = json.load(fd)
    except FileNotFoundError:
        print(f"Data file for {city} not found")
        return

    raw_data_df = pd.json_normalize(data)

    if city.lower() == "paris":
        # Traitement spécifique pour Paris
        if "stationcode" not in raw_data_df.columns:
            print("Erreur : La colonne 'stationcode' est manquante dans les données de Paris")
            return

        raw_data_df["id"] = raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
        raw_data_df["address"] = None
        raw_data_df["created_date"] = date.today()

        paris_station_data_df = raw_data_df[[
            "id",
            "stationcode",
            "name",
            "nom_arrondissement_communes",
            "code_insee_commune",
            "address",
            "coordonnees_geo.lon",
            "coordonnees_geo.lat",
            "is_installed",
            "created_date",
            "capacity"
        ]].copy()

        paris_station_data_df.rename(columns={
            "stationcode": "code",
            "name": "name",
            "coordonnees_geo.lon": "longitude",
            "coordonnees_geo.lat": "latitude",
            "is_installed": "status",
            "nom_arrondissement_communes": "city_name",
            "code_insee_commune": "city_code"
        }, inplace=True)

        con.register("paris_station_data_df", paris_station_data_df)
        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")

    else:
        # Traitement spécifique pour Nantes et Toulouse
        if "number" not in raw_data_df.columns:
            print(f"Erreur : La colonne 'number' est manquante dans les données de {city.capitalize()}")
            return

        # Récupération dynamique du city_code depuis CONSOLIDATE_CITY
        city_code_query = f"""
        SELECT id as city_code FROM CONSOLIDATE_CITY WHERE LOWER(name) = '{city.lower()}'
        """
        city_code = con.execute(city_code_query).fetchone()
        if not city_code:
            print(f"City code for {city.capitalize()} not found in CONSOLIDATE_CITY table")
            return
        city_code = city_code[0]        
        raw_data_df["city_code"] = city_code

        if city == 'nantes':
            raw_data_df["id"] = raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
        elif city == 'toulouse':
            raw_data_df["id"] = raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")

        raw_data_df["created_date"] = date.today()

        station_data_df = raw_data_df[[
            "id",
            "number",
            "name",
            "contract_name",
            "city_code",
            "address",
            "position.lon",
            "position.lat",
            "status",
            "created_date",
            "available_bike_stands"
        ]].copy()

        station_data_df.rename(columns={
            "number": "code",
            "name": "name",
            "position.lon": "longitude",
            "position.lat": "latitude",
            "status": "status",
            "contract_name": "city_name",
            "city_code": "city_code"
        }, inplace=True)

        con.register("station_data_df", station_data_df)
        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")

    con.close()


#======================================================================================================================================================

def consolidate_city_data(city):
    """
    Cette fonction consolide les données d'une ville spécifique en utilisant des données démographiques
    et des données en temps réel, puis les insère dans la table CONSOLIDATE_CITY de la base de données DuckDB.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    # Charger toutes les données des communes pour la correspondance des populations
    with open(f"data/raw_data/{today_date}/all_communes_data.json") as fd:
        all_communes_data = json.load(fd)
    communes_df = pd.json_normalize(all_communes_data)
    
    population_lookup = communes_df[["code", "population", "nom"]].rename(columns={
        "code": "id",
        "population": "nb_inhabitants",
        "nom": "commune_name"
    })

    # Récupération dynamique de l'ID de la ville
    city_id_row = population_lookup[population_lookup["commune_name"].str.strip().str.casefold() == city.strip().casefold()]
    city_id = city_id_row.iloc[0]["id"]

    # Chargement des données spécifiques à la ville
    if city.lower() == "paris":
        file_path = f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json"
    elif city.lower() == "nantes":
        file_path = f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json"
    elif city.lower() == "toulouse":
        file_path = f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json"
    else:
        print("City not supported")
        return

    with open(file_path) as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    # Traitement des données selon la ville
    if city.lower() == "paris":
        city_data_df = raw_data_df[[
            "code_insee_commune",
            "nom_arrondissement_communes"
        ]].copy()
        city_data_df.rename(columns={
            "code_insee_commune": "id",
            "nom_arrondissement_communes": "name"
        }, inplace=True)
    else:
        raw_data_df["id"] = city_id
        city_data_df = raw_data_df[[
            "id",
            "contract_name"
        ]].copy()
        city_data_df.rename(columns={
            "contract_name": "name"
        }, inplace=True)

    # Jointure avec les données de population
    city_data_df = city_data_df.merge(population_lookup[["id", "nb_inhabitants"]], on="id", how="left")

    city_data_df["nb_inhabitants"].fillna(0, inplace=True)

    city_data_df.drop_duplicates(inplace=True)
    city_data_df["created_date"] = date.today().strftime("%Y-%m-%d")

    city_data_df = city_data_df[["id", "name", "nb_inhabitants", "created_date"]]

    con.register("city_data_df", city_data_df)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")
    con.close()


#======================================================================================================================================================

def consolidate_station_statement_data(city):
    """
    Cette fonction consolide les données des stations de vélos pour une ville spécifique
    et les insère dans la table CONSOLIDATE_STATION_STATEMENT de la base de données DuckDB.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    data = {}

    # Détermination du chemin du fichier et du code ville
    if city.lower() == "paris":
        file_path = f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json"
        city_code = PARIS_CITY_CODE
    elif city.lower() == "nantes":
        file_path = f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json"
        city_code = NANTES_CITY_CODE
    elif city.lower() == "toulouse":
        file_path = f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json"
        city_code = TOULOUSE_CITY_CODE
    else:
        print("City not supported")
        return

    with open(file_path) as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    # Traitement spécifique à chaque ville
    if city.lower() == "paris":
        raw_data_df["station_id"] = raw_data_df["stationcode"].apply(lambda x: f"{city_code}-{x}")
        raw_data_df["created_date"] = date.today()
        station_statement_data_df = raw_data_df[[
            "station_id",
            "numdocksavailable",
            "numbikesavailable",
            "duedate",
            "created_date"
        ]]
        station_statement_data_df.rename(columns={
            "numdocksavailable": "bicycle_docks_available",
            "numbikesavailable": "bicycle_available",
            "duedate": "last_statement_date",
        }, inplace=True)

    elif city.lower() == "nantes":
        raw_data_df["station_id"] = raw_data_df["number"].apply(lambda x: f"{city_code}-{x}")
        raw_data_df["created_date"] = date.today()
        station_statement_data_df = raw_data_df[[
            "station_id",
            "bike_stands",
            "available_bikes",
            "last_update",
            "created_date"
        ]]
        station_statement_data_df.rename(columns={
            "bike_stands": "bicycle_docks_available",
            "available_bikes": "bicycle_available",
            "last_update": "last_statement_date",
        }, inplace=True)

    elif city.lower() == "toulouse":
        raw_data_df["station_id"] = raw_data_df["number"].apply(lambda x: f"{city_code}-{x}")
        raw_data_df["created_date"] = date.today()
        station_statement_data_df = raw_data_df[[
            "station_id",
            "bike_stands",
            "available_bikes",
            "last_update",
            "created_date"
        ]]
        station_statement_data_df.rename(columns={
            "bike_stands": "bicycle_docks_available",
            "available_bikes": "bicycle_available",
            "last_update": "last_statement_date",
        }, inplace=True)

    station_statement_data_df.drop_duplicates(inplace=True)

    con.register("station_statement_data_df", station_statement_data_df)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_data_df;")
    con.close()

