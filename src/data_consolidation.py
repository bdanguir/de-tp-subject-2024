import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)


def consolidate_station_data(city):
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Charger les données pour Paris et Nantes
    data = {city: json.load(open(f"data/raw_data/{today_date}/{city}_realtime_bicycle_data.json")) 
            for city in ["paris", "nantes","toulouse"]}

    if city.lower() == "paris":
        # Charger les données pour Paris
        paris_raw_data_df = pd.json_normalize(data["paris"])

        if "stationcode" not in paris_raw_data_df.columns:
            print("Erreur : La colonne 'stationcode' est manquante dans les données de Paris")
            return

        # Ajouter des colonnes et transformer les données
        paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].astype(str).apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
        paris_raw_data_df["address"] = None
        paris_raw_data_df["created_date"] = date.today()

        # Créer un nouveau DataFrame pour éviter le SettingWithCopyWarning
        paris_station_data_df = paris_raw_data_df[[
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

        # Renommer les colonnes
        paris_station_data_df.rename(columns={
            "stationcode": "code",
            "name": "name",
            "coordonnees_geo.lon": "longitude",
            "coordonnees_geo.lat": "latitude",
            "is_installed": "status",
            "nom_arrondissement_communes": "city_name",
            "code_insee_commune": "city_code"
        }, inplace=True)

        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")

    elif city.lower() == "nantes":
        # Charger les données pour Nantes
        nantes_raw_data_df = pd.json_normalize(data["nantes"])

        if "number" not in nantes_raw_data_df.columns:
            print("Erreur : La colonne 'number' est manquante dans les données de Nantes")
            return

        # Convertir la colonne 'number' en chaîne de caractères
        nantes_raw_data_df["number"] = nantes_raw_data_df["number"].astype(str)
        nantes_raw_data_df["id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
        nantes_raw_data_df["created_date"] = date.today()
        nantes_raw_data_df["city_code"] = "44109"

        # Créer un nouveau DataFrame pour éviter le SettingWithCopyWarning
        nantes_station_data_df = nantes_raw_data_df[[
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

        nantes_station_data_df.rename(columns={
            "number": "code",
            "name": "name",
            "position.lon": "longitude",
            "position.lat": "latitude",
            "status": "status",
            "contract_name": "city_name",
            "city_code": "city_code"
        }, inplace=True)

        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM nantes_station_data_df;")
    
    elif city.lower() == "toulouse":
        # Charger les données pour Toulouse
        toulouse_raw_data_df = pd.json_normalize(data["toulouse"])

        if "number" not in toulouse_raw_data_df.columns:
            print("Erreur : La colonne 'number' est manquante dans les données de Nantes")
            return

        # Convertir la colonne 'number' en chaîne de caractères
        toulouse_raw_data_df["number"] = toulouse_raw_data_df["number"].astype(str)
        toulouse_raw_data_df["id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
        toulouse_raw_data_df["created_date"] = date.today()
        toulouse_raw_data_df["city_code"] = "31555"

        # Créer un nouveau DataFrame pour éviter le SettingWithCopyWarning
        toulouse_station_data_df = toulouse_raw_data_df[[
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

        toulouse_station_data_df.rename(columns={
            "number": "code",
            "name": "name",
            "position.lon": "longitude",
            "position.lat": "latitude",
            "status": "status",
            "contract_name": "city_name",
            "city_code": "city_code"
        }, inplace=True)

        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM toulouse_station_data_df;")
    
    else:
        print("City not supported")


def consolidate_city_data(city):
    import json
    import os
    import pandas as pd
    import duckdb
    from datetime import date

    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    # Load all communes data for population lookup
    with open("data/raw_data/2024-11-21/all_communes_data.json") as fd:
        all_communes_data = json.load(fd)
    communes_df = pd.json_normalize(all_communes_data)
    
    # Ensure population lookup DataFrame has required columns
    population_lookup = communes_df[["code", "population"]].rename(columns={"code": "id", "population": "nb_inhabitants"})

    # Load city-specific data
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

    # Process data based on the city
    if city.lower() == "paris":
        city_data_df = raw_data_df[[
            "code_insee_commune",
            "nom_arrondissement_communes"
        ]].copy()
        city_data_df.rename(columns={
            "code_insee_commune": "id",
            "nom_arrondissement_communes": "name"
        }, inplace=True)
    elif city.lower() == "nantes":
        raw_data_df["id"] = "44109"
        city_data_df = raw_data_df[[
            "id",
            "contract_name"
        ]].copy()
        city_data_df.rename(columns={
            "contract_name": "name"
        }, inplace=True)
    elif city.lower() == "toulouse":
        raw_data_df["id"] = "31555"
        city_data_df = raw_data_df[[
            "id",
            "contract_name"
        ]].copy()
        city_data_df.rename(columns={
            "contract_name": "name"
        }, inplace=True)

    # Join with population data
    city_data_df = city_data_df.merge(population_lookup, on="id", how="left")

    # Fill missing population with 0 or a default value if necessary
    city_data_df["nb_inhabitants"].fillna(0, inplace=True)

    # Common processing
    city_data_df.drop_duplicates(inplace=True)
    city_data_df["created_date"] = date.today()

    # Register the DataFrame with DuckDB
    con.register("city_data_df", city_data_df)

    # Insert into the database
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")
    con.close()



def consolidate_station_statement_data(city):
    import json
    import pandas as pd
    import duckdb
    from datetime import datetime, date

    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    data = {}

    # Determine file path based on the city
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

    # Load the data
    with open(file_path) as fd:
        data = json.load(fd)

    # Normalize JSON data into a DataFrame
    raw_data_df = pd.json_normalize(data)

    # Add city-specific processing
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

    # Insert into the database
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_data_df;")
    con.close()
