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


###################################################################################

def consolidate_station_data(city):

    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Load city-specific data
    try:
        with open(f"data/raw_data/{today_date}/{city}_realtime_bicycle_data.json") as fd:
            data = json.load(fd)
    except FileNotFoundError:
        print(f"Data file for {city} not found")
        return

    raw_data_df = pd.json_normalize(data)

    if city.lower() == "paris":
        # Paris-specific processing
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

        # Insert into DuckDB
        con.register("paris_station_data_df", paris_station_data_df)
        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")

    else:
        # Nantes and Toulouse processing
        if "number" not in raw_data_df.columns:
            print(f"Erreur : La colonne 'number' est manquante dans les données de {city.capitalize()}")
            return

        # Retrieve city_code dynamically from CONSOLIDATE_CITY
        city_code_query = f"""
        SELECT id as city_code FROM CONSOLIDATE_CITY WHERE LOWER(name) = '{city.lower()}'
        """
        city_code = con.execute(city_code_query).fetchone()
        if not city_code:
            print(f"City code for {city.capitalize()} not found in CONSOLIDATE_CITY table")
            return
        city_code = city_code[0]        
        raw_data_df["city_code"] = city_code
        if city=='nantes':
            raw_data_df["id"] = raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
        elif city=='toulouse':
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

        # Insert into DuckDB
        con.register("station_data_df", station_data_df)
        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")

    con.close()

############################################################


def consolidate_city_data(city):

    # Connect to DuckDB
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    # Load all communes data for population lookup
    with open("data/raw_data/2024-11-27/all_communes_data.json") as fd:
        all_communes_data = json.load(fd)
    communes_df = pd.json_normalize(all_communes_data)
    
    # Ensure population lookup DataFrame has required columns
    population_lookup = communes_df[["code", "population", "nom"]].rename(columns={
        "code": "id",
        "population": "nb_inhabitants",
        "nom": "commune_name"
    })

    # Dynamically retrieve the ID for the city
    # Normalize and find the exact match for the city name
    city_id_row = population_lookup[population_lookup["commune_name"].str.strip().str.casefold() == city.strip().casefold()]


    # Retrieve the city ID from the matching row
    city_id = city_id_row.iloc[0]["id"]


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
    else:
        raw_data_df["id"] = city_id
        city_data_df = raw_data_df[[
            "id",
            "contract_name"
        ]].copy()
        city_data_df.rename(columns={
            "contract_name": "name"
        }, inplace=True)

    # Join with population data
    city_data_df = city_data_df.merge(population_lookup[["id", "nb_inhabitants"]], on="id", how="left")

    # Fill missing population with 0 or a default value if necessary
    city_data_df["nb_inhabitants"].fillna(0, inplace=True)

    # Common processing
    city_data_df.drop_duplicates(inplace=True)
    city_data_df["created_date"] = date.today().strftime("%Y-%m-%d")

    # Ensure final columns match the target table schema
    city_data_df = city_data_df[["id", "name", "nb_inhabitants", "created_date"]]

    # Register the DataFrame with DuckDB
    con.register("city_data_df", city_data_df)


    # Insert into the database
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")
    con.close()


#########################################


def consolidate_station_statement_data(city):

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
