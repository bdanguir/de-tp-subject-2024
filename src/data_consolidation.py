import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_station_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    
    # Consolidation logic for Paris Bicycle data
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["address"] = None
    paris_raw_data_df["created_date"] = date.today()

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
    ]]

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


def consolidate_city_data(city):
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    data = {}

    # Load the JSON data based on the city
    if city.lower() == "paris":
        file_path = f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json"
    elif city.lower() == "nantes":
        file_path = f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json"
    else:
        print("City not supported")
        return

    with open(file_path) as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    # Process data based on the city
    if city.lower() == "paris":
        raw_data_df["nb_inhabitants"] = None
        city_data_df = raw_data_df[[
            "code_insee_commune",
            "nom_arrondissement_communes",
            "nb_inhabitants"
        ]]
        city_data_df.rename(columns={
            "code_insee_commune": "id",
            "nom_arrondissement_communes": "name"
        }, inplace=True)

    elif city.lower() == "nantes":
        raw_data_df["nb_inhabitants"] = None
        raw_data_df["id"]="Unknown"
        city_data_df = raw_data_df[[
            "id",
            "contract_name",
            "nb_inhabitants"
        ]]
        city_data_df.rename(columns={
            "contract_name": "name"
        }, inplace=True)

    # Common processing
    city_data_df.drop_duplicates(inplace=True)
    city_data_df["created_date"] = date.today()


    # Insert into the database
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")
    con.close()


def consolidate_station_statement_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    # Consolidate station statement data for Paris
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["created_date"] = datetime.fromisoformat('2024-10-21')
    paris_station_statement_data_df = paris_raw_data_df[[
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "created_date"
    ]]
    
    paris_station_statement_data_df.rename(columns={
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date",
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;")
