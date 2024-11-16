from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statements
)
"""
from data_consolidation import (
    create_consolidate_tables,
    # consolidate_city_data,
    consolidate_station_data,
<<<<<<< HEAD
    consolidate_station_statement_data
)
||||||| f65fa32
    consolidate_station_statement_data
)"""
=======
    #consolidate_station_statement_data
)
>>>>>>> b4c1c9c880293fccd717e05896be02b97c557383
from data_ingestion import (
    get_paris_realtime_bicycle_data,
    get_nantes_realtime_bicycle_data,
    get_nantes_communes_data
)


def main():
    print("Process start.")
    # data ingestion

    print("Data ingestion started.")
    get_paris_realtime_bicycle_data()
    get_nantes_realtime_bicycle_data()
    get_nantes_communes_data()
    print("Data ingestion ended.")

    
    # data consolidation
    print("Consolidation data started.")
    create_consolidate_tables()
<<<<<<< HEAD
    consolidate_city_data("paris")
    consolidate_city_data("nantes")
    """consolidate_station_data()
||||||| f65fa32
    consolidate_city_data()
    consolidate_station_data()
=======
    """
    consolidate_city_data()
    """
    consolidate_station_data("paris")
    consolidate_station_data("nantes")
    print("Consolidation data ended.")

    """
>>>>>>> b4c1c9c880293fccd717e05896be02b97c557383
    consolidate_station_statement_data()
    print("Consolidation data ended.")"""

    """# data agregation
    print("Agregate data started.")
    create_agregate_tables()
    agregate_dim_city()
    agregate_dim_station()
    agregate_fact_station_statements()
    print("Agregate data ended.")"""

if __name__ == "__main__":
    main()