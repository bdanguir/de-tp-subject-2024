
from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statements
)

from data_consolidation import (
    create_consolidate_tables,
    consolidate_city_data,
    consolidate_station_data,
    consolidate_station_statement_data
)
from data_ingestion import (
    get_paris_realtime_bicycle_data,
    get_nantes_realtime_bicycle_data,
    get_all_communes_data,
    get_toulouse_realtime_bicycle_data
)


def main():
    print("Process start.")

    # data ingestion
    print("Data ingestion started.")
    get_paris_realtime_bicycle_data()
    get_nantes_realtime_bicycle_data()
    get_toulouse_realtime_bicycle_data()
    get_all_communes_data()
    print("Data ingestion ended.")

    # data consolidation
    print("Consolidation data started.")
    create_consolidate_tables()
    consolidate_city_data("paris")
    consolidate_city_data("nantes")
    consolidate_city_data("toulouse")
    consolidate_station_data("paris")
    consolidate_station_data("nantes")
    consolidate_station_data("toulouse")
    consolidate_station_statement_data("paris")
    consolidate_station_statement_data("nantes")
    consolidate_station_statement_data("toulouse")
    print("Consolidation data ended.")

    # data agregation
    print("Agregate data started.")
    create_agregate_tables()
    agregate_dim_city()
    agregate_dim_station()
    agregate_fact_station_statements()
    print("Agregate data ended.")
    

if __name__ == "__main__":
    main()