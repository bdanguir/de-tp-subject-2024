import json
import pandas as pd

# Load the JSON file
file_path = "data/raw_data/2024-11-21/nantes_realtime_bicycle_data.json"
with open(file_path, 'r', encoding='utf-8') as file:
    data = json.load(file)

# Normalize the JSON data into a pandas DataFrame
df = pd.json_normalize(data)

# Extract unique contract names
unique_contract_names = df["contract_name"].unique()

# Print the unique contract names
print("Unique contract names:")
for name in unique_contract_names:
    print(name)
