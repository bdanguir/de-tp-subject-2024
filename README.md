# Compte-rendu du projet : "Introduction à la data ingénierie"

**Réalisé par : Basma Danguir et Mohamed En-naciri**

Dans ce projet, nous avons étendu le pipeline ETL initial, qui était limité aux données des bornes de vélos en temps réel de la ville de Paris, en y intégrant également les données des villes de Nantes et de Toulouse. De plus, nous avons enrichi ces données avec des informations descriptives sur les villes de France, récupérées via une API open-source proposée par l'État français. Cette extension permet d'élargir la portée géographique du pipeline et d'offrir une vue plus complète des données d'utilisation des bornes de vélos dans plusieurs grandes villes françaises.

## Comment faire fonctionner ce projet?

Le dépôt initial, contenant Paris uniquement, a été conservé dans la branche **main**, et tous les développements que nous avons effectués sont sur la branche **develop**. Donc pour récupérer le bon dépôt, il faut :

```python
git clone https://github.com/bdanguir/de-tp-subject-2024

cd de-tp-subject-2024

git checkout develop

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt

python src/main.py
```

### Ingestion des données

Comme pour Paris, nous avons récupéré les données depuis les API open-source des villes de Nantes, Toulouse et des communes de France.

### Consolidation des données

L'approche pour Nantes et Toulouse est similaire à celle de Paris. Cependant, comme nous n'avons pas le nombre d'habitants directement dans les données pour ces deux villes, dans la fonction consolidate_city_data, nous avons récupéré cette information depuis les données des communes en effectuant un **merge**. Ce merge a permis de croiser l'identifiant unique de la ville avec celui des communes pour intégrer la population correspondante dans les données consolidées. Si aucune correspondance n'était trouvée, la population a été initialisée à zéro pour garantir la cohérence des données.

Aussi, pour contourner l'absence du code INSEE dans les données de Nantes et Toulouse, nous avons récupéré cet identifiant directement depuis la table CONSOLIDATE_CITY. Cette table contient les informations des communes, y compris leur code INSEE, que nous avons consolidées à partir des données de l'API des communes françaises.

Voici comment cela a été fait :

1- **Requête SQL dynamique** : Nous avons utilisé une requête SQL pour chercher le id (code INSEE) correspondant au nom de la ville dans CONSOLIDATE_CITY :

        ```sql
        
        SELECT id FROM CONSOLIDATE_CITY WHERE LOWER(city_name) = '{city.lower()}';
        ```

2- **Association du code INSEE aux données des stations** : Une fois le code récupéré, il a été ajouté dans les données des stations en tant que city_code. Ce champ a ensuite été utilisé pour identifier les stations de manière unique et cohérente.

### Agrégation des données

Nous n'avons apporté aucune modification à cette partie. Le code reste le même et fonctionne sans problème, même après l'ajout des données des autres villes.

### Le fichier main.py

Le fichier `main.py` contient le code principal du processus et exécute séquentiellement les différentes fonctions expliquées plus haut.

### Requêtes sql à exécuter

Enfin, les requêtes sql que nous avions à exécuter sont :

```sql
-- Nb d'emplacements disponibles de vélos dans une ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');

-- Nb de vélos disponibles en moyenne dans chaque station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```

Voici ce que retourne notre code :

### Allons plus loin : Construction du pipeline sur Azure

#### Architecture du projet

![Architecture du pipeline](images/AzurePipeline.png)

### Services Azure Utilisés

#### 1. **Azure Data Factory**
- **Rôle :** Orchestration et ingestion des données.
- **Utilisation :**
  - Récupération des données depuis l'API de Paris et leur stockage dans le layer Bronze d'Azure Data Lake.
  - Automatisation quotidienne grâce à un trigger basé sur la date.

#### 2. **Azure Data Lake Storage Gen2**
- **Rôle :** Stockage des données à chaque étape (Bronze, Silver, Gold).
- **Organisation :**
  - **Bronze :** Données brutes extraites des APIs.
  - **Silver :** Données nettoyées, transformées et organisées (par ville, disponibilité, station).
  - **Gold :** Données enrichies et agrégées, prêtes pour des analyses avancées.

#### 3. **Azure Databricks**
- **Rôle :** Transformation des données entre les couches (Bronze -> Silver, Silver -> Gold).
- **Utilisation :**
  - Nettoyage des données.
  - Structuration des données en dossiers logiques.
  - Enrichissement avec des informations supplémentaires comme des horodatages et des formats de données.

#### 4. **Azure Synapse Analytics (optionnel)**
- **Rôle :** Analyse SQL sur les données enrichies.
- **Utilisation :** Permet des requêtes rapides sur les données Gold.

#### 5. **Azure Key Vault**
- **Rôle :** Sécurisation des secrets et credentials pour accéder aux services et APIs.

---

## Étapes Suivies

### 1. **Ingestion des Données**
- **Action :**
  - Création d'un pipeline dans Azure Data Factory pour appeler l'API de Paris.
  - Sauvegarde des données sous forme de fichiers Parquet dans la couche Bronze d'Azure Data Lake.

![Azure Data factory Pipeline , la premiere activité consiste en l'ingestion](images/AzureDFPipeline.png)
![Bronze  layer content](images/BronzeContent.png)
![Dedans une date , on trouve le parquet file (raw data) ](images/ParquetFile.png)

### 2. Transformation Bronze -> Silver

- **Action :**

- Utilisation de notebooks Databricks pour nettoyer et structurer les données.
- Organisation des données dans trois sous-dossiers : `availability`, `city`, `station`.
- Chaque sous-dossier est partitionné par date pour conserver l'historique.

![Interface Databricks contenant nos notebooks](images/Databricks.png)


- **Mounting Azure Data Lake Storage dans Databricks**

Avant de commencer les transformations, il est nécessaire de **monter** les conteneurs d'Azure Data Lake Storage (ADLS) dans Databricks. Cela permet un accès facile aux données directement via les points de montage `/mnt/bronze`, `/mnt/silver`, et `/mnt/gold`.

- **Qu'est-ce que le mounting ?**

Monter un conteneur ADLS signifie créer un lien entre Databricks et les conteneurs de stockage Azure. Cela facilite la lecture et l'écriture des fichiers directement dans ADLS en utilisant des chemins simplifiés.

- **À noter :**

- Cette opération est effectuée **une seule fois** lors de la configuration initiale.
- Une fois montés, les conteneurs restent accessibles pour tous les notebooks Databricks.

- **Code utilisé pour le mounting :**

```python
# Configuration pour se connecter à Azure Data Lake
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "32ca1c75-31d6-4a94-bad6-f0de42f8ee02",  # Application (client) ID
  "fs.azure.account.oauth2.client.secret": ".oS8Q~zYjTHv~oN5Hs2mMTy1hNNbSYsjkY4Pca83",  # Client secret
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d40d5b6c-caf9-4d8f-8bf5-6e5da3f9fb5a/oauth2/token"  # Directory (tenant) ID
}

# Monter le conteneur "bronze"
dbutils.fs.mount(
  source = "abfss://bronze@mystorageaccount2312.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs
)

# Monter le conteneur "silver"
dbutils.fs.mount(
  source = "abfss://silver@mystorageaccount2312.dfs.core.windows.net/",
  mount_point = "/mnt/silver",
  extra_configs = configs
)

# Monter le conteneur "gold"
dbutils.fs.mount(
  source = "abfss://gold@mystorageaccount2312.dfs.core.windows.net/",
  mount_point = "/mnt/gold",
  extra_configs = configs
)
```


- **Pour la consolidation des données de stations voici le contenu du notebook Bronze_to_silver_station**

## Pour la consolidation des données de stations, voici le contenu du notebook `Bronze_to_silver_station

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, when, concat
from datetime import datetime
import pytz

# Initialize Spark session
spark = SparkSession.builder.appName("StationDataTransformation").getOrCreate()

# Define paths and today's date
bronze_base_path = "/mnt/bronze"
silver_base_path = "/mnt/silver/stationdata"
# Set the timezone to Paris
paris_timezone = pytz.timezone("Europe/Paris")

# Get the current date and time in the Paris timezone
paris_time = datetime.now(paris_timezone)

# Format the date
today_date = paris_time.strftime("%Y-%m-%d")
# Define paths for today
today_bronze_folder = f"{bronze_base_path}/{today_date}"
silver_output_path = f"{silver_base_path}/{today_date}_station.parquet"

# Define Paris city code
PARIS_CITY_CODE = "1"

try:
    # Load today's data from the Bronze layer
    station_data = spark.read.parquet(today_bronze_folder)
    print(f"Successfully loaded data from {today_bronze_folder}")

    # Select relevant columns and perform transformations
    transformed_data = station_data.select(
        col("stationcode").alias("code"),
        col("name").alias("name"),
        col("nom_arrondissement_communes").alias("city_name"),
        col("code_insee_commune").alias("city_code"),
        col("capacity").alias("capacity"),
        to_date(col("duedate")).alias("updated_date"),  # Rename duedate to updated_date
        lit(today_date).alias("created_date"),  # Add today's date as created_date
        when(col("is_installed") == "OUI", 1).otherwise(0).alias("status"),  # Encode status as 1/0
        when(col("is_renting") == "OUI", 1).otherwise(0).alias("is_renting"),  # Encode is_renting as 1/0
        when(col("is_returning") == "OUI", 1).otherwise(0).alias("is_returning"),  # Encode is_returning as 1/0
    ).withColumn(
        "id", concat(lit(PARIS_CITY_CODE), lit("-"), col("code"))  # Add unique ID combining Paris city code and station code
    )

    # Reorder columns to move 'id' to the beginning
    reordered_columns = [
        "id", "code", "name", "city_name", "city_code", 
        "capacity", "updated_date", "created_date", "status", "is_renting", "is_returning"
    ]
    transformed_data = transformed_data.select(*reordered_columns)

    # Write the transformed data to the Silver layer
    transformed_data.write.mode("overwrite").parquet(silver_output_path)
    print(f"Data successfully saved to {silver_output_path}")

    # Load and display the transformed data from Silver
    print("Loading data from Silver layer for verification...")
    silver_data = spark.read.parquet(silver_output_path)
    print(f"Successfully loaded data from {silver_output_path}")
    
    # Display the data
    display(silver_data)

except Exception as e:
    print(f"Error processing station data: {e}")
    raise
```


### Explication

Ce code Python utilise **PySpark** pour effectuer les étapes suivantes :

#### Initialisation :
- Une session Spark est créée pour gérer le traitement des données.
- Les chemins pour les couches Bronze et Silver sont définis.

####  Chargement des données :
- Les données du jour (par date) sont récupérées depuis la couche Bronze sous forme de fichiers Parquet.

#### Transformation des données :
- Les colonnes sont renommées et transformées pour répondre aux besoins métiers :
  - Exemple : Le champ `is_installed` est encodé en binaire (1 pour "OUI", 0 sinon).
- Une colonne unique `id` est créée en combinant le code de la ville de Paris avec le code de la station.

#### Écriture dans la couche Silver :
- Les données transformées sont enregistrées sous forme de fichiers Parquet dans la couche Silver, organisées par date.

#### Chargement et vérification :
- Les données écrites dans Silver sont rechargées pour vérifier leur intégrité.


- **Pour la consolidation des données de stations voici le contenu du notebook Bronze_to_silver_availability**

```python

from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, concat, round
import pytz

# Initialize Spark session
spark = SparkSession.builder.appName("ConsolidateAvailabilityData").getOrCreate()

# Define paths
bronze_base_path = "/mnt/bronze"
silver_base_path = "/mnt/silver"
# Set the timezone to Paris
paris_timezone = pytz.timezone("Europe/Paris")

# Get the current date and time in the Paris timezone
paris_time = datetime.now(paris_timezone)

# Format the date
today_date = paris_time.strftime("%Y-%m-%d")
bronze_folder_path = f"{bronze_base_path}/{today_date}"
silver_availability_path = f"{silver_base_path}/availability/{today_date}_availability.parquet"

# Load today's data from the bronze layer
try:
    availability_data = spark.read.parquet(bronze_folder_path)
    print(f"Successfully loaded data from: {bronze_folder_path}")
except Exception as e:
    print(f"Error loading data from: {bronze_folder_path}")
    raise e

# Process and transform the data
availability_data = (
    availability_data
    .withColumn("id", concat(lit("1_"), col("stationcode")))  # Add unique ID as "1_Stationcode"
    .withColumnRenamed("numdocksavailable", "bicycle_docks_available")
    .withColumnRenamed("numbikesavailable", "bicycle_available")
    .withColumn("last_statement_date", to_date(col("duedate")))  # Convert duedate to a proper date
    .withColumn("created_date", lit(today_date))  # Add the created_date column with today's date
    .select(
        "id",
        "capacity",
        "bicycle_docks_available",
        "bicycle_available",
        "last_statement_date",
        "created_date"
    )  # Select relevant columns
)

# Write the transformed data to the silver layer
try:
    availability_data.write.mode("overwrite").parquet(silver_availability_path)
    print(f"Data successfully written to: {silver_availability_path}")
except Exception as e:
    print(f"Error writing data to: {silver_availability_path}")
    raise e

# Display the processed data for verification
display(availability_data)
```

### Explication

Ce code Python utilise **PySpark** pour transformer les données de disponibilité (availability) de la couche Bronze vers la couche Silver. Voici les étapes principales :

#### Initialisation :
- Une session Spark est créée pour le traitement des données.
- Les chemins pour les couches Bronze (`bronze_base_path`) et Silver (`silver_base_path`) sont définis.
- Le fuseau horaire est configuré pour Paris, et la date actuelle est obtenue pour créer des partitions et nommer les fichiers.

#### Chargement des données :
- Les données du jour sont récupérées depuis la couche Bronze en lisant les fichiers Parquet.
- Une vérification est effectuée pour s'assurer que les données sont chargées correctement.

#### Transformation des données :
- Ajout d'une colonne `id` unique basée sur le code de la station (`stationcode`), préfixée par `1_`.
- Renommage des colonnes pour des noms plus explicites :
  - `numdocksavailable` devient `bicycle_docks_available`.
  - `numbikesavailable` devient `bicycle_available`.
- Conversion de la colonne `duedate` en un format de date approprié (`last_statement_date`).
- Ajout d'une colonne `created_date` contenant la date du jour.
- Sélection des colonnes pertinentes pour simplifier la structure des données :
  - `id`, `capacity`, `bicycle_docks_available`, `bicycle_available`, `last_statement_date`, et `created_date`.

#### Écriture des données dans la couche Silver :
- Les données transformées sont enregistrées dans la couche Silver sous forme de fichiers Parquet.
- La structure des dossiers dans Silver inclut le répertoire `availability`, avec un fichier nommé selon la date du jour (par exemple, `2024-12-13_availability.parquet`).

#### Vérification :
- Les données transformées sont affichées dans Databricks pour une vérification manuelle de leur intégrité.

![Silver Folders](images/SilverFolders.png)
![Silver SubFolders](images/SilverSubfolders.png)


### 3. **Transformation Silver -> Gold**
- **Action :**
  - Application de transformations supplémentaires dans Databricks pour enrichir les données.
  - Organisation en dossiers Gold prêts pour l'analyse.
- **Recommandation de capture d'écran :**
  - Vue du dossier `gold` montrant les répertoires enrichis.

### 4. **Stockage et Historisation**
- **Action :**
  - Historisation des données à chaque couche (Bronze, Silver, Gold) pour permettre des analyses rétrospectives.
  - Vérification des partitions journalières.

---

## Résultat Final

- Un pipeline automatisé, scalable et sécurisé qui permet de collecter, nettoyer, transformer et enrichir les données.
- Les données sont organisées en plusieurs couches logiques pour répondre aux besoins métiers et faciliter les analyses.

---

## Recommandations pour Captures d'Écran

1. **Pipeline Azure Data Factory :**
   - Capture du pipeline complet avec les étapes de copie et transformation.
   - Capture des paramètres de l'API dans le pipeline.

2. **Contenu des Layers dans Azure Data Lake :**
   - Capture du contenu du dossier `bronze` (avec les fichiers partitionnés par date).
   - Capture des dossiers `silver` (`availability`, `city`, `station`).
   - Capture du dossier `gold` organisé.

3. **Notebooks Databricks :**
   - Capture des cellules importantes montrant les transformations Bronze -> Silver et Silver -> Gold.

4. **Azure Synapse Analytics (optionnel) :**
   - Capture des tables ou requêtes utilisées pour les analyses si applicable.

---

Ajoutez les captures recommandées et dites-moi si vous souhaitez que je développe davantage certains points ou clarifie certaines parties !

