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

