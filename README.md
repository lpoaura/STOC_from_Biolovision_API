# Module GeoNature gn_pr_stoc.

Centralisation des données issues des protocole [STOC Point d'écoute (EPS)](http://www.vigienature.fr/fr/suivi-temporel-des-oiseaux-communs-stoc) dans une base de données [GeoNature](https://github.com/PnX-SI/GeoNature) sur un serveur PostgreSQL/PostGIS.

Les données sont automatiquement collectées via l'API des portails VisioNature à l'aide de l'outil [Client-API-VN](https://framagit.org/lpo/Client_API_VN) développé par @dthonon.

L'intégration des données s'effectue en deux étapes:
* Intégration des données VisioNature au format JSON issues de l'API dans un schéma de réception.
* Peuplement automatique des données STOC (relevés et observations) à l'aide de triggers déclenchés sur les tables de réception des données JSON.

Les données de formulaires reçues de l'API sont structurées de la sorte:

```json
{
    "habitat": {
        "hp1": "A0_A",
        "hp2": "A1_2",
        "hs1": "B0_B",
        "hs2": "B1_1",
        "hp3A": "A2_1",
        "hp3B": "A2_9",
        "hp4A": "A3_8",
        "hs3A": "B2_1",
        "hs4A": "B3_2"
    },
    "advanced": "0",
    "site_code": "71071",
    "stoc_rain": "NO_RAIN",
    "stoc_snow": "NO_SNOW",
    "stoc_wind": "NO_WIND",
    "stoc_cloud": "ONE_THIRD",
    "visit_number": "3",
    "protocol_name": "STOC_EPS",
    "local_site_code": "",
    "sequence_number": "10",
    "stoc_visibility": "GOOD_VISIBILITY"
}
{
    "habitat": {
        "hp1": "B0_B",
        "hp2": "B1_1",
        "hs1": "A0_A",
        "hs2": "A1_3",
        "hp3A": "B2_3",
        "hp4A": "B3_1",
        "hs3B": "A2_1"
    },
    "advanced": "0",
    "site_code": "71071",
    "stoc_rain": "NO_RAIN",
    "stoc_snow": "NO_SNOW",
    "stoc_wind": "NO_WIND",
    "stoc_cloud": "ONE_THIRD",
    "visit_number": "3",
    "protocol_name": "STOC_EPS",
    "local_site_code": "",
    "sequence_number": "9",
    "stoc_visibility": "GOOD_VISIBILITY"
}
```
