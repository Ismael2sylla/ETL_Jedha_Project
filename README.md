<<<<<<< HEAD
# 📦 SuperCourier - Mini ETL Pipeline

Ce projet est une simulation d’un pipeline *ETL pour l’entreprise fictive SuperCourier, dont l’objectif est de prédire les retards de livraison à partir de données logistiques, météorologiques et de suivi.

## Objectif

Créer un jeu de données propre et structuré, combinant des sources générés, et calculant le statut de livraison (`On-time` ou `Delayed`) selon plusieurs facteurs : type de colis, distance, zone de livraison, météo, heure/jour, etc.

## Fonctionnement

Le pipeline complet est contenu dans le fichier `de-code.py` et suit ces étapes :

1. **Génération de données** :
   - Création d'une base de données SQLite avec 1000 livraisons simulées
   - Génération de conditions météo aléatoires sur 90 jours (JSON)

2. **Extraction** :
   - Lecture des livraisons depuis SQLite
   - Chargement des conditions météo depuis le fichier JSON

3. **Transformation** :
   - Enrichissement des livraisons avec la météo
   - Calcul des durées de livraison réelles et théoriques
   - Détermination du statut de livraison (`Delayed` ou `On-time`)

4. **Chargement** :
   - Export final dans un fichier CSV `deliveries.csv`
   - Journalisation des étapes et statistiques globales


## Colonnes du CSV final

- `Delivery_ID`
- `Pickup_DateTime`
- `Weekday`
- `Hour`
- `Package_Type`
- `Distance` (km)
- `Delivery_Zone`
- `Weather_Condition`
- `Actual_Delivery_Time` (minutes)
- `Status` (`On-time` / `Delayed`)

## Exécution du pipeline

Dans un terminal :

```bash
python de-code.py



=======
# Projet_colis_jedha
>>>>>>> 4ecd5553b232cad2ef19d3a41408b4194599b14f
