# 🏎️ F1 Data Engineering Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-231F20.svg)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C.svg)](https://spark.apache.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE.svg)](https://airflow.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-316192.svg)](https://postgresql.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32-FF4B4B.svg)](https://streamlit.io)
[![Docker](https://img.shields.io/badge/Docker%20Compose-✓-2496ED.svg)](https://docker.com)

Projet Data Engineering complet démontrant la mise en place d'un pipeline de données
de bout en bout sur les données Formula 1 — de l'ingestion au dashboard analytique.

## Architecture

```
FastF1 (Python) → Kafka Producer → [Topics Kafka] → Spark ETL → PostgreSQL → Streamlit
                                        ↑
                              Airflow DAG (orchestration)
```

## Stack technique

| Couche | Technologie | Rôle |
|--------|-------------|------|
| Source | FastF1 3.3 | Récupération données F1 officielles |
| Streaming | Apache Kafka 7.5 | File de messages, découplage |
| Traitement | Apache Spark 3.5 | ETL, transformations, agrégations |
| Orchestration | Apache Airflow 2.8 | Scheduling, dépendances, monitoring |
| Stockage | PostgreSQL 15 | Schéma en étoile analytique |
| Visualisation | Streamlit 1.32 | Dashboard interactif |
| Infra | Docker Compose | Déploiement local reproductible |

## Données ingérées

- **Résultats de courses** : positions, points, statuts — saisons 2023/2024
- **Télémétrie** : vitesse, accélération, frein, DRS, RPM
- **Pit stops** : durée, stratégie pneus (compound, tyre life)
- **Météo** : température air/piste, humidité, pluie

## Lancement rapide

```bash
git clone https://github.com/ton-username/f1-data-pipeline
cd f1-data-pipeline

# Copie et configure les variables d'environnement
cp .env.example .env

# Lance tous les services
docker compose up --build -d

# Vérifie que tout est UP
docker compose ps
```

| Interface | URL |
|-----------|-----|
| Streamlit Dashboard | http://localhost:8501 |
| Airflow UI | http://localhost:8080 (admin/admin) |
| Spark Master UI | http://localhost:8081 |
| Kafka | localhost:9092 |
| PostgreSQL | localhost:5432 |

## Structure du projet

```
f1-data-pipeline/
├── docker-compose.yml     # Orchestration des 6 services
├── producer/              # FastF1 → Kafka (JSON)
├── spark/jobs/            # ETL : Kafka → PostgreSQL
├── airflow/dags/          # Orchestration batch hebdomadaire
├── postgres/init.sql      # Schéma en étoile (facts + dims)
└── streamlit/app.py       # Dashboard analytique
```

## Schéma de données

Modélisation en étoile (Kimball) :

```
dim_driver ──┐
dim_circuit ─┤──→ fact_race_result
dim_team ────┘
              ──→ fact_pit_stop
              ──→ fact_weather
```

## Points techniques notables

- **Pattern idempotent** : contraintes `UNIQUE` en base + `dropDuplicates` Spark → pipeline rejouable sans doublons
- **Deux listeners Kafka** : séparation réseau interne (conteneurs) / externe (host)
- **Cache FastF1** : évite le retéléchargement des données F1
- **XCom Airflow** : passage de contexte entre tâches (année, round GP)
- **Vues PostgreSQL** : logique analytique centralisée, Streamlit ne fait que lire
## Status
🚧 Active development — core pipeline operational, 
dashboard and orchestration in final testing


## Auteur

BENIHYA OMAR — [LinkedIn](https://www.linkedin.com/in/omarbenihya/) — [GitHub](https://github.com/ma3sttro10)