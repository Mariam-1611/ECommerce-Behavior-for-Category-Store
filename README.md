# E-Commerce Big Data Analytics Pipeline

A big data pipeline for e-commerce customer behavior analysis, combining real-time
event streaming with batch analytics, feeding a unified Power BI dashboard.

## Overview

- **Dataset:** [eCommerce behavior data from multi-category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (Kaggle)
- **Goal:** demonstrate a production-style big data pipeline — ingest, process (both
  streaming and batch), orchestrate, and serve — using this dataset as the source.

## Architecture

Two parallel lanes converge into a single Power BI dashboard:

**Real-time lane:** `Kaggle CSV -> Replay Producer (Python) -> Kafka -> Spark Structured
Streaming -> Power BI (streaming dataset)`

**Batch lane:** `Kaggle CSV -> HDFS raw -> Spark batch ETL -> HDFS curated (Parquet) ->
Power BI (import model)`

**Orchestration:** Apache Airflow schedules the batch pipeline runs and triggers the
Power BI dataset refresh; it also monitors the health of the always-on streaming job.

## Repo structure

```
project-root/
├── docker-compose.yml   # brings up Kafka, HDFS, Spark, Airflow
├── README.md            # this file
├── docker/README.md     # service-level setup notes and port map
├── data/                 # local dataset folder (not committed — see .gitignore)
├── jobs/                 # producer script, Spark streaming job, Spark batch ETL job
└── dags/                 # Airflow DAG definitions
```

> Note: place `docker-setup-readme.md` (included alongside this file) into a `docker/`
> folder as `docker/README.md` once you set up the repo, to match the structure above.

## Getting started

1. Install Docker Desktop (or Docker Engine if working inside the Ubuntu VM).
2. Clone this repo and create the local data folder:
   ```bash
   git clone <repo-url>
   cd <repo-name>
   mkdir -p data
   ```
3. Download the dataset via the Kaggle CLI (see `docker/README.md` for full setup)
   and place the CSV(s) in `data/`.
4. Start the stack:
   ```bash
   docker compose up -d
   ```
5. Confirm services are up — see `docker/README.md` for the full port map and
   access URLs (Kafka UI, HDFS NameNode UI, Spark UI, Airflow UI).

## Project stages

| Stage | Scope | Status |
|-------|-------|--------|
| 1. Infrastructure | Docker Compose stack for Kafka, HDFS, Spark, Airflow | Done |
| 2. Ingestion | Replay producer script, Kafka topic design | Done |
| 3. Processing | Spark Structured Streaming job + Spark batch ETL job | Not started |
| 4. Orchestration | Airflow DAGs for batch runs and Power BI refresh | Not started |
| 5. Serving | Power BI streaming dataset + import model dashboard | Not started |

## Notes for contributors

- Add ingestion/processing scripts under `jobs/`, not at the repo root.
- Add Airflow DAG files under `dags/` — Airflow picks them up automatically.
- Do not commit the raw dataset or any API tokens/credentials — see `.gitignore`.
- Kafka is reachable at `localhost:9092` from the host machine, or `kafka:29092`
  from inside another container.