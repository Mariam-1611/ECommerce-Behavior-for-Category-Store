# E-Commerce Big Data Analytics Pipeline

A big data pipeline for e-commerce customer behavior analysis, combining real-time
event streaming with batch analytics, feeding a unified Power BI dashboard.

## Overview

- **Dataset:** [eCommerce behavior data from multi-category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (Kaggle)
- **Goal:** demonstrate a production-style big data pipeline — ingest, process (both
  streaming and batch), orchestrate, and serve — using this dataset as the source.

## Architecture

Two parallel lanes, each with its own dashboard:

**Real-time lane:** `Kaggle CSV -> Replay Producer (Python) -> Kafka -> Spark Structured
Streaming -> shared JSON file -> Streamlit dashboard`

**Batch lane:** `Kaggle CSV -> HDFS raw -> Spark batch ETL -> HDFS curated (Parquet) ->
Power BI (import model)`

**Orchestration:** Apache Airflow schedules the batch pipeline runs and triggers the
Power BI dataset refresh; it also monitors the health of the always-on streaming job.

> Note: the real-time lane originally targeted a Power BI streaming dataset, but
> switched to a Streamlit dashboard reading a shared JSON-lines file
> (`data/streaming_kpis.jsonl`) written by the Spark job. The batch lane still
> targets Power BI import mode.

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
6. Install the Python dependencies for the ingestion/serving scripts:
   ```bash
   pip install -r requirements.txt
   ```
7. Run the streaming lane end to end (each in its own terminal):
   ```bash
   # Terminal 1 — Spark Structured Streaming job (leave running)
   docker exec -it --user root -e HOME=/root -e HADOOP_USER_NAME=spark spark-master \
     spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
     --conf spark.jars.ivy=/root/.ivy2 /opt/jobs/spark_streaming.py

   # Terminal 2 — producer (sends a batch, then exits)
   python jobs/producer.py --csv data/2019-Oct.csv --topic ecommerce-events --delay 0.05 --limit 3000

   # Terminal 3 — dashboard (leave running)
   streamlit run jobs/streamlit_dashboard.py
   ```

## Project stages

| Stage | Scope | Status |
|-------|-------|--------|
| 1. Infrastructure | Docker Compose stack for Kafka, HDFS, Spark, Airflow | Done |
| 2. Ingestion | Replay producer script, Kafka topic design | Done |
| 3. Processing (streaming) | Spark Structured Streaming job, writes windowed KPIs to shared file | Done |
| 3. Processing (batch) | Spark batch ETL job (HDFS raw -> curated Parquet) | In progress |
| 4. Orchestration | Airflow DAGs for batch runs and Power BI refresh | Not started |
| 5. Serving (streaming) | Streamlit dashboard reading `data/streaming_kpis.jsonl` | Done |
| 5. Serving (batch) | Power BI import model dashboard | Not started |

## Notes for contributors

- Add ingestion/processing scripts under `jobs/`, not at the repo root.
- Add Airflow DAG files under `dags/` — Airflow picks them up automatically.
- Do not commit the raw dataset or any API tokens/credentials — see `.gitignore`.
- Kafka is reachable at `localhost:9092` from the host machine, or `kafka:29092`
  from inside another container.
- The streaming job writes to `data/streaming_kpis.jsonl` — read this file fresh on
  each refresh (it's newline-delimited JSON, appended continuously) rather than
  trying to tail it incrementally.