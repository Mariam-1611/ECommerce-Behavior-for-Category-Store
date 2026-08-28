# E-commerce big data pipeline — infrastructure stage

This stack brings up every service the pipeline needs: Kafka, HDFS, Spark, and Airflow,
all networked together so they can talk to each other by container name.

## Folder structure expected

```
project-root/
├── docker-compose.yml
├── data/        # place the Kaggle CSV(s) here — mounted into Spark containers
├── jobs/        # Spark scripts (producer, streaming job, batch ETL) go here
└── dags/        # Airflow DAG files go here
```

Create the `data/`, `jobs/`, and `dags/` folders before starting the stack (they're
mounted as volumes, so Docker needs them to exist).

## Starting the stack

```bash
docker compose up -d
```

First run will take a while (pulling ~6 images). Check everything is healthy:

```bash
docker compose ps
```

## Service access points

| Service          | URL / address              | Notes                              |
|------------------|-----------------------------|-------------------------------------|
| Kafka broker     | `localhost:9092`            | use this from producer scripts on host |
| Kafka (in-container) | `kafka:29092`            | use this from Spark/Airflow containers |
| Kafka UI         | http://localhost:8085       | browse topics/messages visually     |
| HDFS NameNode UI | http://localhost:9870       | check HDFS health, browse files     |
| Spark master UI  | http://localhost:8090       | job status, worker status           |
| Airflow UI       | http://localhost:8080       | login: admin / admin                |

## Handoff notes for the team

- **Ingestion (producer + Kafka topic):** drop your script in `jobs/`, connect to
  `localhost:9092` if running on the host, or `kafka:29092` if running inside a container.
- **Processing (Spark streaming + batch):** submit jobs against `spark://spark-master:7077`.
  Scripts belong in `jobs/`, they're already mounted into both Spark containers.
- **Orchestration (Airflow DAGs):** drop DAG files in `dags/` — Airflow picks them up
  automatically within a minute or two, no restart needed.
- **HDFS paths:** the curated Parquet zone should live under `hdfs://namenode:9000/curated/`.

## Stopping / resetting

```bash
docker compose down          # stop everything, keep data
docker compose down -v       # stop and wipe all volumes (fresh start)
```
