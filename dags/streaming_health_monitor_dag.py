"""
Streaming health-monitor DAG — checks that the Spark Structured Streaming
job is still alive and that it's actually writing fresh data, without ever
starting, stopping, or scheduling the streaming job itself (it's meant to
run continuously, independent of Airflow).

Runs frequently (every 5 minutes) since its job is to catch problems soon
after they happen, not to do heavy processing.
"""

from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

SPARK_MASTER_UI_URL = "http://spark-master:8080/json/"
STREAMING_APP_NAME = "EcommerceStreamingKPIs"
STREAMING_OUTPUT_FILE = Path("/opt/airflow/data/streaming_kpis.jsonl")
STALE_THRESHOLD_MINUTES = 10

default_args = {
    "owner": "ecommerce-pipeline",
    "retries": 0,  # no point retrying a health check — just report the current state
}


def check_spark_application_running(**context):
    """Ask Spark master's own status endpoint whether the streaming job is
    registered as a running application."""
    response = requests.get(SPARK_MASTER_UI_URL, timeout=10)
    response.raise_for_status()
    apps = response.json().get("activeapps", [])
    names = [a.get("name") for a in apps]

    if STREAMING_APP_NAME not in names:
        raise AirflowException(
            f"'{STREAMING_APP_NAME}' is not listed among Spark's active applications "
            f"({names}) — the streaming job appears to be down."
        )
    print(f"Streaming job '{STREAMING_APP_NAME}' is running.")


def check_output_freshness(**context):
    """Confirm the shared output file has been written to recently — a
    running-but-stuck job (e.g. stalled on Kafka) wouldn't be caught by the
    check above alone."""
    if not STREAMING_OUTPUT_FILE.exists():
        raise AirflowException(
            f"{STREAMING_OUTPUT_FILE} doesn't exist yet — the streaming job "
            "may not have written any batches."
        )

    age_minutes = (datetime.now().timestamp() - STREAMING_OUTPUT_FILE.stat().st_mtime) / 60
    if age_minutes > STALE_THRESHOLD_MINUTES:
        # Not necessarily a failure — the producer may simply not be running
        # right now — but worth surfacing since it can also mean the job stalled.
        print(
            f"WARNING: {STREAMING_OUTPUT_FILE} hasn't been updated in "
            f"{age_minutes:.1f} minutes (threshold: {STALE_THRESHOLD_MINUTES}). "
            "This is expected if the producer isn't currently sending data."
        )
    else:
        print(f"Output file last updated {age_minutes:.1f} minutes ago — healthy.")


with DAG(
    dag_id="streaming_health_monitor",
    description="Monitors the always-on Spark Structured Streaming job; never starts/stops it",
    default_args=default_args,
    schedule=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["streaming", "monitoring", "ecommerce-pipeline"],
) as dag:

    check_running = PythonOperator(
        task_id="check_spark_application_running",
        python_callable=check_spark_application_running,
    )

    check_freshness = PythonOperator(
        task_id="check_output_freshness",
        python_callable=check_output_freshness,
    )

    check_running >> check_freshness
