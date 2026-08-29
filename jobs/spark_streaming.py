"""
Spark Structured Streaming job — stage 3 (processing, real-time lane).

Reads raw JSON events from the `ecommerce-events` Kafka topic, parses them,
and computes a windowed count of events per event_type (view/cart/purchase)
in 1-minute tumbling windows. Writes each batch as newline-delimited JSON to
a shared file that a Streamlit dashboard (or anything else) can read and
refresh from. Optionally also pushes to a Power BI streaming dataset if
POWERBI_PUSH_URL is set — this is now a secondary, optional sink.

Submit with:
    docker exec -it --user root -e HOME=/root -e HADOOP_USER_NAME=spark spark-master \
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.jars.ivy=/root/.ivy2 /opt/jobs/spark_streaming.py
"""

import json
import os
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"  # in-container address, not localhost
KAFKA_TOPIC = "ecommerce-events"

# Primary sink: newline-delimited JSON file, one line per row, appended each
# batch. /opt/data is the container path for the ./data folder mounted in
# docker-compose.yml — so on the host this file shows up at ./data/streaming_kpis.jsonl,
# where a Streamlit app (running on the host or in another container with the
# same volume) can read it.
OUTPUT_FILE_PATH = os.environ.get("STREAM_OUTPUT_PATH", "/opt/data/streaming_kpis.jsonl")

# Optional secondary sink — set POWERBI_PUSH_URL to also push to a Power BI
# streaming dataset. Leave unset to skip this entirely (the default now).
POWERBI_PUSH_URL = os.environ.get("POWERBI_PUSH_URL", "")
POWERBI_MAX_ROWS_PER_REQUEST = 100
POWERBI_PAUSE_BETWEEN_REQUESTS_SEC = 1.0

# Matches the fields the replay producer sends
EVENT_SCHEMA = StructType([
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", LongType()),
    StructField("category_id", LongType()),
    StructField("category_code", StringType()),
    StructField("brand", StringType()),
    StructField("price", DoubleType()),
    StructField("user_id", LongType()),
    StructField("user_session", StringType()),
])


def write_batch_to_file(rows: list[dict]) -> None:
    """Append each row as one JSON line to the shared output file."""
    if not rows:
        return
    os.makedirs(os.path.dirname(OUTPUT_FILE_PATH), exist_ok=True)
    with open(OUTPUT_FILE_PATH, "a") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _post_chunk(rows: list[dict]) -> None:
    body = json.dumps(rows).encode("utf-8")
    request = urllib.request.Request(
        POWERBI_PUSH_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


def push_to_powerbi(rows: list[dict]) -> None:
    """POST rows to the Power BI streaming dataset, chunked and paced to stay
    under its volume limits. No-op if POWERBI_PUSH_URL isn't set."""
    if not rows or not POWERBI_PUSH_URL:
        return

    for i in range(0, len(rows), POWERBI_MAX_ROWS_PER_REQUEST):
        chunk = rows[i : i + POWERBI_MAX_ROWS_PER_REQUEST]
        try:
            _post_chunk(chunk)
        except Exception as exc:
            # Don't crash the streaming query over one failed chunk — log and keep going
            print(f"Power BI push failed for a chunk of {len(chunk)} rows: {exc}")
        if i + POWERBI_MAX_ROWS_PER_REQUEST < len(rows):
            time.sleep(POWERBI_PAUSE_BETWEEN_REQUESTS_SEC)


def main():
    spark = (
        SparkSession.builder
        .appName("EcommerceStreamingKPIs")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # For a live Power BI demo, start from the latest offset rather than
    # replaying the whole backlog — pushing a full day's worth of aggregated
    # history in one burst is exactly what trips Power BI's volume limit.
    # Override with STARTING_OFFSETS=earliest if you want backlog reprocessing
    # instead (e.g. when testing without a Power BI sink).
    starting_offsets = os.environ.get("STARTING_OFFSETS", "latest")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    # Kafka gives us raw bytes in a "value" column — parse it as JSON using our schema
    events = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        # event_time comes in as a string like "2019-10-01 00:00:00 UTC" — Spark needs
        # a real timestamp column to window on, so cast it explicitly
        .withColumn("event_ts", col("event_time").cast("timestamp"))
        # rows with no event_type or a null timestamp aren't useful for the KPI —
        # drop them rather than let them silently skew window counts
        .filter(col("event_type").isNotNull() & col("event_ts").isNotNull())
    )

    windowed_counts = (
        events
        # watermark tells Spark how long to wait for late-arriving events before
        # closing a window — 2 minutes is generous given this is a replayed stream
        .withWatermark("event_ts", "2 minutes")
        .groupBy(
            window(col("event_ts"), "1 minute"),
            col("event_type"),
        )
        .agg(count("*").alias("event_count"))
        .select(
            # Power BI's DateTime field expects ISO 8601 — Spark's default
            # timestamp-to-string cast ("2019-10-01 03:54:00") won't parse
            # reliably, so format it explicitly
            date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_start"),
            date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_end"),
            col("event_type"),
            col("event_count"),
        )
    )

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        rows = [row.asDict() for row in batch_df.collect()]
        write_batch_to_file(rows)
        push_to_powerbi(rows)  # no-op unless POWERBI_PUSH_URL is set
        print(f"Batch {batch_id}: wrote {len(rows)} rows to {OUTPUT_FILE_PATH}")

    query = (
        windowed_counts.writeStream
        .outputMode("update")
        .foreachBatch(process_batch)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()