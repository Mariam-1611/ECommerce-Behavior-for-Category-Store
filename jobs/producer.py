"""
Replay producer for the e-commerce behavior dataset.

Reads the Kaggle CSV in chunks and publishes each row as a JSON message to a
Kafka topic, with a small delay between messages to simulate real-time arrival.

Usage:
    python producer.py --csv data/2019-Oct.csv --topic ecommerce-events
"""

import argparse
import json
import time

import pandas as pd
from kafka import KafkaProducer


def parse_args():
    parser = argparse.ArgumentParser(description="Replay a CSV into Kafka as simulated real-time events.")
    parser.add_argument("--csv", required=True, help="Path to the source CSV file")
    parser.add_argument("--topic", default="ecommerce-events", help="Kafka topic to publish to")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--chunksize", type=int, default=1000, help="Rows read from disk per pandas chunk")
    parser.add_argument("--delay", type=float, default=0.01, help="Seconds to sleep between messages")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N messages (omit to run the full file)")
    parser.add_argument("--key-field", default="user_id", help="Column to use as the Kafka message key")
    return parser.parse_args()


def make_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        linger_ms=5,
    )


def main():
    args = parse_args()
    producer = make_producer(args.bootstrap_servers)

    sent = 0
    start = time.time()

    print(f"Streaming '{args.csv}' -> topic '{args.topic}' @ {args.bootstrap_servers}")

    try:
        for chunk in pd.read_csv(args.csv, chunksize=args.chunksize):
            # NaN isn't valid JSON — normalize missing values to None before serializing
            chunk = chunk.where(pd.notnull(chunk), None)

            for row in chunk.to_dict(orient="records"):
                key = row.get(args.key_field)
                producer.send(args.topic, key=key, value=row)
                sent += 1

                if args.delay:
                    time.sleep(args.delay)

                if sent % 500 == 0:
                    elapsed = time.time() - start
                    print(f"  sent {sent} messages ({sent / elapsed:.1f} msgs/sec)")

                if args.limit and sent >= args.limit:
                    raise StopIteration

    except StopIteration:
        pass
    finally:
        producer.flush()
        producer.close()

    elapsed = time.time() - start
    print(f"Done. Sent {sent} messages in {elapsed:.1f}s ({sent / elapsed:.1f} msgs/sec avg).")


if __name__ == "__main__":
    main()
