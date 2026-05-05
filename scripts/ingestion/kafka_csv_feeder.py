#!/usr/bin/env python3
"""
kafka_csv_feeder.py
────────────────────────────────────────────────────────────
Replays raw CSV market data into the Kafka topic
'market-data-minute' at a configurable rate.

This bridges the batch CSV files with the real-time Kafka
streaming pipeline (kafka_to_cassandra_streaming.py).

Usage (inside spark-master container):
    python3 kafka_csv_feeder.py                   # default 50 msg/s
    python3 kafka_csv_feeder.py --rate 200        # 200 msg/s
    python3 kafka_csv_feeder.py --rate 0          # as fast as possible
    python3 kafka_csv_feeder.py --once            # send all, then exit
"""

import os
import sys
import csv
import json
import time
import logging
import argparse
import glob
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("KafkaCsvFeeder")

# ── Config ────────────────────────────────────────────────────────────────────
BROKER        = os.environ.get("FINCEPT_KAFKA_BROKER", "kafka:9092")
TOPIC         = "market-data-minute"
CSV_DIR       = "/opt/spark/data-external/raw/csv"
DEFAULT_RATE  = 50   # messages per second; 0 = unlimited


# ── Helpers ───────────────────────────────────────────────────────────────────

def connect(broker: str, retries: int = 10) -> KafkaProducer:
    """Create KafkaProducer with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            log.info(f"Connecting to Kafka at {broker}  (attempt {attempt}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=5,          # small batching for throughput
                batch_size=65536,
            )
            log.info("✓ Connected to Kafka")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Broker not available, retrying in {attempt * 2}s...")
            time.sleep(attempt * 2)
    raise RuntimeError(f"Could not connect to Kafka at {broker} after {retries} attempts")


def normalise_timestamp(raw: str) -> str:
    """
    Convert CSV timestamp (e.g. '2026-03-24 09:30:00.566322')
    to ISO-8601 string expected by the Spark streaming consumer
    (e.g. '2026-03-24T09:30:00').
    """
    try:
        dt = datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        dt = datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S")
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def row_to_message(row: dict) -> dict:
    """Convert a CSV row dict into a Kafka message payload."""
    return {
        "symbol":         row["symbol"],
        "timestamp":      normalise_timestamp(row["timestamp"]),
        "open":           float(row["open"]),
        "high":           float(row["high"]),
        "low":            float(row["low"]),
        "close":          float(row["close"]),
        "volume":         int(row["volume"]),
        "adjusted_close": float(row["adjusted_close"]),
    }


def load_csv_rows(csv_dir: str) -> list:
    """Read all CSV files in the directory and return a flat list of rows."""
    files = sorted(glob.glob(os.path.join(csv_dir, "*.csv")))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {csv_dir}")
    log.info(f"Found {len(files)} CSV file(s): {[os.path.basename(f) for f in files]}")

    rows = []
    for fpath in files:
        with open(fpath, newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append(row)
    log.info(f"Loaded {len(rows):,} total rows from CSV files")
    return rows


# ── Main feeder ───────────────────────────────────────────────────────────────

def feed(producer: KafkaProducer, rows: list, rate: int, once: bool):
    """
    Stream rows into Kafka.
    rate=0  → no throttling (as fast as possible)
    once=True → single pass then exit
    """
    delay = (1.0 / rate) if rate > 0 else 0
    pass_num = 0

    while True:
        pass_num += 1
        log.info(f"━━ Pass {pass_num} — sending {len(rows):,} messages to '{TOPIC}' "
                 f"({'unlimited' if rate == 0 else str(rate) + ' msg/s'}) ━━")

        sent = 0
        t0 = time.time()

        for row in rows:
            try:
                msg = row_to_message(row)
                producer.send(TOPIC, msg)
                sent += 1

                if delay:
                    time.sleep(delay)

                # Progress every 1000 messages
                if sent % 1000 == 0:
                    elapsed = time.time() - t0
                    actual_rate = sent / elapsed if elapsed > 0 else 0
                    log.info(f"  Sent {sent:,}/{len(rows):,}  "
                             f"({actual_rate:.0f} msg/s actual)")

            except Exception as e:
                log.error(f"Error sending row {sent}: {e}")

        producer.flush()
        elapsed = time.time() - t0
        actual_rate = sent / elapsed if elapsed > 0 else 0
        log.info(f"✓ Pass {pass_num} complete — {sent:,} messages in "
                 f"{elapsed:.1f}s  ({actual_rate:.0f} msg/s)")

        if once:
            log.info("--once flag set, exiting after single pass.")
            break

        log.info("Looping in 5s for continuous simulation (Ctrl-C to stop)...")
        time.sleep(5)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Feed CSV market data into Kafka")
    parser.add_argument("--broker", default=BROKER,        help="Kafka broker (host:port)")
    parser.add_argument("--topic",  default=TOPIC,         help="Target Kafka topic")
    parser.add_argument("--dir",    default=CSV_DIR,       help="Directory containing CSV files")
    parser.add_argument("--rate",   type=int, default=DEFAULT_RATE,
                        help="Messages per second (0 = unlimited)")
    parser.add_argument("--once",   action="store_true",
                        help="Send all rows once then exit (no replay loop)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  Kafka CSV Feeder")
    log.info(f"  Broker : {args.broker}")
    log.info(f"  Topic  : {args.topic}")
    log.info(f"  Dir    : {args.dir}")
    log.info(f"  Rate   : {'unlimited' if args.rate == 0 else str(args.rate) + ' msg/s'}")
    log.info(f"  Mode   : {'single pass' if args.once else 'continuous replay'}")
    log.info("=" * 60)

    producer = connect(args.broker)
    rows     = load_csv_rows(args.dir)

    try:
        feed(producer, rows, args.rate, args.once)
    except KeyboardInterrupt:
        log.info("\n✓ Feeder stopped by user")
    finally:
        producer.close()
        log.info("Producer closed.")


if __name__ == "__main__":
    main()
