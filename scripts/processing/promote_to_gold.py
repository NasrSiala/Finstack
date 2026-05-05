#!/usr/bin/env python3
"""
Promote data from Cassandra (Silver) to TWO Gold Layer sinks in parallel:
  1. PostgreSQL  → Power BI / relational BI tools
  2. Parquet     → FinceptTerminal / analytical reads

Architecture:
  Cassandra (Silver)
       │
       ▼
  Spark Aggregation (daily OHLCV)
       │
       ├── [Thread 1] ──▶ PostgreSQL  (daily_summary table)
       │
       └── [Thread 2] ──▶ Parquet     (/opt/spark/data-external/processed/)

The DataFrame is .cache()'d after aggregation so both threads share
the same in-memory result — the heavy Cassandra read + groupBy is
executed only ONCE regardless of how many sinks we write to.
"""

import os
import sys
import threading
import time

from pyspark.sql.functions import col, to_date, min as spark_min, max as spark_max
from pyspark.sql.functions import sum as spark_sum, first, last

# Allow importing spark_utils from the repo root
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from scripts.utils.spark_utils import create_spark_session

# ── Sink configuration ────────────────────────────────────────────────────────

POSTGRES_URL      = "jdbc:postgresql://postgres-gold:5432/financial_gold"
POSTGRES_TABLE    = "daily_summary"
POSTGRES_USER     = "admin"
POSTGRES_PASSWORD = "financial_secret"
POSTGRES_DRIVER   = "org.postgresql.Driver"

PARQUET_OUTPUT    = "/opt/spark/data-external/processed/daily_summary"

# ── Sink writers ──────────────────────────────────────────────────────────────

def write_postgres(df, results: dict):
    """Write the cached DataFrame to PostgreSQL (Gold Layer for Power BI)."""
    sink = "PostgreSQL"
    try:
        print(f"  [{sink}] Starting write...")
        start = time.time()
        df.write \
            .format("jdbc") \
            .option("url",      POSTGRES_URL) \
            .option("dbtable",  POSTGRES_TABLE) \
            .option("user",     POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver",   POSTGRES_DRIVER) \
            .mode("overwrite") \
            .save()
        elapsed = round(time.time() - start, 1)
        results[sink] = f"✓ Done in {elapsed}s  →  {POSTGRES_URL}/{POSTGRES_TABLE}"
    except Exception as e:
        results[sink] = f"✗ FAILED: {e}"


def write_parquet(df, results: dict):
    """Write the cached DataFrame to Parquet (Gold Layer for FinceptTerminal)."""
    sink = "Parquet"
    try:
        print(f"  [{sink}]   Starting write...")
        start = time.time()
        df.write \
            .mode("overwrite") \
            .partitionBy("symbol") \
            .parquet(PARQUET_OUTPUT)
        elapsed = round(time.time() - start, 1)
        results[sink] = f"✓ Done in {elapsed}s  →  {PARQUET_OUTPUT}"
    except Exception as e:
        results[sink] = f"✗ FAILED: {e}"


# ── Main pipeline ─────────────────────────────────────────────────────────────

def promote_to_gold(spark):
    """
    1. Read  minute_data from Cassandra.
    2. Aggregate to daily OHLCV.
    3. Cache the result.
    4. Fan out to PostgreSQL + Parquet in parallel threads.
    """

    # ── Step 1: Read from Cassandra Silver Layer ──────────────────────────────
    print("\n[1/4] Reading from Cassandra Silver Layer...")
    raw_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="minute_data", keyspace="financial_data") \
        .load()

    initial_count = raw_df.count()
    print(f"      Loaded {initial_count:,} minute-level records")

    if initial_count == 0:
        print("! WARNING: Cassandra Silver Layer is empty. Skipping gold promotion.")
        return 0

    # ── Step 2: Aggregate to daily OHLCV ─────────────────────────────────────
    print("\n[2/4] Aggregating to daily OHLCV summaries...")
    aggregated_df = (
        raw_df
        .withColumn("date", to_date(col("timestamp")))
        .groupBy("symbol", "date")
        .agg(
            first("open").alias("open"),          # first minute open of the day
            spark_max("high").alias("high"),       # day high
            spark_min("low").alias("low"),         # day low
            last("close").alias("close"),          # last minute close of the day
            spark_sum("volume").alias("volume"),   # total daily volume
        )
        .orderBy("symbol", "date")
    )

    # ── Step 3: Cache — pay the computation cost exactly once ─────────────────
    print("\n[3/4] Caching aggregated DataFrame (shared by both sinks)...")
    aggregated_df.cache()
    row_count = aggregated_df.count()          # triggers caching
    print(f"      Cached {row_count:,} daily summary rows")
    aggregated_df.show(5, truncate=False)

    # ── Step 4: Sequential writes ────────────────────────────────────────────────
    # We sequentialize these because parallel writes from the same session on 
    # Windows/Docker mounts can cause Hadoop committer race conditions.
    print("\n[4/4] Writing to Gold Layer sinks...\n")
    results = {}
    
    write_postgres(aggregated_df, results)
    write_parquet(aggregated_df, results)

    aggregated_df.unpersist()

    # ── Report ────────────────────────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("  GOLD LAYER PROMOTION RESULTS")
    print("─" * 60)
    for sink, status in results.items():
        print(f"  {sink:<12} {status}")
    print("─" * 60)

    failed = [s for s, r in results.items() if r.startswith("✗")]
    if failed:
        raise RuntimeError(f"Sinks failed: {failed}")

    return row_count


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  PROMOTE TO GOLD — Dual Sink (PostgreSQL + Parquet)")
    print("=" * 60)

    # Create Spark session
    print("Creating Spark session...")
    spark = create_spark_session("Promote_To_Gold_DualSink")
    
    # Quick check for JDBC driver in Spark classpath
    try:
        spark._jvm.Class.forName(POSTGRES_DRIVER)
        print(f"✓ JDBC Driver {POSTGRES_DRIVER} found in classpath.")
    except:
        print(f"! WARNING: JDBC Driver {POSTGRES_DRIVER} not found in Spark classpath.")
        print("  Make sure to run with --jars /opt/bitnami/spark/jars/postgresql-42.6.0.jar")

    try:
        rows = promote_to_gold(spark)
        print(f"\n✓ Pipeline complete — {rows:,} daily rows promoted to Gold Layer.")
        return 0
    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
