#!/usr/bin/env python3
"""
Aggregate minute-level data to various timeframes
"""

from pyspark.sql.functions import (
    col, window, first, last, max as spark_max, min as spark_min,
    sum as spark_sum, avg, to_date, when
)
from pyspark.sql.window import Window
import sys
import os

# Add scripts directory to path to allow importing from utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from scripts.utils.spark_utils import create_spark_session

def aggregate_to_hourly(df):
    """Aggregate minute data to hourly"""
    
    print("\nAggregating to hourly timeframe...")
    
    # Sort to ensure deterministic first/last prices
    df = df.orderBy("timestamp")
    
    df_hourly = df.groupBy(
        "symbol",
        window("timestamp", "1 hour")
    ).agg(
        first("open", ignorenulls=True).alias("open"),
        spark_max("high").alias("high"),
        spark_min("low").alias("low"),
        last("close", ignorenulls=True).alias("close"),
        spark_sum("volume").alias("volume"),
        avg("close").alias("avg_price")
    ).select(
        "symbol",
        col("window.start").alias("timestamp"),
        "open",
        "high",
        "low",
        "close",
        "volume",
        "avg_price"
    )
    
    count = df_hourly.count()
    print(f"  Hourly records: {count:,}")
    
    return df_hourly

def aggregate_to_daily(df):
    """Aggregate minute data to daily"""
    
    print("\nAggregating to daily timeframe...")
    
    # Sort to ensure deterministic first/last prices
    df = df.orderBy("timestamp")
    
    df_daily = df.groupBy(
        "symbol",
        to_date("timestamp").alias("date")
    ).agg(
        first("open", ignorenulls=True).alias("open"),
        spark_max("high").alias("high"),
        spark_min("low").alias("low"),
        last("close", ignorenulls=True).alias("close"),
        spark_sum("volume").alias("volume"),
        avg("close").alias("avg_price"),
        # VWAP calculation with null handling for zero volume
        (when(spark_sum("volume") > 0, spark_sum(col("close") * col("volume")) / spark_sum("volume")).otherwise(avg("close"))).alias("vwap")
    )
    
    count = df_daily.count()
    print(f"  Daily records: {count:,}")
    
    return df_daily

def main():
    """Main aggregation workflow"""
    
    # Create Spark session
    print("Creating Spark session...")
    spark = create_spark_session("Data_Aggregation")
    
    try:
        # Read cleaned data
        input_path = "/opt/spark/data-external/processed/parquet/cleaned_minute_data"
        print(f"\nReading cleaned data from: {input_path}")
        
        if not os.path.exists(input_path):
            print(f"! WARNING: Input path {input_path} does not exist. No data to aggregate.")
            return 0

        df = spark.read.parquet(input_path)
        record_count = df.count()
        print(f"Loaded {record_count:,} records")
        
        if record_count == 0:
            print("! WARNING: Cleaned dataset is empty. Skipping aggregation.")
            return 0
        
        # Aggregate to hourly
        df_hourly = aggregate_to_hourly(df)
        hourly_output = "/opt/spark/data-external/processed/parquet/hourly_data"
        print(f"Saving hourly data to: {hourly_output}")
        df_hourly.write.mode("overwrite").parquet(hourly_output)
        
        # Aggregate to daily
        df_daily = aggregate_to_daily(df)
        daily_output = "/opt/spark/data-external/processed/parquet/daily_data"
        print(f"Saving daily data to: {daily_output}")
        df_daily.write.mode("overwrite").parquet(daily_output)
        
        # Write daily data to Cassandra (requires spark-defaults.conf to be loaded)
        print("Saving daily data to Cassandra...")
        df_daily.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="daily_data", keyspace="financial_data") \
            .save()
        
        print("\n✓ Data aggregation complete!")
        return 0
        
    except Exception as e:
        print(f"\n✗ Error during aggregation: {str(e)}", file=sys.stderr)
        return 1
        
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
