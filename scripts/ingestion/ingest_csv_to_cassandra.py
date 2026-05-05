#!/usr/bin/env python3
"""
Ingest CSV files into Cassandra using Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType, TimestampType
import os
import sys

# Add scripts directory to path to allow importing from utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from scripts.utils.spark_utils import create_spark_session

def define_schema():
    """Define schema for minute-level financial data"""
    
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("open", DecimalType(18, 2), True),
        StructField("high", DecimalType(18, 2), True),
        StructField("low", DecimalType(18, 2), True),
        StructField("close", DecimalType(18, 2), True),
        StructField("volume", LongType(), True),
        StructField("adjusted_close", DecimalType(18, 2), True)
    ])
    
    return schema

def ingest_csv_files(spark, input_dir):
    """
    Read CSV files and ingest into Cassandra
    
    Args:
        spark: SparkSession
        input_dir: Directory containing CSV files
    """
    
    print(f"Reading CSV files from: {input_dir}")
    
    # Define schema
    schema = define_schema()
    
    # Read all CSV files
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(f"{input_dir}/*.csv")
    
    print(f"Loaded {df.count():,} records from CSV files")
    
    # Convert timestamp string to timestamp type explicitly
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
    
    # Show sample data
    print("\nSample data:")
    df.show(5, truncate=False)
    
    # Write to Cassandra
    print("\nWriting data to Cassandra...")
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="minute_data", keyspace="financial_data") \
        .save()
    
    print("✓ Data successfully ingested into Cassandra")
    
    # Verify data in Cassandra
    print("\nVerifying data in Cassandra...")
    cassandra_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="minute_data", keyspace="financial_data") \
        .load()
    
    print(f"Total records in Cassandra: {cassandra_df.count():,}")
    
    # Show records by symbol
    print("\nRecords by symbol:")
    cassandra_df.groupBy("symbol").count().show()

def main():
    """Main ingestion workflow"""
    
    # Configuration
    input_dir = "/opt/spark/data-external/raw/csv"
    
    # Create Spark session
    print("Creating Spark session...")
    spark = create_spark_session("CSV_to_Cassandra_Ingestion")
    
    try:
        # Ingest CSV files
        ingest_csv_files(spark, input_dir)
        
        print("\n✓ Ingestion complete!")
        return 0
        
    except Exception as e:
        print(f"\n✗ Error during ingestion: {str(e)}", file=sys.stderr)
        return 1
        
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
