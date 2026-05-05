#!/usr/bin/env python3
"""
Stream data from Kafka to Cassandra using Spark Structured Streaming
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType
import sys
import os

# Add scripts directory to path to allow importing from utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from scripts.utils.spark_utils import create_spark_session

def define_schema():
    """Define schema for incoming Kafka messages"""
    
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

def process_stream(spark):
    """Process streaming data from Kafka to Cassandra"""
    
    print("Starting Kafka to Cassandra streaming...")
    
    # Define schema
    schema = define_schema()
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "market-data-minute") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON messages
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convert timestamp — generator serialises as ISO 8601: "2024-01-01T09:30:00+00:00"
    parsed_df = parsed_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )
    
    # Write to Cassandra
    query = parsed_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="minute_data", keyspace="financial_data") \
        .outputMode("append") \
        .option("checkpointLocation", "/opt/spark/data-external/checkpoints/kafka-cassandra") \
        .start()
    
    print("✓ Streaming started")
    print("  Kafka topic: market-data-minute")
    print("  Cassandra table: financial_data.minute_data")
    print("\nWaiting for data...")
    
    query.awaitTermination()

def main():
    """Main streaming workflow"""
    
    # Create Spark session
    print("Creating Spark session...")
    spark = create_spark_session("Kafka_to_Cassandra_Streaming")
    
    try:
        # Start streaming
        process_stream(spark)
        return 0
        
    except KeyboardInterrupt:
        print("\n✓ Streaming stopped by user")
        return 0
        
    except Exception as e:
        print(f"\n✗ Error during streaming: {str(e)}", file=sys.stderr)
        return 1
        
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
