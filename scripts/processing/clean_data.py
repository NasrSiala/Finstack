#!/usr/bin/env python3
"""
Clean and validate financial data from Cassandra
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull, when, count
import sys
import os

# Add scripts directory to path to allow importing from utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from scripts.utils.spark_utils import create_spark_session

def clean_data(spark):
    """Clean and validate data"""
    
    print("Reading data from Cassandra...")
    
    # Read data
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="minute_data", keyspace="financial_data") \
        .load()
    
    initial_count = df.count()
    print(f"Initial record count: {initial_count:,}")
    
    # Check for null values
    print("\nChecking for null values...")
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])
    null_counts.show()
    
    # Remove records with null prices
    print("\nRemoving records with null prices...")
    df_clean = df.filter(
        col("open").isNotNull() &
        col("high").isNotNull() &
        col("low").isNotNull() &
        col("close").isNotNull()
    )
    
    # Remove records with invalid prices (negative or zero)
    print("Removing records with invalid prices...")
    df_clean = df_clean.filter(
        (col("open") > 0) &
        (col("high") > 0) &
        (col("low") > 0) &
        (col("close") > 0)
    )
    
    # Remove records where high < low
    print("Removing records with invalid high/low...")
    df_clean = df_clean.filter(col("high") >= col("low"))
    
    # Remove records where high < open or high < close
    df_clean = df_clean.filter(
        (col("high") >= col("open")) &
        (col("high") >= col("close"))
    )
    
    # Remove records where low > open or low > close
    df_clean = df_clean.filter(
        (col("low") <= col("open")) &
        (col("low") <= col("close"))
    )
    
    final_count = df_clean.count()
    removed_count = initial_count - final_count
    
    print(f"\nCleaning summary:")
    print(f"  Initial records: {initial_count:,}")
    print(f"  Final records: {final_count:,}")
    print(f"  Removed records: {removed_count:,}")
    
    if initial_count > 0:
        print(f"  Retention rate: {(final_count/initial_count)*100:.2f}%")
    else:
        print(f"  Retention rate: N/A (No records found)")
    
    if final_count == 0:
        print("\n! WARNING: No records left after cleaning. Skipping write.")
        return df_clean

    # Save cleaned data
    output_path = "/opt/spark/data-external/processed/parquet/cleaned_minute_data"
    print(f"\nSaving cleaned data to: {output_path}")
    
    df_clean.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print("✓ Data cleaning complete!")
    
    return df_clean

def main():
    """Main cleaning workflow"""
    
    # Create Spark session
    print("Creating Spark session...")
    spark = create_spark_session("Data_Cleaning")
    
    try:
        # Clean data
        clean_data(spark)
        return 0
        
    except Exception as e:
        print(f"\n✗ Error during cleaning: {str(e)}", file=sys.stderr)
        return 1
        
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
