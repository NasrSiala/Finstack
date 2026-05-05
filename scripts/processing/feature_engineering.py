#!/usr/bin/env python3
"""
Create technical indicators and features for financial analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg as spark_avg, stddev, log, when
from pyspark.sql.window import Window
import sys
import os

# Add scripts directory to path to allow importing from utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from scripts.utils.spark_utils import create_spark_session

def calculate_technical_indicators(df):
    """Calculate technical indicators"""
    
    print("\nCalculating technical indicators...")
    
    # Define window specifications
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")
    
    # Calculate returns with null handling for zero prices
    df = df.withColumn(
        "returns",
        when(lag("close", 1).over(window_spec) > 0, 
             (col("close") - lag("close", 1).over(window_spec)) / lag("close", 1).over(window_spec))
        .otherwise(0)
    )
    
    df = df.withColumn(
        "log_returns",
        log(col("close") / lag("close", 1).over(window_spec))
    )
    
    # Calculate moving averages
    for period in [20, 50, 200]:
        window_ma = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-period+1, 0)
        df = df.withColumn(
            f"sma_{period}",
            spark_avg("close").over(window_ma)
        )
    
    # Calculate Bollinger Bands (20-period)
    window_bb = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-19, 0)
    df = df.withColumn("bb_middle", spark_avg("close").over(window_bb))
    df = df.withColumn("bb_std", stddev("close").over(window_bb))
    df = df.withColumn("bb_upper", col("bb_middle") + (2 * col("bb_std")))
    df = df.withColumn("bb_lower", col("bb_middle") - (2 * col("bb_std")))
    
    # Calculate volatility
    for period in [10, 20, 30]:
        window_vol = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-period+1, 0)
        df = df.withColumn(
            f"volatility_{period}",
            stddev("returns").over(window_vol)
        )
    
    print("  ✓ Technical indicators calculated")
    
    return df

def main():
    """Main feature engineering workflow"""
    
    # Create Spark session
    print("Creating Spark session...")
    spark = create_spark_session("Feature_Engineering")
    
    try:
        # Read cleaned data
        input_path = "/opt/spark/data-external/processed/parquet/cleaned_minute_data"
        print(f"\nReading cleaned data from: {input_path}")
        
        if not os.path.exists(input_path):
            print(f"! WARNING: Input path {input_path} does not exist. No data to process features.")
            return 0

        df = spark.read.parquet(input_path)
        record_count = df.count()
        print(f"Loaded {record_count:,} records")
        
        if record_count == 0:
            print("! WARNING: Cleaned dataset is empty. Skipping feature engineering.")
            return 0
        
        # Calculate features
        df_features = calculate_technical_indicators(df)
        
        # Show sample
        print("\nSample of engineered features:")
        df_features.select(
            "symbol", "timestamp", "close",
            "returns", "sma_20", "sma_50",
            "bb_upper", "bb_lower", "volatility_20"
        ).show(10, truncate=False)
        
        # Save features
        output_path = "/opt/spark/data-external/processed/parquet/features"
        print(f"\nSaving features to: {output_path}")
        
        df_features.write.mode("overwrite").parquet(output_path)
        
        # Write features to Cassandra
        print("Saving features to Cassandra...")
        df_features.select(
            "symbol", "timestamp", "returns", "log_returns",
            "sma_20", "sma_50", "sma_200", "bb_middle",
            "bb_upper", "bb_lower", "volatility_20"
        ).write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="engineered_features", keyspace="financial_data") \
            .save()
        
        print("\n✓ Feature engineering complete!")
        return 0
        
    except Exception as e:
        print(f"\n✗ Error during feature engineering: {str(e)}", file=sys.stderr)
        return 1
        
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
