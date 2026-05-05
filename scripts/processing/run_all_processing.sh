#!/bin/bash

# Run all processing jobs in sequence

set -e

echo "================================================"
echo "Starting Financial Data Processing Pipeline"
echo "================================================"

# Step 1: Data Cleaning
echo -e "\n[1/3] Running data cleaning..."
    docker exec -i spark-master \
        /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/scripts/processing/clean_data.py

# Step 2: Data Aggregation
echo -e "\n[2/3] Running data aggregation..."
    docker exec -i spark-master \
        /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/scripts/processing/aggregate_data.py

# Step 3: Feature Engineering
echo -e "\n[3/3] Running feature engineering..."
    docker exec -i spark-master \
        /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/scripts/processing/feature_engineering.py

echo -e "\n================================================"
echo "✓ All processing jobs completed successfully!"
echo "================================================"
