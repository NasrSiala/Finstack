#!/bin/bash

# Create Kafka topics for financial data streaming

set -e

echo "Waiting for Kafka broker to be ready..."
until docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 &> /dev/null; do
    echo "  Still waiting for Kafka..."
    sleep 5
done

echo "Creating Kafka topics..."

# Topic for minute-level market data
docker exec -i kafka kafka-topics \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic market-data-minute \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete \
    --config compression.type=snappy

# Topic for daily aggregated data
docker exec -i kafka kafka-topics \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic market-data-daily \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=2592000000 \
    --config cleanup.policy=compact

# Topic for asset metadata updates
docker exec -i kafka kafka-topics \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic asset-metadata \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=compact

echo "Kafka topics created successfully!"

# List all topics
echo -e "\nExisting topics:"
docker exec -i kafka kafka-topics \
    --list \
    --bootstrap-server localhost:9092
