#!/bin/bash

# End-to-end system test

set -e

echo "================================================"
echo "Big Data Financial Pipeline - End-to-End Test"
echo "================================================"

# Test 1: Check all containers are running
echo -e "\n[Test 1] Checking container status..."
containers=("cassandra-node1" "cassandra-node2" "cassandra-node3" 
            "zookeeper" "kafka"
            "spark-master" "spark-worker-1" "spark-worker-2" "spark-worker-3"
            "fincept-terminal")

for container in "${containers[@]}"; do
    if docker ps | grep -q "$container"; then
        echo "  ✓ $container is running"
    else
        echo "  ✗ $container is not running"
        exit 1
    fi
done

# Test 2: Verify Cassandra cluster
echo -e "\n[Test 2] Verifying Cassandra cluster..."
docker exec cassandra-node1 nodetool status | awk '/^UN/{count++} END{print count+0}' | \
    grep -q "3" && echo "  ✓ Cassandra cluster healthy (3 nodes up)" || \
    (echo "  ✗ Cassandra cluster unhealthy (expected 3 UN nodes)" && exit 1)

# Test 3: Check Cassandra data
echo -e "\n[Test 3] Checking Cassandra data..."
record_count=$(docker exec cassandra-node1 \
    cqlsh -e "SELECT COUNT(*) FROM financial_data.minute_data;" | \
    grep -oP '\d+' | tail -1)
echo "  ✓ Found $record_count records in Cassandra"

# Test 4: Verify Spark cluster
echo -e "\n[Test 4] Verifying Spark cluster..."
worker_count=$(curl -s http://localhost:8080 | grep -c "spark-worker")
if [ "$worker_count" -ge 3 ]; then
    echo "  ✓ Spark cluster healthy ($worker_count workers)"
else
    echo "  ✗ Spark cluster unhealthy"
    exit 1
fi

# Test 5: Check processed data
echo -e "\n[Test 5] Checking processed data..."
if [ -d "data/processed/parquet/cleaned_minute_data" ]; then
    echo "  ✓ Cleaned data exists"
else
    echo "  ✗ Cleaned data not found"
    exit 1
fi

if [ -d "data/processed/parquet/daily_data" ]; then
    echo "  ✓ Daily aggregated data exists"
else
    echo "  ✗ Daily aggregated data not found"
    exit 1
fi

if [ -d "data/processed/parquet/features" ]; then
    echo "  ✓ Engineered features exist"
else
    echo "  ✗ Engineered features not found"
    exit 1
fi

# Test 6: Verify Kafka services
echo -e "\n[Test 6] Verifying Kafka brokers..."
if docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 &> /dev/null; then
    echo "  ✓ Kafka brokers accessible"
else
    echo "  ✗ Kafka brokers not accessible"
    exit 1
fi

echo -e "\n================================================"
echo "✓ All tests passed successfully!"
echo "================================================"
