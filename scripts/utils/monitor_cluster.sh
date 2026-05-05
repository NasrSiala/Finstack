#!/bin/bash

# Monitor cluster health and performance

echo "================================================"
echo "Cluster Health Monitor"
echo "================================================"

# Cassandra status
echo -e "\n=== Cassandra Cluster Status ==="
docker exec cassandra-node1 nodetool status

# Spark status
echo -e "\n=== Spark Cluster Status ==="
echo "Master: http://localhost:8080"
echo "Workers:"
echo "  - Worker 1: http://localhost:8081"
echo "  - Worker 2: http://localhost:8082"
echo "  - Worker 3: http://localhost:8083"

# Container resource usage
echo -e "\n=== Container Resource Usage ==="
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}"

# Disk usage
echo -e "\n=== Data Directory Disk Usage ==="
du -h --max-depth=1 data/
