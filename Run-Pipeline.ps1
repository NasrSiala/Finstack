<#
.SYNOPSIS
End-to-End Orchestrator for Big Data Financial Pipeline.
This script starts the infrastructure, generates data, processes it through the Bronze/Silver/Gold layers, and prepares visualization.
#>

Write-Host "================================================" -ForegroundColor Cyan
Write-Host " Starting Fincept Big Data Pipeline (End-to-End)" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan

# 1. Start Infrastructure
Write-Host "`n[1/8] Starting Docker Containers..." -ForegroundColor Yellow
docker-compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to start docker containers. Ensure Docker Desktop is running." -ForegroundColor Red
    exit 1
}

Write-Host "Waiting for clusters (Kafka/Cassandra/Spark) to stabilize..." -ForegroundColor DarkGray
# Wait for Cassandra to be healthy before proceeding
while ($(docker inspect --format '{{.State.Health.Status}}' cassandra-node1) -ne "healthy") {
    Write-Host "Waiting for Cassandra node1 to become healthy..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 5
}

# 1.5 Initialize Cassandra Schema
Write-Host "`n[1.5/8] Initializing Cassandra Schema..." -ForegroundColor Yellow
docker exec -i cassandra-node1 cqlsh -f /docker-entrypoint-initdb.d/01-create-schema.cql
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to initialize Cassandra schema." -ForegroundColor Red
    exit 1
}

# 2. Create Kafka Topics
Write-Host "`n[2/8] Ensuring Kafka Topics exist..." -ForegroundColor Yellow
docker exec -i kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic market-data-minute --partitions 6 --replication-factor 1 --config retention.ms=604800000 --config cleanup.policy=delete --config compression.type=snappy
docker exec -i kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic market-data-daily --partitions 3 --replication-factor 1 --config retention.ms=2592000000 --config cleanup.policy=compact
docker exec -i kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic asset-metadata --partitions 1 --replication-factor 1 --config cleanup.policy=compact

# 3. Start Streaming Job (Background)
Write-Host "`n[3/8] Launching Spark Streaming Job (Kafka -> Cassandra)..." -ForegroundColor Yellow
# We kill any existing streaming job first to avoid conflicts
$existing_job = docker exec spark-master bash -c "ps aux | grep kafka_to_cassandra_streaming | grep -v grep | awk '{print `$2}'"
if ($existing_job) { docker exec spark-master kill -9 $existing_job }

docker exec -d spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/scripts/ingestion/kafka_to_cassandra_streaming.py
Write-Host "Streaming job running in background." -ForegroundColor DarkGray

# 4. Fetch Public API Data (Background Loop)
Write-Host "`n[4/8] Launching Real-time Market Data Feeder (Loop)..." -ForegroundColor Yellow
$existing_feeder = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "*yfinance_feeder.py*" }
if ($existing_feeder) { 
    Write-Host "Stopping existing feeder (PID: $($existing_feeder.ProcessId))..." -ForegroundColor DarkGray
    Stop-Process -Id $existing_feeder.ProcessId -Force 
}

Start-Process python -ArgumentList "scripts\ingestion\yfinance_feeder.py --loop --delay 60" -NoNewWindow
Write-Host "Feeder running in background (polling every 60s)." -ForegroundColor DarkGray

Write-Host "Waiting 30 seconds for streaming job to sink data into Cassandra..." -ForegroundColor DarkGray
Start-Sleep -Seconds 30

# 5. Batch Processing: Cleaning & Aggregation
Write-Host "`n[5/8] Running Spark Batch: Data Cleaning..." -ForegroundColor Yellow
docker exec -i spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/scripts/processing/clean_data.py

Write-Host "`n[6/8] Running Spark Batch: Data Aggregation & Feature Engineering..." -ForegroundColor Yellow
docker exec -i spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/scripts/processing/aggregate_data.py
docker exec -i spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/scripts/processing/feature_engineering.py

# 6. Promote to Gold Layer (PostgreSQL)
Write-Host "`n[7/8] Promoting processed data to Gold Layer (Postgres for Power BI)..." -ForegroundColor Yellow
docker exec -i spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/bitnami/spark/jars/postgresql-42.6.0.jar /opt/spark/scripts/processing/promote_to_gold.py

# 7. Restart UI for fresh connection
Write-Host "`n[8/8] Refreshing Fincept Terminal UI..." -ForegroundColor Yellow
docker-compose restart fincept-terminal

Write-Host "`n================================================" -ForegroundColor Green
Write-Host " PIPELINE EXECUTION COMPLETE! " -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green
Write-Host "`nData flow:"
Write-Host " Yahoo Finance -> Kafka -> Spark Streaming -> Cassandra (Silver) -> Spark Batch -> Postgres (Gold)"
Write-Host "`nVisualization Ready:"
Write-Host " 1. Fincept Terminal (C++ Qt) should be popping up on your screen (requires VcXsrv running)."
Write-Host " 2. Power BI can now connect to localhost:5432 (User: admin, Pass: financial_secret) to read Gold tables."
Write-Host "================================================"
