from pyspark.sql import SparkSession
import os

def create_spark_session(app_name):
    """
    Create a SparkSession with all required configurations for the financial pipeline.
    Ensures Cassandra and Kafka connectors are properly initialized.
    """
    
    # Check if we are running in local mode or cluster mode
    master = os.environ.get("SPARK_MASTER_URL", "local[*]")
    
    # Base packages required for the pipeline
    # These are already downloaded in the Dockerfile into the jars/ directory,
    # but we can explicitly declare them here for extra robustness if needed.
    # spark-cassandra-connector_2.12:3.5.0
    # spark-sql-kafka-0-10_2.12:3.5.0
    
    # os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {packages} pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = "pyspark-shell"
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.cassandra.connection.host", os.environ.get("CASSANDRA_HOSTS", "cassandra-node1,cassandra-node2,cassandra-node3")) \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .config("spark.sql.streaming.checkpointLocation", "/opt/spark/data-external/checkpoints")
    
    # Only set master if not already set (e.g. by spark-submit)
    if "spark.master" not in [k for k, v in builder._options.items()]:
        builder = builder.master(master)
        
    spark = builder.getOrCreate()
    
    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
