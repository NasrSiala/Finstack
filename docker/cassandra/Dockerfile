# Use official Cassandra image as base
FROM cassandra:4.1

# Set environment variables
ENV CASSANDRA_CLUSTER_NAME=FinancialCluster
ENV CASSANDRA_DC=dc1
ENV CASSANDRA_RACK=rack1
ENV CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch

# Install Python client for testing
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --break-system-packages cassandra-driver pandas && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy initialization scripts
COPY init-scripts/ /docker-entrypoint-initdb.d/

# Expose Cassandra ports
# 7000: Intra-node communication
# 7001: TLS intra-node communication
# 7199: JMX
# 9042: CQL native transport
# 9160: Thrift client API
EXPOSE 7000 7001 7199 9042 9160

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=5 \
    CMD cqlsh -e "DESCRIBE KEYSPACES" || exit 1

# Set working directory
WORKDIR /opt/cassandra
