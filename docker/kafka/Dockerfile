# Use Confluent Kafka image
FROM confluentinc/cp-kafka:7.5.0

USER root

# The base image already includes Python 3.9 and Pip
RUN pip3 install --no-cache-dir \
    kafka-python \
    avro

# Create directories
RUN mkdir -p /opt/kafka/data /opt/kafka/logs && \
    chown -R appuser:appuser /opt/kafka

USER appuser

WORKDIR /opt/kafka
