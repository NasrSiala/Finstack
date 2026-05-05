#!/usr/bin/env python3
"""
Test Kafka producer and consumer
"""

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
from datetime import datetime
import sys

def test_kafka():
    """Test Kafka producer and consumer"""
    
    try:
        # Configuration
        import os
        # Default to internal kafka:9092 (preferred inside docker) or external localhost:9093
        bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092').split(',')
        
        # Create producer
        print(f"Creating Kafka producer with servers: {bootstrap_servers}...")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        print("✓ Producer created")
        
        # Send test message
        print("\nSending test message...")
        test_message = {
            'symbol': 'TEST',
            'timestamp': datetime.now().isoformat(),
            'price': 100.50,
            'volume': 1000
        }
        
        future = producer.send('market-data-minute', test_message)
        record_metadata = future.get(timeout=10)
        
        print(f"✓ Message sent successfully")
        print(f"  Topic: {record_metadata.topic}")
        print(f"  Partition: {record_metadata.partition}")
        print(f"  Offset: {record_metadata.offset}")
        
        producer.close()
        
        # Create consumer
        print(f"Creating Kafka consumer with servers: {bootstrap_servers}...")
        consumer = KafkaConsumer(
            'market-data-minute',
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        print("✓ Consumer created")
        
        # Consume test message
        print("\nConsuming messages...")
        message_count = 0
        for message in consumer:
            print(f"  Received: {message.value}")
            message_count += 1
            break  # Only read one message for testing
        
        consumer.close()
        
        if message_count > 0:
            print("\n✓ All tests passed!")
            return True
        else:
            print("\n✗ No messages received")
            return False
        
    except KafkaError as e:
        print(f"\n✗ Kafka error: {str(e)}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"\n✗ Error: {str(e)}", file=sys.stderr)
        return False

if __name__ == "__main__":
    success = test_kafka()
    sys.exit(0 if success else 1)
