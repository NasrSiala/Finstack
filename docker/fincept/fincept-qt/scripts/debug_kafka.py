import sys
import os
import json

# Add scripts dir to path
sys.path.append(os.path.dirname(__file__))

try:
    from kafka_producer import FinceptKafkaProducer
    print("✓ Successfully imported FinceptKafkaProducer")
    
    broker = os.environ.get('FINCEPT_KAFKA_BROKER', 'kafka:9092')
    print(f"Connecting to broker: {broker}")
    
    producer = FinceptKafkaProducer(broker)
    if producer.producer:
        print("✓ Kafka producer connected")
        
        topic = "market-data-minute"
        data = {"debug": True, "timestamp": 123456789, "msg": "Debug from script"}
        
        print(f"Producing to topic: {topic}")
        success = producer.produce(topic, data)
        
        if success:
            print("✓ Produce command sent")
            producer.flush()
            print("✓ Producer flushed")
        else:
            print("✗ Produce command failed")
    else:
        print("✗ Kafka producer failed to connect (no producer object)")

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
