import os
import json
import time
import sys
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("KafkaProducer")

class FinceptKafkaProducer:
    """
    Kafka Producer for Fincept Terminal data.
    Pushes market data, metadata, and analytics to Kafka topics.
    """
    
    def __init__(self, bootstrap_servers: Optional[str] = None):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker list (e.g., 'localhost:9092').
                              Defaults to FINCEPT_KAFKA_BROKER env var.
        """
        self.bootstrap_servers = bootstrap_servers or os.environ.get('FINCEPT_KAFKA_BROKER', 'localhost:9092')
        self.producer = None
        self._connect()
        
    def _connect(self):
        """Establish connection to Kafka with retries"""
        retries = 5
        for i in range(retries):
            try:
                logger.info(f"Connecting to Kafka at {self.bootstrap_servers} (Attempt {i+1}/{retries})...")
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                logger.info("Successfully connected to Kafka.")
                return
            except Exception as e:
                logger.warning(f"Failed to connect to Kafka: {e}")
                if i < retries - 1:
                    time.sleep(2 * (i + 1))
                else:
                    logger.error("Could not connect to Kafka after multiple attempts.")
                    
    def produce(self, topic: str, data: Union[Dict, List]):
        """
        Produce data to a specific topic.
        
        Args:
            topic: Kafka topic name
            data: Data to send (dictionary or list of dictionaries)
        """
        if not self.producer:
            logger.error("Producer not connected. Cannot send data.")
            return False
            
        try:
            if isinstance(data, list):
                for item in data:
                    self.producer.send(topic, item)
            else:
                self.producer.send(topic, data)
                
            self.producer.flush()
            return True
        except Exception as e:
            logger.error(f"Error producing to topic {topic}: {e}")
            return False

    def produce_market_data(self, data: Union[Dict, List], interval: str = 'minute'):
        """
        Produce market data to the appropriate topic.
        
        Args:
            data: Market data object(s)
            interval: 'minute' or 'daily'
        """
        topic = f"market-data-{interval}"
        return self.produce(topic, data)

def main():
    """CLI Entry point for piping data to Kafka"""
    import argparse
    parser = argparse.ArgumentParser(description="Fincept Kafka Producer CLI")
    parser.add_argument("--topic", help="Target Kafka topic", default="market-data-minute")
    parser.add_argument("--broker", help="Kafka broker address")
    parser.add_argument("--stdin", action="store_true", help="Read from stdin")
    
    args = parser.parse_args()
    
    producer = FinceptKafkaProducer(args.broker)
    
    if args.stdin:
        logger.info(f"Reading from stdin and producing to {args.topic}...")
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                # Try to parse as JSON
                data = json.loads(line)
                
                # Check if it's Fincept standard output
                if isinstance(data, dict) and "data" in data and "success" in data:
                    # Extract the payload
                    payload = data["data"]
                    # If it's a batch/table, it might be in rows
                    if isinstance(payload, dict) and payload.get("type") == "table":
                        payload = payload.get("rows", [])
                    
                    producer.produce(args.topic, payload)
                else:
                    # Direct data
                    producer.produce(args.topic, data)
                    
            except json.JSONDecodeError:
                # Not JSON, just send as text if possible (though producer expects dict)
                logger.warning(f"Skipping non-JSON line: {line[:50]}...")
            except Exception as e:
                logger.error(f"Error processing line: {e}")
    else:
        print("No data provided. Use --stdin to pipe JSON lines.")

if __name__ == "__main__":
    main()
