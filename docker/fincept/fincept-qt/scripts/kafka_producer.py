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
        retries = 3
        for i in range(retries):
            try:
                # Use a short timeout for initialization so we don't block the terminal forever
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks=1, # Faster than 'all'
                    retries=1,
                    request_timeout_ms=5000,
                    delivery_timeout_ms=10000
                )
                return
            except Exception as e:
                if i < retries - 1:
                    time.sleep(1)
                    
    def produce(self, topic: str, data: Union[Dict, List]):
        """Produce data to a specific topic"""
        if not self.producer:
            return False
            
        try:
            if isinstance(data, list):
                for item in data:
                    self.producer.send(topic, item)
            else:
                self.producer.send(topic, data)
            return True
        except Exception:
            return False

    def flush(self):
        """Flush messages"""
        if self.producer:
            self.producer.flush()
