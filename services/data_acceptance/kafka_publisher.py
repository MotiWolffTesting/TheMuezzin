import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
import json
from shared.logger import Logger
from services.data_acceptance.config import DataAcceptanceConfig

config = DataAcceptanceConfig.from_env()
logger = Logger.get_logger(
    name="kafka_publisher",
    es_host=config.logger_es_host,
    index=config.logger_index
)



class KafkaPublisher:
    """Kafka Producer class that recieves the json"""
    def __init__(self, bootstrap_servers: str, topic: str = DataAcceptanceConfig.from_env().kafka_topic_name) -> None:
        "Initiate topic"
        self.topic = topic
        logger.info(f"Initializing Kafka producer for topic: {topic}")
        
        try:
            # Filling producer with its properties
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                linger_ms=10,
                acks='all',
                request_timeout_ms=15000,
                retries=5,
                retry_backoff_ms=500,
            )
            logger.info(f"Kafka producer initialized successfully for servers: {bootstrap_servers}")
            
        except Exception as e:
            logger.error(f"Error initializing Kafka producer for {bootstrap_servers}: {type(e).__name__}: {e}")
            self.producer = None

    def publish(self, json_data: dict) -> None:
        "Send json data to the Kafka topic"
        if not self.producer:
            logger.error("Kafka producer is not initialized - message not sent")
            raise RuntimeError("Kafka producer is not initialized")
        
        try:
            logger.info(f"Publishing message to Kafka topic: {self.topic}")
            
            # Send the message to the Kafka topic and wait
            future = self.producer.send(self.topic, json_data)
            future.get(timeout=15)
            self.producer.flush()
            logger.info(f"Message published successfully to Kafka topic '{self.topic}'")
            
        except KafkaTimeoutError as e:
            logger.error(f"Kafka timeout publishing to '{self.topic}': {type(e).__name__}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error publishing message to Kafka topic '{self.topic}': {type(e).__name__}: {e}")
            raise