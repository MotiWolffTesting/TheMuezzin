from kafka import KafkaProducer
import json
import logging 
from config import DataAcceptanceConfig

logger = logging.getLogger(__name__)


class KafkaPublisher:
    """Kafka Producer class that recieves the json"""
    def __init__(self, bootstrap_servers: str, topic: str = DataAcceptanceConfig.from_env().kafka_topic_name) -> None:
        "Initiate topic"
        self.topic = topic
        
        try:
            # Filling producer with its properties
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                linger_ms=10,
                acks="all",
            )
            
        except Exception as e:
            logger.error(f"Error connecting to Kafka broker at {bootstrap_servers}: {e}")
            self.producer = None

    def publish(self, json_data: dict) -> None:
        "Send json data to the Kafka topic"
        if not self.producer:
            logger.info("Kafka producer is not initialized. Message not sent.")
            return
        try:
            # Send json data to the Kafka topic
            self.producer.send(self.topic, json_data)
            self.producer.flush()
            logger.info(f"Sent metadata to Kafka topic '{self.topic}'")
            
        except Exception as e:
            logger.error(f"Error sending message to Kafka topic '{self.topic}': {e}")
