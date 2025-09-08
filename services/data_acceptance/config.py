import os
from dataclasses import dataclass

@dataclass
class DataAcceptanceConfig:
    """Configuration for the data acceptance service loaded from environment variables."""
     
    # Kafka
    kafka_bootstrap_servers: str
    kafka_topic_name: str
    
    @staticmethod
    def from_env() -> "DataAcceptanceConfig":
        return DataAcceptanceConfig(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic_name=os.getenv("KAFKA_TOPIC_NAME", "file_metadata_topic"),
        )