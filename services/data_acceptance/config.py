import os
from dataclasses import dataclass

@dataclass
class DataAcceptanceConfig:
    """Configuration for the data acceptance service loaded from environment variables."""
     
    # Kafka
    kafka_bootstrap_servers: str
    kafka_topic_name: str
    
    # Logger
    logger_es_host: str
    logger_index: str
    
    @staticmethod
    def from_env() -> "DataAcceptanceConfig":
        return DataAcceptanceConfig(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic_name=os.getenv("KAFKA_TOPIC_NAME", "file_metadata_topic"),
            logger_es_host=os.getenv("LOGGER_ES_HOST", "localhost:9200"),
            logger_index=os.getenv("LOGGER_INDEX", "muezzin_logs")
        )