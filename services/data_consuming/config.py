import os
from dataclasses import dataclass

@dataclass
class DataConsumingConfig:
    """Configuration for the data consuming service loaded from environment variables."""
    
    # Mongo (Atlas)
    mongodb_uri: str
    mongodb_db_name: str
    mongodb_collection_name: str

    # Kafka
    kafka_bootstrap_servers: str
    kafka_topic_name: str
    kafka_group_id: str

    # Elasticsearch
    elasticsearch_host: str
    elasticsearch_port: str
    elasticsearch_index: str
    elasticsearch_username: str
    elasticsearch_password: str
    
    @staticmethod
    def from_env() -> "DataConsumingConfig":
        return DataConsumingConfig(
            mongodb_uri=os.getenv("MONGODB_ATLAS_URI", "mongodb://localhost:21017"),
            mongodb_db_name=os.getenv("MONGODB_DB_NAME", "kafka_db"),
            mongodb_collection_name=os.getenv("MONGODB_COLLECTION_NAME", "collection"),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic_name=os.getenv("KAFKA_TOPIC_NAME", "file_metadata_topic"),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", "data-processing-group"),
            elasticsearch_host=os.getenv("ELASTICSEARCH_HOST", "localhost"),
            elasticsearch_port=os.getenv("ELASTICSEARCH_PORT", "9200"),
            elasticsearch_index=os.getenv("ELASTICSEARCH_INDEX", "file_metadata"),
            elasticsearch_username=os.getenv("ELASTICSEARCH_USERNAME", "elastic"),
            elasticsearch_password=os.getenv("ELASTICSEARCH_PASSWORD", "")
        )
    