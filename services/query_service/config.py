import os
from dataclasses import dataclass


@dataclass
class QueryServiceConfig:
    """Configuration for the query service loaded from environment variables."""

    # Elasticsearch
    elasticsearch_host: str
    elasticsearch_port: str
    elasticsearch_index_files: str
    elasticsearch_index_bds: str

    # MongoDB
    mongodb_uri: str
    mongodb_db_name: str
    mongodb_collection_name: str

    # Service
    api_host: str
    api_port: int

    @staticmethod
    def from_env() -> "QueryServiceConfig":
        return QueryServiceConfig(
            elasticsearch_host=os.getenv("ELASTICSEARCH_HOST", "localhost"),
            elasticsearch_port=os.getenv("ELASTICSEARCH_PORT", "9200"),
            elasticsearch_index_files=os.getenv("ELASTICSEARCH_INDEX", "file_metadata"),
            elasticsearch_index_bds=os.getenv("ELASTICSEARCH_INDEX_BDS", "bds_analysis"),
            mongodb_uri=os.getenv("MONGODB_ATLAS_URI", "mongodb://localhost:27017"),
            mongodb_db_name=os.getenv("MONGODB_DB_NAME", "kafka_db"),
            mongodb_collection_name=os.getenv("MONGODB_COLLECTION_NAME", "processed_messages"),
            api_host=os.getenv("API_HOST", "0.0.0.0"),
            api_port=int(os.getenv("API_PORT", "8080")),
        )


