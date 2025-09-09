import os 
from dataclasses import dataclass

@dataclass
class DataTranscriberConfig:
    """Configuration for the data transcriber service loaded from environment variables."""
    
    # MongoDB 
    mongodb_uri: str
    mongodb_db_name: str
    mongodb_collection_name: str
    
    # Elastic 
    elasticsearch_host: str
    elasticsearch_port: str
    elasticsearch_index: str
    elasticsearch_username: str
    elasticsearch_password: str
    
    # Transcription
    speech_recognition_language: str
    
    # Logger
    logger_es_host: str
    logger_index: str
    
    @staticmethod
    def from_env() -> "DataTranscriberConfig":
        return DataTranscriberConfig(
            mongodb_uri=os.getenv("MONGODB_ATLAS_URI", "mongodb://localhost:27017"),
            mongodb_db_name=os.getenv("MONGODB_DB_NAME", "kafka_db"),
            mongodb_collection_name=os.getenv("MONGODB_COLLECTION_NAME", "processed_messages"),
            elasticsearch_host=os.getenv("ELASTICSEARCH_HOST", "localhost"),
            elasticsearch_port=os.getenv("ELASTICSEARCH_PORT", "9200"),
            elasticsearch_index=os.getenv("ELASTICSEARCH_INDEX", "file_metadata"),
            elasticsearch_username=os.getenv("ELASTICSEARCH_USERNAME", "elastic"),
            elasticsearch_password=os.getenv("ELASTICSEARCH_PASSWORD", ""),
            logger_es_host=os.getenv("LOGGER_ES_HOST", "localhost:9200"),
            logger_index=os.getenv("LOGGER_INDEX", "muezzin_logs"),
            speech_recognition_language=os.getenv('SPEECH_RECOGNITION_LANGUAGE', 'en-US'),
        )