import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class BDSAnalysisConfig:
    """Configuration for BDS Analysis service"""
    
    # Kafka settings
    kafka_bootstrap_servers: str
    kafka_topic_transcription: str
    kafka_topic_bds_results: str
    kafka_consumer_group: str
    
    # Database settings
    mongodb_connection_string: str
    mongodb_database_name: str
    mongodb_collection_bds: str
    
    # Elasticsearch settings
    elasticsearch_host: str
    elasticsearch_index_bds: str
    
    # Logger settings
    logger_es_host: str
    logger_index: str
    
    # BDS settings
    bds_threshold_low: float
    bds_threshold_medium: float
    bds_threshold_high: float
    
    @staticmethod
    def from_env() -> "BDSAnalysisConfig":
        "Load configuration from environment variables"
        return BDSAnalysisConfig(
            kafka_bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            kafka_topic_transcription=os.getenv('KAFKA_TOPIC_TRANSCRIPTION', 'audio_transcription'),
            kafka_topic_bds_results=os.getenv('KAFKA_TOPIC_BDS_RESULTS', 'bds_analysis_results'),
            kafka_consumer_group=os.getenv('KAFKA_CONSUMER_GROUP_BDS', 'bds_analysis_group'),
            mongodb_connection_string=os.getenv('MONGODB_CONNECTION_STRING', 'mongodb://localhost:27017/'),
            mongodb_database_name=os.getenv('MONGODB_DATABASE_NAME', 'muezzin_db'),
            mongodb_collection_bds=os.getenv('MONGODB_COLLECTION_BDS', 'bds_analysis'),
            elasticsearch_host=os.getenv('ELASTICSEARCH_HOST', 'localhost:9200'),
            elasticsearch_index_bds=os.getenv('ELASTICSEARCH_INDEX_BDS', 'bds_analysis'),
            logger_es_host=os.getenv('LOGGER_ES_HOST', 'localhost:9200'),
            logger_index=os.getenv('LOGGER_INDEX', 'muezzin_logs'),
            bds_threshold_low=float(os.getenv('BDS_THRESHOLD_LOW', '5.0')),
            bds_threshold_medium=float(os.getenv('BDS_THRESHOLD_MEDIUM', '15.0')),
            bds_threshold_high=float(os.getenv('BDS_THRESHOLD_HIGH', '30.0'))
        )
