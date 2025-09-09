import os
from shared.logger import Logger
from .file_reader import FilePathGetter
from .metadata import MetadataGetter
from .json_builder import JsonBuilder
from .kafka_publisher import KafkaPublisher
from .config import DataAcceptanceConfig

# Initialize the centralized logger
config = DataAcceptanceConfig.from_env()
logger = Logger.get_logger(
    name="data_acceptance_service",
    es_host=config.logger_es_host,
    index=config.logger_index
)

# Set Envs
FILE_PATH = config.file_path
KAFKA_TOPIC = config.kafka_topic_name
KAFKA_BOOTSTRAP_SERVERS = config.kafka_bootstrap_servers

def main():
    "Handling data acceptance"
    try:
        logger.info("The Muezzin data acceptance service started successfully")
        
        # Initiate and activate file path getter
        logger.info(f"Processing file: {FILE_PATH}")
        path_getter = FilePathGetter(FILE_PATH)
        validated_path = path_getter.get_file_path()
        logger.info(f"File path validated successfully: {validated_path}")

        # Initiate and activate metadata getter
        metadata_getter = MetadataGetter(validated_path)
        metadata = metadata_getter.extract_metadata()
        logger.info("Metadata extracted successfully from file")

        # Initiate and activate json builder
        json_builder = JsonBuilder(metadata, validated_path)
        json_data = json_builder.build_json()
        logger.info("JSON data structure built successfully")

        # Initiate and activate Kafka producer
        kafka_pub = KafkaPublisher(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC)
        kafka_pub.publish(json_data)
        logger.info("Data acceptance process completed successfully")
        
    except FileNotFoundError as e:
        logger.error(f"File not found error: {type(e).__name__}: {e}")
        return 1
    except Exception as e:
        logger.error(f"Critical error in data acceptance service: {type(e).__name__}: {e}")
        return 1
    return 0

if __name__ == "__main__":
    main()