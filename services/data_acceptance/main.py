import os
from file_reader import FilePathGetter
from metadata import MetadataGetter
from json_builder import JsonBuilder
from kafka_publisher import KafkaPublisher
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# Set Envs
FILE_PATH = os.getenv('FILE_PATH', '/app/podcasts/download (1).wav')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'file_metadata_topic')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

def main():
    "Handling data acceptance"
    try:
        # Initiate and activate file path getter
        path_getter = FilePathGetter(FILE_PATH)
        validated_path = path_getter.get_file_path()

        # Initiate and activate metadata getter
        metadata_getter = MetadataGetter(validated_path)
        metadata = metadata_getter.extract_metadata()

        # Initiate and activate json builder
        json_builder = JsonBuilder(metadata, validated_path)
        json_data = json_builder.build_json()

        # Initiate and activate Kafka producer
        kafka_pub = KafkaPublisher(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC)
        kafka_pub.publish(json_data)
        
    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1
    return 0

if __name__ == "__main__":
    logger.info("Initiating data-consuming services...")
    main()