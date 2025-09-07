import os
from file_reader import FilePathGetter
from metadata import MetadataGetter
from json_builder import JsonBuilder
from kafka_publisher import KafkaPublisher

# Set Envs
FILE_PATH = os.getenv('FILE_PATH', '/Users/mordechaywolff/Desktop/podcasts/download (1).wav')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'file_metadata_topic')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

def main():
    "Handling data acceptance"
    
    # Initiate and activate file path getter
    path_getter = FilePathGetter(FILE_PATH)
    validated_path = path_getter.get_file_path(FILE_PATH)

    # Initiate and activate metadata getter
    metadata_getter = MetadataGetter(validated_path)
    metadata = metadata_getter.extract_metadata()

    # Initiate and activate json builder
    json_builder = JsonBuilder(metadata, validated_path)
    json_data = json_builder.build_json()

    # Initiate and activate Kafka producer
    kafka_pub = KafkaPublisher(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC)
    kafka_pub.publish(json_data)

if __name__ == "__main__":
    main()