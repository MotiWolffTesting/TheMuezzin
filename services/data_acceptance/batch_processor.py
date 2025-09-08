import os
import sys
import glob
import time
import signal
from contextlib import contextmanager
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

@contextmanager
def timeout(duration):
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Operation timed out after {duration} seconds")
    
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(duration)
    try:
        yield
    finally:
        signal.alarm(0)

from shared.logger import Logger
from file_reader import FilePathGetter
from metadata import MetadataGetter
from json_builder import JsonBuilder
from kafka_publisher import KafkaPublisher
from config import DataAcceptanceConfig

def process_file(file_path, i, total_files, kafka_pub, logger):
    "Process a single file and publish its metadata to Kafka"
    
    filename = os.path.basename(file_path)
    try:
        logger.info(f"Processing file {i}/{total_files}: {filename}")

        # Validate file path
        path_getter = FilePathGetter(file_path)
        validated_path = path_getter.get_file_path()
        logger.info(f"File path validated successfully for {filename}.")

        # Extract metadata
        metadata_getter = MetadataGetter(validated_path)
        metadata = metadata_getter.extract_metadata()
        logger.info(f"Metadata extracted successfully from {filename}.")

        # Build JSON
        json_builder = JsonBuilder(metadata, validated_path)
        json_data = json_builder.build_json()
        logger.info(f"JSON successfully built from {filename}.")

        # Publish to Kafka with retry logic and timeout
        max_retries = 3
        for retry_count in range(1, max_retries + 1):
            try:
                with timeout(15):  # 15 second timeout for Kafka operations
                    kafka_pub.publish(json_data)
                logger.info(f"Successfully processed and published {filename} ({i}/{total_files}).")
                return "success"
            except TimeoutError:
                if retry_count < max_retries:
                    logger.warning(f"Kafka publish timeout for {filename} (attempt {retry_count}/{max_retries}). Retrying in 2 seconds...")
                    time.sleep(2)
                else:
                    logger.error(f"Failed to publish {filename} to Kafka after {max_retries} timeout attempts")
                    return "kafka_failed"
            except Exception as kafka_error:
                if retry_count < max_retries:
                    logger.warning(f"Kafka publish failed for {filename} (attempt {retry_count}/{max_retries}). Retrying in 2 seconds...")
                    time.sleep(2)
                else:
                    logger.error(f"Failed to publish {filename} to Kafka after {max_retries} attempts: {type(kafka_error).__name__}: {kafka_error}")
                    return "kafka_failed"
        time.sleep(0.5)
    except FileNotFoundError as e:
        logger.error(f"File not found for {filename}: {type(e).__name__}: {e}")
        return "failed"
    except Exception as e:
        logger.error(f"Error processing {filename}: {type(e).__name__}: {e}")
        return "failed"
    return "failed"

def process_all_files():
    "Process all .wav files in the podcasts directory"

    # Initialize logger
    config = DataAcceptanceConfig.from_env()
    logger = Logger.get_logger(
        name="batch_processor",
        es_host=config.logger_es_host,
        index=config.logger_index,
    )

    # Configuration
    PODCASTS_DIR = '/Users/mordechaywolff/Desktop/podcasts'
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "file_metadata_topic")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')  # Use external port

    # Find all .wav files
    wav_files = glob.glob(os.path.join(PODCASTS_DIR, '*.wav'))
    total_files = len(wav_files)

    logger.info(f"Starting batch processing of {total_files} .wav files from {PODCASTS_DIR}...")

    # Initialize Kafka pub
    kafka_pub = KafkaPublisher(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC)

    successful_files = 0
    failed_files = 0
    kafka_failed_files = 0

    for i, file_path in enumerate(wav_files, 1):
        result = process_file(file_path, i, total_files, kafka_pub, logger)
        if result == "success":
            successful_files += 1
        elif result == "kafka_failed":
            kafka_failed_files += 1
        else:
            failed_files += 1

    # Summary
    logger.info(f"Batch processing completed! Successfully processed: {successful_files}, Kafka failed: {kafka_failed_files}, Processing failed: {failed_files}, Total: {total_files}")

    return successful_files, failed_files, kafka_failed_files
    
if __name__ == "__main__":
    success_count, fail_count, kafka_fail_count = process_all_files()
    print("\n Batch Processing Summary:")
    print(f"Successfully processed: {success_count} files")
    print(f"Failed to process: {fail_count} files")
    print(f"Kafka publishing failed: {kafka_fail_count} files")
    print(f"Total files: {success_count + fail_count + kafka_fail_count}")
