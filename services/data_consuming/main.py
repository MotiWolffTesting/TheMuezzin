import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from shared.logger import Logger
from .kafka_consumer import KafkaSubscriber
from .config import DataConsumingConfig

# Initialize the centralized logger
config = DataConsumingConfig.from_env()
logger = Logger.get_logger(
    name="data_consuming_service",
    es_host=config.logger_es_host,
    index=config.logger_index
)

def main():
    try:
        logger.info("The Muezzin data consuming service started successfully")
        consumer = KafkaSubscriber()
        logger.info("Kafka subscriber initialized successfully")
        consumer.start_consuming()
        logger.info("Data consuming service completed successfully")
    except Exception as e:
        logger.error(f"Critical error in data consuming service: {type(e).__name__}: {e}")
        return 1
    return 0

if __name__ == "__main__":
    main()
