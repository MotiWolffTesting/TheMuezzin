import logging
from kafka_consumer import KafkaSubscriber

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

def main():
    consumer = KafkaSubscriber()
    consumer.start_consuming()

if __name__ == "__main__":
    main()
