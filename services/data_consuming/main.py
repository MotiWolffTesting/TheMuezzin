import logging
from kafka_consumer import KafkaSubscriber

def main():
    logging.basicConfig(level=logging.INFO)
    consumer = KafkaSubscriber()
    consumer.start_consuming()

if __name__ == "__main__":
    main()
