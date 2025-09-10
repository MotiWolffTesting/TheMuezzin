import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from services.bds_analysis.kafka_consumer import BDSKafkaConsumer

if __name__ == "__main__":
    consumer = BDSKafkaConsumer()
    consumer.run()
