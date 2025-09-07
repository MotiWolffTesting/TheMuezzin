import json
import logging
import uuid
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from elasticsearch_service import ElasticsearchService
from mongodb_service import MongoDBService
from config import DataProcessingConfig

logger = logging.getLogger(__name__)

class KafkaSubscriber:
    """Kafka consumer"""
    def __init__(self):
        # Load config from environment
        self.config = DataProcessingConfig.from_env()
        self.topic_name = self.config.kafka_topic_name
        self.group_id = self.config.kafka_group_id
        self.consumer = None
        self.failed_messages = []
        self.elasticsearch = None
        self.mongodb = None
        self._init_consumer()
        self._init_processor()
        self._init_services()

    def _init_consumer(self):
        "Init consumer with properties"
        try:
            self.consumer = KafkaConsumer(
                self.topic_name,
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            logger.info("Kafka consumer initialized.")
        except NoBrokersAvailable:
            logger.error("No Kafka brokers available.")
            self.consumer = None
        except Exception as e:
            logger.error(f"Error initializing Kafka consumer: {e}")
            self.consumer = None

    def _init_processor(self):
        "Init processor (disabled, not implemented)"
        self.data_processor = None

    def _init_services(self):
        "Init Elasticsearch and MongoDB services"
        # Initiate Elastic
        try:
            self.elasticsearch = ElasticsearchService(
                es_host=f"http://{self.config.elasticsearch_host}:{self.config.elasticsearch_port}",
                index_name=self.config.elasticsearch_index
            )
            logger.info("Elasticsearch service initialized.")
        except Exception as e:
            logger.error(f"Error initializing Elasticsearch service: {e}")
            self.elasticsearch = None
        # Initiate MongoDB
        try:
            self.mongodb = MongoDBService(
                conn_string=self.config.mongodb_uri,
                database_name=self.config.mongodb_db_name
            )
            logger.info("MongoDB service initialized.")
        except Exception as e:
            logger.error(f"Error initializing MongoDB service: {e}")
            self.mongodb = None

    def start_consuming(self):
        "Start consume message from Kafka"
        if not self.consumer or not self.data_processor:
            logger.error("Consumer or processor not initialized.")
            return
        
        logger.info("Starting Kafka consumption...")
        try:
            # Iterate over the messages in the consumer
            for message in self.consumer:
                self._process_message(message)
                
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
        finally:
            self._cleanup()

    def _process_message(self, message):
        "Split the message from the topic to metadata, content etc."
        try:
            data = message.value
            logger.info(f"Processing message: {data}")
            if isinstance(data, dict) and 'error' in data:
                logger.warning(f"Skipping corrupted message: {data}")
                return

            # Ensure unique ID
            doc_id = data.get('id') or str(uuid.uuid4())
            data['id'] = doc_id

            # Split metadata/content
            metadata = {k: v for k, v in data.items() if k != 'content'}
            content = data.get('content')

            # Send metadata to Elasticsearch
            if self.elasticsearch:
                try:
                    self.elasticsearch.index_metadata(doc_id, metadata)
                    logger.info(f"Metadata indexed in Elasticsearch for id {doc_id}.")
                    
                except Exception as e:
                    logger.error(f"Failed to index metadata in Elasticsearch: {e}")

            # Send content and metadata to MongoDB
            if self.mongodb:
                try:
                    self.mongodb.insert_document(doc_id, content, metadata)
                    logger.info(f"Document inserted in MongoDB for id {doc_id}.")
                    
                except Exception as e:
                    logger.error(f"Failed to insert document in MongoDB: {e}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.failed_messages.append({
                'data': getattr(message, 'value', None),
                'error': str(e)
            })

    def _cleanup(self):
        "Close services if up and not in use based on call"
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed.")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
                
        if self.mongodb:
            try:
                self.mongodb.cleanup()
                logger.info("MongoDB service cleaned up.")
            except Exception as e:
                logger.error(f"Error cleaning up MongoDB service: {e}")

    def get_failed_messages(self):
        return self.failed_messages
                    