import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import json
import uuid
from typing import Optional
from shared.logger import Logger
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from services.data_consuming.elasticsearch_service import ElasticsearchService
from services.data_consuming.mongodb_service import MongoDBService
from services.data_consuming.config import DataConsumingConfig

config = DataConsumingConfig.from_env()
logger = Logger.get_logger(
    name="kafka_consumer",
    es_host=config.logger_es_host,
    index=config.logger_index
)

class KafkaSubscriber:
    def __init__(self, config: Optional[DataConsumingConfig] = None):
        # Load config from environment
        self.config = config or DataConsumingConfig.from_env()
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
            logger.info("Initializing Kafka consumer...")
            self.consumer = KafkaConsumer(
                self.topic_name,
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=False
            )
            logger.info(f"Kafka consumer initialized successfully for topic '{self.topic_name}' with group_id '{self.group_id}'")
        except NoBrokersAvailable:
            logger.error(f"No Kafka brokers available at {self.config.kafka_bootstrap_servers}")
            self.consumer = None
        except Exception as e:
            logger.error(f"Error initializing Kafka consumer: {type(e).__name__}: {e}")
            self.consumer = None

    def _init_processor(self):
        "Init processor (disabled, not implemented)"
        self.data_processor = None

    def _init_services(self):
        "Init Elasticsearch and MongoDB services"
        # Initiate Elastic
        try:
            logger.info("Initializing Elasticsearch service...")
            self.elasticsearch = ElasticsearchService(
                es_host=self.config.elasticsearch_host,
                index_name=self.config.elasticsearch_index
            )
            logger.info("Elasticsearch service initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Elasticsearch service: {type(e).__name__}: {e}")
            self.elasticsearch = None
        # Initiate MongoDB
        try:
            logger.info("Initializing MongoDB service...")
            self.mongodb = MongoDBService(
                conn_string=self.config.mongodb_uri,
                database_name=self.config.mongodb_db_name
            )
            logger.info("MongoDB service initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing MongoDB service: {type(e).__name__}: {e}")
            self.mongodb = None

    def start_consuming(self):
        "Start consume message from Kafka"
        if not self.consumer:
            logger.error("Consumer not initialized - cannot start consuming")
            return
        
        logger.info("Starting Kafka message consumption...")
        try:
            # Iterate over the messages in the consumer
            for message in self.consumer:
                self._process_message(message)
                
        except Exception as e:
            logger.error(f"Critical error in consumer loop: {type(e).__name__}: {e}")
        finally:
            logger.info("Cleaning up Kafka consumer...")
            self._cleanup()

    def _process_message(self, message):
        "Split the message from the topic to metadata, content etc."
        try:
            data = message.value
            logger.info(f"Processing message with keys: {list(data.keys()) if isinstance(data, dict) else 'Invalid data'}")
            
            if isinstance(data, dict) and 'error' in data:
                logger.error(f"Skipping corrupted message - contains error: {data}")
                return

            # Ensure unique ID
            doc_id = data.get('id') or str(uuid.uuid4())
            data['id'] = doc_id
            logger.info(f"Processing message with ID: {doc_id}")

            # Read binary content from file path
            binary_content = None
            validated_path = data.get('validated_path')
            if validated_path:
                try:
                    with open(validated_path, 'rb') as file:
                        binary_content = file.read()
                        logger.info(f"Successfully read {len(binary_content)} bytes from {validated_path}")
                except Exception as e:
                    logger.error(f"Failed to read file content from {validated_path}: {type(e).__name__}: {e}")
                    binary_content = None

            # Prepare metadata (exclude validated_path from stored metadata if desired)
            metadata = dict(data.items())

            # Send metadata to Elasticsearch
            if self.elasticsearch:
                try:
                    self.elasticsearch.index_metadata(doc_id, metadata)
                    logger.info(f"Metadata indexed successfully in Elasticsearch for document ID: {doc_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to index metadata in Elasticsearch for document ID {doc_id}: {type(e).__name__}: {e}")

            # Send content and metadata to MongoDB
            if self.mongodb:
                try:
                    result = self.mongodb.insert_document(doc_id, binary_content, metadata)
                    if result:
                        logger.info(f"Document with binary content inserted successfully in MongoDB for ID: {doc_id}")
                    else:
                        logger.error(f"Failed to insert document in MongoDB for ID: {doc_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to insert document in MongoDB for document ID {doc_id}: {type(e).__name__}: {e}")

        except Exception as e:
            logger.error(f"Critical error processing message: {type(e).__name__}: {e}")
            self.failed_messages.append({
                'data': getattr(message, 'value', None),
                'error': str(e),
                'error_type': type(e).__name__
            })

    def _cleanup(self):
        "Close services if up and not in use based on call"
        logger.info("Starting cleanup process...")
        
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed successfully")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {type(e).__name__}: {e}")
                
        if self.mongodb:
            try:
                self.mongodb.cleanup()
                logger.info("MongoDB service cleaned up successfully")
            except Exception as e:
                logger.error(f"Error cleaning up MongoDB service: {type(e).__name__}: {e}")
        
        logger.info("Cleanup process completed")

    def get_failed_messages(self):
        return self.failed_messages
