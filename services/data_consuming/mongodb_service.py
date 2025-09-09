import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from pymongo import MongoClient
from shared.logger import Logger
from datetime import datetime
from .config import DataConsumingConfig

config = DataConsumingConfig.from_env()
logger = Logger.get_logger(
    name="mongodb_service",
    es_host=config.logger_es_host,
    index=config.logger_index
)

class MongoDBService:
    """MongoDB Service to store file content with uuid"""
    def __init__(self,
                 conn_string: str,
                 database_name: str):
        "Initialize variables for mongodb connection"
        self.conn_string = conn_string
        self.database_name = database_name
        self.client = None
        self.db = None
        self.collection = None
        self._initialize_connection()
        self._setup_indexes()

    def _initialize_connection(self):
        "Initialize MongoDB connection"
        try:
            logger.info(f"Attempting to connect to MongoDB at: {self.conn_string}")
            self.client = MongoClient(
                self.conn_string,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            # Test connection
            self.client.admin.command('ping')
            
            # Update db and collection
            self.db = self.client[self.database_name]
            self.collection = self.db['processed_messages']
            logger.info(f"MongoDB connection established successfully to database: {self.database_name}")
            
        except Exception as e:
            logger.error(f"MongoDB connection failed: {type(e).__name__}: {e}")
            self.client = None
            self.db = None
            self.collection = None

    def _setup_indexes(self):
        "Setup indexes to optimize performance"
        try:
            if self.collection is not None:
                logger.info("Setting up MongoDB indexes...")
                # Create the index based on the id
                self.collection.create_index("id", unique=True)
                self.collection.create_index("processed_at")
                self.collection.create_index("metadata.data_type")
                logger.info("MongoDB indexes created successfully")
            else:
                logger.error("MongoDB collection is None - cannot create indexes")
        except Exception as e:
            logger.error(f"Failed to create MongoDB indexes: {type(e).__name__}: {e}")

    def insert_document(self, doc_id, content, metadata):
        "Insert document into MongoDB"
        try:
            logger.info(f"Inserting document with ID: {doc_id}")
            # Prepare document
            document = {
                'id': doc_id,
                'data': content,
                'metadata': metadata,
                'processed_at': datetime.now(),
                'status': 'processed'
            }
            if self.collection is not None:
                # Insert document into the collection
                result = self.collection.insert_one(document)
                logger.info(f"Document inserted successfully in MongoDB with ObjectID: {result.inserted_id}")
                return result.inserted_id
            else:
                logger.error(f"MongoDB collection is None - cannot insert document with ID: {doc_id}")
                return None
            
        except Exception as e:
            logger.error(f"Error inserting document with ID {doc_id}: {type(e).__name__}: {e}")
            return None

    def cleanup(self):
        "Close MongoDB connection safely"
        try:
            if self.client:
                self.client.close()
                logger.info("MongoDB connection closed successfully")
            else:
                logger.info("MongoDB client was not initialized - nothing to cleanup")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {type(e).__name__}: {e}")
