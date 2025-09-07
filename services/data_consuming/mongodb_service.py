from pymongo import MongoClient
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

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
            logger.info("MongoDB connection established successfully")
            
        except Exception as e:
            logger.error(f"Unexpected MongoDB connection error: {e}")

    def _setup_indexes(self):
        "Setup indexes to optimize performance"
        try:
            if self.collection is not None:
                # Create the index based on the unique_id
                self.collection.create_index("unique_id", unique=True)
                self.collection.create_index("processed_at")
                self.collection.create_index("metadata.data_type")
                logger.info("MongoDB indexes created successfully")
            else:
                logger.warning("MongoDB collection is None, cannot create indexes.")
        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")

    def insert_document(self, doc_id, content, metadata):
        "Insert document into MongoDB"
        try:
            # Prepare document
            document = {
                'unique_id': doc_id,
                'data': content,
                'metadata': metadata,
                'processed_at': datetime.now(),
                'status': 'processed'
            }
            if self.collection is not None:
                # Inster document into the collection
                result = self.collection.insert_one(document)
                logger.info(f"Document inserted with ID: {result.inserted_id}")
                return result.inserted_id
            else:
                logger.error("MongoDB collection is None, cannot insert document.")
                return None
            
        except Exception as e:
            logger.error(f"Error inserting document: {e}")
            return None

    def cleanup(self):
        "Close MongoDB connection safely"
        try:
            if self.client:
                self.client.close()
                logger.info("MongoDB connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {e}")
