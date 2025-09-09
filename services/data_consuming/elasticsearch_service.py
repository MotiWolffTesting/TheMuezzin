import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from shared.logger import Logger
from services.data_consuming.config import DataConsumingConfig

# Configure ElasticSearch variables
config = DataConsumingConfig.from_env()
ELASTICSEARCH_HOST = config.elasticsearch_host
ELASTICSEARCH_INDEX = config.elasticsearch_index
ELASTICSEARCH_USERNAME = config.elasticsearch_username
ELASTICSEARCH_PASSWORD = config.elasticsearch_password

logger = Logger.get_logger(
    name="elasticsearch_service",
    es_host=config.logger_es_host,
    index=config.logger_index
)

class ElasticsearchService:
    """ElasticSearch Service for indexing the metadata"""
    def __init__(self,
                 es_host: str = ELASTICSEARCH_HOST,
                 index_name: str = ELASTICSEARCH_INDEX):
        "Init elasticsearch variables"
        self.es_host = es_host
        self.index_name = index_name
        self.client = None
        self._initialize_connection()

    def _initialize_connection(self):
        "Init es connection"
        # Sanitize host to remove protocol and port if present
        host = self.es_host.replace('http://', '').replace('https://', '').split(':')[0]
        port = config.elasticsearch_port
        es_url = f"http://{host}:{port}"
        try:
            logger.info(f"Attempting to connect to Elasticsearch at {es_url}")
            
            # Try the simplest possible connection
            self.client = Elasticsearch(
                hosts=[{'host': host, 'port': int(port), 'scheme': 'http'}],
                verify_certs=False,
                ssl_show_warn=False
            )
            
            # Test connection
            info = self.client.info()
            logger.info(f"Successfully connected to Elasticsearch version: {info['version']['number']}")
            self._ensure_index()
        except Exception as e:
            logger.error(f"Error connecting to Elasticsearch: {type(e).__name__}: {e}")
            # Try a different connection method
            try:
                logger.info("Attempting alternative connection method...")
                self.client = Elasticsearch([es_url])
                info = self.client.info()
                logger.info(f"Successfully connected to Elasticsearch using alternative method - version: {info['version']['number']}")
                self._ensure_index()
            except Exception as e2:
                logger.error(f"Alternative connection also failed: {type(e2).__name__}: {e2}")
                self.client = None
    
    def _ensure_index(self):
        "Ensure es index exists. if not, create it"
        try:
            # Check if the client is connected and there is no index
            if self.client and not self.client.indices.exists(index=self.index_name):
                # Create the index
                self.client.indices.create(
                    index=self.index_name,
                    mappings={
                        'properties': {
                            'id': {'type': 'keyword'},
                            'metadata.filesize': {'type': 'long'},
                            'metadata.duration': {'type': 'float'},
                            'transcription': {'type': 'text'},
                            'timestamp': {'type': 'date'}
                        }
                    }
                )
                logger.info(f"Created Elasticsearch index: {self.index_name}")
        except Exception as e:
            logger.error(f"Failed ensuring Elasticsearch index '{self.index_name}': {type(e).__name__}: {e}")

    def index_metadata(self, doc_id, metadata):
        "Index metadata in Elasticsearch with doc_id as _id"
        if not self.client:
            logger.error("Elasticsearch client not initialized - cannot index metadata")
            return None
        try:
            # Index the document in Elasticsearch
            resp = self.client.index(index=self.index_name, id=doc_id, document=metadata)
            logger.info(f"Metadata indexed successfully in Elasticsearch for document ID {doc_id}: {resp.get('result')}")
            return resp.get('_id')
        except es_exceptions.ConflictError:
            # Document already exists
            logger.error(f"Document with ID {doc_id} already exists in Elasticsearch - conflict error")
            return doc_id
        except Exception as e:
            logger.error(f"Error indexing metadata in Elasticsearch for document ID {doc_id}: {type(e).__name__}: {e}")
            return None