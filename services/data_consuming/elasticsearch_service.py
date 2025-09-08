from elasticsearch import Elasticsearch, exceptions as es_exceptions
import logging
from config import DataConsumingConfig

logger = logging.getLogger(__name__)

# Configure ElasticSearch variables
config = DataConsumingConfig.from_env()
ELASTICSEARCH_HOST = config.elasticsearch_host
ELASTICSEARCH_INDEX = config.elasticsearch_index
ELASTICSEARCH_USERNAME = config.elasticsearch_username
ELASTICSEARCH_PASSWORD = config.elasticsearch_password

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
        host = self.es_host.replace('http://', '').split(':')[0]
        port = config.elasticsearch_port
        es_url = f"http://{host}:{port}"
        try:
            logger.info(f"Connecting to Elasticsearch at {es_url}")
            
            # Try the simplest possible connection
            self.client = Elasticsearch(
                hosts=[{'host': host, 'port': int(port), 'scheme': 'http'}],
                verify_certs=False,
                ssl_show_warn=False
            )
            
            # Test connection
            info = self.client.info()
            logger.info(f"Connected to Elasticsearch: {info['version']['number']}")
        except Exception as e:
            logger.error(f"Error connecting to Elasticsearch: {e}")
            # Try a different connection method
            try:
                logger.info("Trying alternative connection method...")
                self.client = Elasticsearch([es_url])
                info = self.client.info()
                logger.info(f"Connected to Elasticsearch (alternative method): {info['version']['number']}")
            except Exception as e2:
                logger.error(f"Alternative connection also failed: {e2}")
                self.client = None

    def index_metadata(self, doc_id, metadata):
        "Index metadata in Elasticsearch with doc_id as _id"
        if not self.client:
            logger.error("Elasticsearch client not initialized")
            return None
        try:
            # Index the document in Elasticsearch
            resp = self.client.index(index=self.index_name, id=doc_id, document=metadata)
            logger.info(f"Indexed metadata for {doc_id} in Elasticsearch: {resp.get('result')}")
            return resp.get('_id')
        except es_exceptions.ConflictError:
            # Document already exists
            logger.warning(f"Document with id {doc_id} already exists in Elasticsearch")
            return doc_id
        except Exception as e:
            logger.error(f"Error indexing metadata in Elasticsearch: {e}")
            return None