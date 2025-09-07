from elasticsearch import Elasticsearch, exceptions as es_exceptions
import logging
import os

logger = logging.getLogger(__name__)

class ElasticsearchService:
    """ElasticSearch Service for indexing the metadata"""
    def __init__(self,
                 es_host: str = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200"),
                 index_name: str = os.getenv("ELASTICSEARCH_INDEX", "file_metadata")):
        "Init elasticsearch variables"
        self.es_host = es_host
        self.index_name = index_name
        self.client = None
        self._initialize_connection()

    def _initialize_connection(self):
        "Init es connection"
        try:
            self.client = Elasticsearch([self.es_host], basic_auth=("elastic", "MotiSterni5784"))
            # Test connection
            if not self.client.ping():
                raise es_exceptions.ConnectionError("Elasticsearch ping failed")
            logger.info(f"Connected to Elasticsearch at {self.es_host}")
        except Exception as e:
            logger.error(f"Error connecting to Elasticsearch: {e}")
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