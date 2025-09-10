import os
import sys
from typing import Dict, Any

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from elasticsearch import Elasticsearch
from shared.logger import Logger
from services.bds_analysis.config import BDSAnalysisConfig

class BDSStorageManager:
    """Stores BDS analysis results into Elasticsearch under the configured index"""
    def __init__(self, config: BDSAnalysisConfig):
        self.config = config
        self.logger = Logger.get_logger(
            name="bds_storage_manager",
            es_host=self.config.logger_es_host,
            index=self.config.logger_index
        )
        
        # Initialize ES client
        host = self.config.elasticsearch_host.replace('http://', '').replace('https://', '')
        if ':' in host:
            host, port = host.split(':', 1)
        else:
            port = '9200'
        self.es = Elasticsearch(hosts=[{'host': host, 'port': int(port), 'scheme': 'http'}])
        self._ensure_index()
        
    def _ensure_index(self) -> None:
        "Ensure ES index exists, if not, create it"
        try:
            if not self.es.indices.exists(index=self.config.elasticsearch_index_bds):
                self.es.indices.create(
                    index=self.config.elasticsearch_index_bds,
                    mappings={
                        'properties': {
                            'filename': {'type': 'keyword'},
                            'timestamp': {'type': 'date'},
                            'bds_percentage': {'type': 'float'},
                            'is_bds': {'type': 'boolean'},
                            'bds_threat_level': {'type': 'keyword'},
                            'high_threat_count': {'type': 'integer'},
                            'medium_threat_count': {'type': 'integer'},
                            'pair_count': {'type': 'integer'}
                        }
                    }
                )
                self.logger.info(f"Created Elasticsearch index: {self.config.elasticsearch_index_bds}")
        except Exception as e:
            self.logger.error(f"Failed ensuring BDS index: {type(e).__name__}: {e}")
            
    def store_result(self, doc: Dict[str, Any]) -> bool:
        "Index a single analysis result document"
        try:
            self.es.index(index=self.config.elasticsearch_index_bds, document=doc)
            self.logger.info("Successfully stored BDS analysis result in Elasticsearch")
            return True
        except Exception as e:
            self.logger.error(f"Error storing BDS analysis result: {type(e).__name__}: {e}")
            return False