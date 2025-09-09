import json
from kafka import KafkaConsumer
from typing import List, Any
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from shared.logger import Logger
from shared.bds_analyzer import BDSThreatAnalyzer
from services.bds_analysis.config import BDSAnalysisConfig
from services.bds_analysis.bds_storage import BDSStorageManager

class BDSKafkaConsumer:
    """Kafka consumer for BDS analysis service"""
    
    def __init__(self):
        self.config = BDSAnalysisConfig.from_env()
        self.logger = Logger.get_logger(
            name="bds_kafka_consumer",
            es_host=self.config.logger_es_host,
            index=self.config.logger_index
        )
        
        self.bds_analyzer = BDSThreatAnalyzer()
        self.storage_manager = BDSStorageManager(self.config)
        
        