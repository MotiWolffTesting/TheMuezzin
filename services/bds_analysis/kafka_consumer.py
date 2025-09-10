import json
from kafka import KafkaConsumer
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
        self.consumer = KafkaConsumer(
            self.config.kafka_topic_transcription,
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            group_id=self.config.kafka_consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
        )
    
    def run(self):
        "Consume transcriptions, analyze, and store results"
        self.logger.info("Starting BDS Kafka consumer loop...")
        for message in self.consumer:
            try:
                payload = message.value
                # Expecting payload with fields
                text = payload.get('text', '')
                filename = payload.get('filename', '')
                if not text:
                    self.logger.info("Skipping message with empty text")
                    continue
                # Analyze the text for BDS threats
                result = self.bds_analyzer.analyze_text(text=text, filename=filename)
                self.storage_manager.store_result(result)
            except Exception as e:
                self.logger.error(f"Error processing Kafka message: {type(e).__name__}: {e}")
        
        
        
        