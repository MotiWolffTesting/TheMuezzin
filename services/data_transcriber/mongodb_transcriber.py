import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from typing import Optional
import time
from pymongo import MongoClient
from elasticsearch import Elasticsearch

from shared.logger import Logger
from .config import DataTranscriberConfig
from .transcriber import AudioTranscriber

class TranscriptionManager:
    """Handles reading audio from MongoDB, transcribing, and updating MongoDB and Elasticsearch"""
    def __init__(self, config: DataTranscriberConfig) -> None:
        "Initializing TranscriptionManager variables"
        self.config = config
        self.logger = Logger.get_logger(
            name="data_transcriber_service",
            es_host=config.logger_es_host,
            index=config.logger_index
        )
        
        self.transcriber = AudioTranscriber(config)
        self.mongo_client = self._create_mongo_client(config.mongodb_uri)
        self.mongo_db = self.mongo_client[config.mongodb_db_name]
        self.collection = self.mongo_db[config.mongodb_collection_name]
        self.es = self._create_es_client(config.elasticsearch_host, config.elasticsearch_port)
        
    def _create_mongo_client(self, uri: str) -> MongoClient:
        "Connect to MongoDB Client"
        self.logger.info(f"Connecting to MongoDB at {uri}")
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        try:
            # Test connection
            client.admin.command('ping')
            self.logger.info("MongoDB connection established successfully")
        except Exception as e:
            self.logger.error(f"MongoDB connection failed: {type(e).__name__}: {e}")
            raise
        return client
    
    def _create_es_client(self, host: str, port: str) -> Elasticsearch:
        "Connect to ElasticSearch"
        self.logger.info(f"Connecting to ElasticSearch at {host}:{port}...")
        try:
            # Connect to ES
            es = Elasticsearch(
                hosts=[{"host": host, "port": int(port), "scheme": "http"}],
                verify_certs=False,
                ssl_show_warn=False
            )
            es.info()
            self.logger.info("ElasticSearch connection established successfully!")
            return es
        except Exception as e:
            self.logger.error(f"ElasticSearch connection failed: {type(e).__name__}: {e}")
            raise
    
    def _fetch_next_document(self) -> Optional[dict]:
        "Fetch a document that has not been transcribed yet"
        return self.collection.find_one({"transcription_status": {"$ne": "processed"}}, sort=[("processed_at", -1)])

    def _extract_filename(self, doc: dict) -> str:
        "Helper function to extract the filename from document metadata, ensuring the metadata is correctly received and parsed"
        metadata = doc.get("metadata", {})
        
        if isinstance(metadata, dict):
            # Some documents may have metadata nested within another "metadata" key.
            inner_metadata = metadata.get("metadata")
            nested = inner_metadata if isinstance(inner_metadata, dict) else {}
            return (
                nested.get("filename")
                or metadata.get("filename")
                or str(doc.get("id") or doc.get("unique_id") or "unknown.wav")
            )
        return str(doc.get("id") or doc.get("unique_id") or "unknown.wav")
        
    def _update_es_transcription(self, doc_id: str, text: Optional[str]) -> None:
        "Add the trascribed data to the metadata in Elastic"
        try:
            self.es.update(index=self.config.elasticsearch_index, id=doc_id, doc={"transcription": text})
            self.logger.info(f"Transcription updated in Elasticsearch for id={doc_id}")
        except Exception as e:
            self.logger.error(f"Failed to update ElasticSearch for id={doc_id}: {type(e).__name__}: {e}")
        
    def run(self) -> None:
        "Run the above processes"
        self.logger.info("Transcription Manager started successfully...")
        while True:
            try:
                # Get the document one by one in a loop
                doc = self._fetch_next_document()
                if not doc:
                    time.sleep(5)
                    continue
                
                # Set doc
                doc_id = doc.get("id") or doc.get("unique_id")
                audio_bytes = doc.get("data")
                filename = self._extract_filename(doc)
                
                # Check if the the above exists, if not - update the status
                if not doc_id or not audio_bytes:
                    self.logger.error(f"Missing required fields for transcription. id={doc_id} has_audio={bool(audio_bytes)}")
                    self.collection.update_one({"_id": doc["_id"]}, {"$set": {"transcription_status": "invalid"}})
                    continue
                
                # Transcribe audio to text using AudioTranscriber
                self.logger.info(f"Starting transcription for doc={doc_id} ({filename})")
                text: Optional[str] = self.transcriber.transcribe_audio_data(audio_bytes, filename)
                
                # Update Elasticsearch with transcription (per guidelines)
                self._update_es_transcription(str(doc_id), text)
                self.logger.info(f"Transcription stored in Elasticsearch for id={doc_id}")
                
                # Mark as processed in MongoDB
                self.collection.update_one({"_id": doc["_id"]}, {"$set": {"transcription_status": "processed"}})
            
            except Exception as e:
                self.logger.error(f"Critical error in orchestrator loop: {type(e).__name__}: {e}")
                time.sleep(2)