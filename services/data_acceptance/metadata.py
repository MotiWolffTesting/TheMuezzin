import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from tinytag import TinyTag
from typing import Dict, Any
from shared.logger import Logger
from services.data_acceptance.config import DataAcceptanceConfig

config = DataAcceptanceConfig.from_env()
logger = Logger.get_logger(
    name="metadata_extractor",
    es_host=config.logger_es_host,
    index=config.logger_index
)

class MetadataGetter:
    """Getting the metadata of the file in question"""
    def __init__(self, file_path: str):
        "Initiate file path"
        self.file_path = file_path

    def extract_metadata(self) -> Dict[str, Any]:
        "Extracting metadata of files"
        try:
            logger.info(f"Extracting metadata from file: {self.file_path}")
            tag = TinyTag.get(self.file_path)
            
            metadata = {
                "filename": tag.filename,
                "filesize": tag.filesize,
                "creation_year": tag.year,
                "duration": tag.duration,
            }
            
            logger.info(f"Metadata extracted successfully - filename: {metadata['filename']}, filesize: {metadata['filesize']} bytes, duration: {metadata['duration']} seconds")
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting metadata from {self.file_path}: {type(e).__name__}: {e}")
            raise
        
        