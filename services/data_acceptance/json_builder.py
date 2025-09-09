import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import json
from shared.logger import Logger
from .config import DataAcceptanceConfig

config = DataAcceptanceConfig.from_env()
logger = Logger.get_logger(
    name="json_builder",
    es_host=config.logger_es_host,
    index=config.logger_index
)

class JsonBuilder:
    """Builds the json out of the metadata dictionary and validated file path"""
    def __init__(self, metadata_dict: dict, validated_path: str):
        self.metadata_dict = metadata_dict
        self.validated_path = validated_path

    def build_json(self) -> dict:
        "Build the json out of the metadata and validated file path"
        try:
            logger.info("Building JSON structure from metadata and file path")
            # Combine metadata and validated path
            output = {
                'metadata': self.metadata_dict,
                'validated_path': self.validated_path
            }
            json_string = json.dumps(output, indent=2)
            result = json.loads(json_string)
            logger.info("JSON structure built successfully")
            return result
        
        except Exception as e:
            logger.error(f"Error building JSON from metadata: {type(e).__name__}: {e}")
            raise
        