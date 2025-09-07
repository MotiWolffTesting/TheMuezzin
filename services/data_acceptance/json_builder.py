import json
import logging

logger = logging.getLogger(__name__)

class JsonBuilder:
    """Builds the json out of the metadata dictionary and validated file path"""
    def __init__(self, metadata_dict: dict, validated_path: str):
        self.metadata_dict = metadata_dict
        self.validated_path = validated_path

    def build_json(self) -> dict:
        "Build the json out of the metadata and validated file path"
        try:
            # Combine metadata and validated path
            output = {
                'metadata': self.metadata_dict,
                'validated_path': self.validated_path
            }
            json_string = json.dumps(output, indent=2)
            return json.loads(json_string)
        
        except Exception as e:
            logger.error(f"Error parsing dict to json: {e}")
            raise
        