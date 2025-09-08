from pathlib import Path
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from shared.logger import Logger
from config import DataAcceptanceConfig

config = DataAcceptanceConfig.from_env()
logger = Logger.get_logger(
    name="file_reader",
    es_host=config.logger_es_host,
    index=config.logger_index
)

class FilePathGetter:
    """Reading file based on its exact path in local folder"""
    def __init__(self, file_path: str) -> None:
        "Initiate path"
        self.file_path: str = file_path

    def get_file_path(self) -> str:
        "Validates a given file and returns a Path object"
        logger.info(f"Validating file path: {self.file_path}")
        # Convert to Path object
        path: Path = Path(self.file_path)

        # Check if path exists
        if not path.exists():
            logger.error(f"File not found at path: {self.file_path}")
            raise FileNotFoundError(f"File not found: {self.file_path}.")
        if not path.is_file():
            logger.error(f"Path is not a file: {self.file_path}")
            raise ValueError(f"The provided path is not a file: {self.file_path}.")

        # Check if file is .wav
        if path.suffix.lower() != '.wav':
            logger.error(f"File is not a .wav file: {self.file_path}")
            raise ValueError(f"File is not a .wav: {self.file_path}")

        logger.info(f"File path validation successful: {self.file_path}")
        return str(path)
    
    def read_file_content(self) -> bytes:
        "Read and return the binary content of the file"
        try:
            logger.info(f"Reading file content from: {self.file_path}")
            validated_path = self.get_file_path()
            with open(validated_path, 'rb') as file:
                content = file.read()
                logger.info(f"Successfully read {len(content)} bytes from {validated_path}")
                return content
        except Exception as e:
            logger.error(f"Error reading file content from {self.file_path}: {type(e).__name__}: {e}")
            raise