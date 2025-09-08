from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class FilePathGetter:
    """Reading file based on its exact path in local folder"""
    def __init__(self, file_path: str) -> None:
        "Initiate path"
        self.file_path: str = file_path

    def get_file_path(self) -> str:
        "Validates a given file and returns a Path object"
        # Convert to Path object
        path: Path = Path(self.file_path)

        # Check if path exists
        if not path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}.")
        if not path.is_file():
            raise ValueError(f"The provided path is not a file: {self.file_path}.")

        # Check if file is .wav
        if path.suffix.lower() != '.wav':
            raise ValueError(f"File is not a .wav: {self.file_path}")

        return str(path)
    
    def read_file_content(self) -> bytes:
        """Read and return the binary content of the file"""
        try:
            validated_path = self.get_file_path()
            with open(validated_path, 'rb') as file:
                content = file.read()
                logger.info(f"Read {len(content)} bytes from {validated_path}")
                return content
        except Exception as e:
            logger.error(f"Error reading file content: {e}")
            raise