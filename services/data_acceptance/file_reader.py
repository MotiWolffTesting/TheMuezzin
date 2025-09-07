from pathlib import Path

class FilePathGetter:
    """Reading file based on its exact path in local folder"""
    def __init__(self, file_path: str):
        "Initiate path"
        self.file_path = file_path
        
    def get_file_path(self, file_path) -> str:
        "Validates a given file and returns a Path object"
        # Convert to Path object
        path = Path(file_path)
        
        # Check if path exists
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}.")
        if not path.is_file():
            raise ValueError(f"The provided path is not a file: {file_path}.")
        
        # Check if file is .wav
        if path.suffix.lower() != '.wav':
            raise ValueError(f"File is not a .wav: {file_path}")
        
        return str(path)