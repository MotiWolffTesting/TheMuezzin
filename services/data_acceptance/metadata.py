from tinytag import TinyTag
from typing import Dict, Any

class MetadataGetter:
    """Getting the metadata of the file in question"""
    def __init__(self, file_path: str):
        "Initiate file path"
        self.file_path = file_path

    def extract_metadata(self) -> Dict[str, Any]:
        "Extracting metadata of files"
        tag = TinyTag.get(self.file_path)
        
        return {
            "filename": tag.filename,
            "filesize": tag.filesize,
            "creation_year": tag.year,
            "duration": tag.duration,
        }
        
        