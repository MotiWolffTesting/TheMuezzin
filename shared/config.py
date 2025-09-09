import os
from dataclasses import dataclass

@dataclass
class SharedConfig:
    """Configuration for the shared folder loaded from environment variables."""
    
    # Logger
    logger_es_host: str
    logger_index: str
    
    @staticmethod
    def load_env() -> "SharedConfig":
        return SharedConfig(
            logger_es_host=os.getenv("LOGGER_ES_HOST", "localhost:9200"),
            logger_index=os.getenv("LOGGER_INDEX", "muezzin_logs"),
        )