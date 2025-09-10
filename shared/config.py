import os
from dataclasses import dataclass

@dataclass
class SharedConfig:
    """Configuration for the shared folder loaded from environment variables."""
    
    # Logger
    logger_es_host: str
    logger_index: str
    
    # BDS Analysis thresholds
    bds_threshold_low: float
    bds_threshold_medium: float
    bds_threshold_high: float
    
    @staticmethod
    def load_env() -> "SharedConfig":
        return SharedConfig(
            logger_es_host=os.getenv("LOGGER_ES_HOST", "localhost:9200"),
            logger_index=os.getenv("LOGGER_INDEX", "muezzin_logs"),
            bds_threshold_low=float(os.getenv("BDS_THRESHOLD_LOW", "5.0")),
            bds_threshold_medium=float(os.getenv("BDS_THRESHOLD_MEDIUM", "15.0")),
            bds_threshold_high=float(os.getenv("BDS_THRESHOLD_HIGH", "30.0"))
        )