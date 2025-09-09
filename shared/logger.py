import logging
from elasticsearch import Elasticsearch
from datetime import datetime, timezone
from shared.config import SharedConfig

class Logger:
    _logger = None
    
    config = SharedConfig.load_env()
    @classmethod
    def get_logger(cls, name="the_muezzin_logger", es_host=config.logger_es_host, 
                   index=config.logger_index, level=logging.DEBUG):
        if cls._logger:
            return cls._logger
            
        logger = logging.getLogger(name)
        logger.setLevel(level)
        
        if not logger.handlers:
            # Initialize Elasticsearch connection
            try:
                # Ensure proper URL format for Elasticsearch
                if not es_host.startswith(('http://', 'https://')):
                    es_url = f"http://{es_host}"
                else:
                    es_url = es_host
                    
                es = Elasticsearch([es_url])
                
                class ESHandler(logging.Handler):
                    def emit(self, record):
                        try:
                            es.index(index=index, document={
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "level": record.levelname,
                                "logger": record.name,
                                "message": record.getMessage()
                            })
                        except Exception as e:
                            print(f"ES log failed: {e}")
                
                logger.addHandler(ESHandler())
            except Exception as e:
                print(f"Failed to initialize Elasticsearch handler: {e}")
            
            # Add console handler for development
            logger.addHandler(logging.StreamHandler())
        
        cls._logger = logger
        return logger
