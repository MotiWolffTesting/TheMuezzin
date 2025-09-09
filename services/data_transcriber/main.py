import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from shared.logger import Logger
from services.data_transcriber.config import DataTranscriberConfig
from services.data_transcriber.mongodb_transcriber import TranscriptionManager


def main() -> int:
    "Main function to run transcriber"
    config = DataTranscriberConfig.from_env()
    logger = Logger.get_logger(
        name="data_transcriber_service",
        es_host=config.logger_es_host,
        index=config.logger_index,
    )
    logger.info("The Muezzin data transcriber service started successfully")

    manager = TranscriptionManager(config=config)
    manager.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

