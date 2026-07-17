from loguru import logger

logger.add("logs/ingestion.log", rotation="50 MB")
