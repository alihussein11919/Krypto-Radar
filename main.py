"""
Entry point for the crypto ingestion pipeline.
"""

from src.api.client import CoinGeckoClient
from src.ingestion.latest import run_latest
from src.ingestion.historical import run_historical
from src.storage.convert_to_parquet import run_conversion
from src.streaming.binance_ws import run_stream
from src.utils.logger import logger


def main():
    logger.info("=== Crypto Ingestion Pipeline Starting ===")

    client = CoinGeckoClient()

    run_latest(client)
    run_historical(client, coin_limit=100)
    run_conversion()

    # Run Binance stream for 30 seconds as a test
    logger.info("=== Starting Binance WebSocket stream (30s test) ===")
    run_stream(duration=30)

    logger.info("=== Crypto Ingestion Pipeline Done ===")


if __name__ == "__main__":
    main()
