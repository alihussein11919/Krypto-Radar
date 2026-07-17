"""
Historical Data Ingestion

Fetches per-coin historical data from CoinGecko and persists
raw responses to the bronze layer.

Endpoints covered
-----------------
- /coins/{id}/market_chart  → price, market cap, volume history
- /coins/{id}/ohlc          → OHLC candles

Credit cost
-----------
Each coin × each endpoint = 1 credit.
With a coin list of 1000, fetching both endpoints = 2000 credits.
Checkpoint ensures we never re-fetch completed coins.
"""

import time
import json
from pathlib import Path

from src.api.client import CoinGeckoClient
from src.api import endpoints
from src.storage.json_writer import write_json
from src.storage.checkpoint import load_completed, mark_completed
from src.utils.logger import logger

SOURCE = "coingecko"

# Delay between requests to stay under rate limit
REQUEST_DELAY = 2.0

# Historical window to fetch (6 months — CoinGecko free tier max)
MARKET_CHART_DAYS = 180

# OHLC supports: 1, 7, 14, 30, 90, 180, 365
OHLC_DAYS = 180


def _load_coin_ids(markets_dir: str = "data/raw/coingecko/markets") -> list[str]:
    """
    Read coin IDs from the already-fetched markets JSON files.
    This avoids spending a credit on coins/list again.

    Returns a deduplicated ordered list of coin IDs sorted by
    market cap rank (order they appeared in the markets pages).
    """
    markets_path = Path(markets_dir)
    coin_ids = []
    seen = set()

    # Walk all date folders and collect files in sorted order
    for json_file in sorted(markets_path.rglob("*.json")):
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        for coin in data:
            cid = coin.get("id")
            if cid and cid not in seen:
                coin_ids.append(cid)
                seen.add(cid)

    logger.info(f"[historical] Loaded {len(coin_ids)} coin IDs from markets cache.")
    return coin_ids


def fetch_market_chart(client: CoinGeckoClient, coin_ids: list[str]) -> None:
    """
    Fetch price/market_cap/volume history for each coin.
    Skips coins already in the checkpoint.
    """
    completed = load_completed("market_chart")
    remaining = [c for c in coin_ids if c not in completed]

    logger.info(f"[historical] market_chart: {len(remaining)} coins to fetch "
                f"({len(completed)} already done).")

    for i, coin_id in enumerate(remaining, 1):
        try:
            endpoint = endpoints.COIN_MARKET_CHART.format(id=coin_id)

            data = client.get(
                endpoint,
                params={"vs_currency": "usd", "days": MARKET_CHART_DAYS}
            )

            write_json(data, source=SOURCE, dataset=f"market_chart/{coin_id}")
            mark_completed("market_chart", coin_id)

            logger.info(f"[historical] market_chart {i}/{len(remaining)}: {coin_id} done.")

        except Exception as e:
            logger.error(f"[historical] market_chart FAILED for {coin_id}: {e}")

        time.sleep(REQUEST_DELAY)


def fetch_ohlc(client: CoinGeckoClient, coin_ids: list[str]) -> None:
    """
    Fetch OHLC candle data for each coin.
    Skips coins already in the checkpoint.
    """
    completed = load_completed("ohlc")
    remaining = [c for c in coin_ids if c not in completed]

    logger.info(f"[historical] ohlc: {len(remaining)} coins to fetch "
                f"({len(completed)} already done).")

    for i, coin_id in enumerate(remaining, 1):
        try:
            endpoint = endpoints.COIN_OHLC.format(id=coin_id)

            data = client.get(
                endpoint,
                params={"vs_currency": "usd", "days": OHLC_DAYS}
            )

            write_json(data, source=SOURCE, dataset=f"ohlc/{coin_id}")
            mark_completed("ohlc", coin_id)

            logger.info(f"[historical] ohlc {i}/{len(remaining)}: {coin_id} done.")

        except Exception as e:
            logger.error(f"[historical] ohlc FAILED for {coin_id}: {e}")

        time.sleep(REQUEST_DELAY)


def run_historical(client: CoinGeckoClient, coin_limit: int = 100) -> None:
    """
    Run historical ingestion for the top N coins by market cap.

    Parameters
    ----------
    client : CoinGeckoClient
        Authenticated HTTP client.

    coin_limit : int
        How many coins to fetch. Defaults to 100 to be
        conservative with credits. Raise when comfortable.
    """
    logger.info(f"=== [historical] Starting historical ingestion (top {coin_limit} coins) ===")

    coin_ids = _load_coin_ids()[:coin_limit]

    fetch_market_chart(client, coin_ids)
    fetch_ohlc(client, coin_ids)

    logger.info("=== [historical] Historical ingestion complete ===")
