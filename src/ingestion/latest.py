"""
Latest Market Data Ingestion

Fetches current/latest data from CoinGecko endpoints and
persists raw responses to the bronze layer via json_writer.

Endpoints covered
-----------------
- /coins/list          → full list of coins with id, symbol, name
- /global              → global crypto market snapshot
- /coins/markets       → paginated market data for top N coins
- /exchanges           → list of exchanges
"""

import time
from src.api.client import CoinGeckoClient
from src.api import endpoints
from src.storage.json_writer import write_json
from src.utils.logger import logger

SOURCE = "coingecko"

# How many coins per page for /coins/markets (max 250 on free tier)
MARKETS_PAGE_SIZE = 250

# How many pages to fetch (250 * 4 = top 1000 coins)
MARKETS_PAGES = 4

# Delay between paginated requests to respect rate limits (seconds)
PAGE_DELAY = 2.0


def fetch_coins_list(client: CoinGeckoClient) -> None:
    """
    Fetch the full list of coins from CoinGecko.
    One request, one file — no pagination needed.
    """
    logger.info("[latest] Fetching coins list...")

    data = client.get(endpoints.COINS_LIST)

    write_json(data, source=SOURCE, dataset="coins_list")

    logger.info(f"[latest] coins_list: {len(data)} coins received.")


def fetch_global_market(client: CoinGeckoClient) -> None:
    """
    Fetch the global crypto market snapshot.
    One request, one file.
    """
    logger.info("[latest] Fetching global market snapshot...")

    data = client.get(endpoints.GLOBAL)

    write_json(data, source=SOURCE, dataset="global_market")

    logger.info("[latest] global_market: snapshot received.")


def fetch_markets(client: CoinGeckoClient) -> None:
    """
    Fetch paginated market data for the top coins.

    Each page is written as a separate file so the bronze layer
    preserves exactly what the API returned per request.
    """
    logger.info(f"[latest] Fetching markets ({MARKETS_PAGES} pages x {MARKETS_PAGE_SIZE} coins)...")

    for page in range(1, MARKETS_PAGES + 1):
        logger.info(f"[latest] markets page {page}/{MARKETS_PAGES}...")

        data = client.get(
            endpoints.COIN_MARKETS,
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": MARKETS_PAGE_SIZE,
                "page": page,
                "sparkline": False,
                "price_change_percentage": "24h"
            }
        )

        write_json(data, source=SOURCE, dataset="markets")

        logger.info(f"[latest] markets page {page}: {len(data)} coins received.")

        if page < MARKETS_PAGES:
            time.sleep(PAGE_DELAY)

    logger.info("[latest] markets: all pages done.")


def fetch_exchanges(client: CoinGeckoClient) -> None:
    """
    Fetch the list of exchanges from CoinGecko.
    """
    logger.info("[latest] Fetching exchanges...")

    data = client.get(
        endpoints.EXCHANGES,
        params={"per_page": 250, "page": 1}
    )

    write_json(data, source=SOURCE, dataset="exchanges")

    logger.info(f"[latest] exchanges: {len(data)} exchanges received.")


def run_latest(client: CoinGeckoClient) -> None:
    """
    Run all latest-data fetchers in sequence.

    Parameters
    ----------
    client : CoinGeckoClient
        Authenticated HTTP client to use for all requests.
    """
    logger.info("=== [latest] Starting latest data ingestion ===")

    fetch_coins_list(client)
    time.sleep(PAGE_DELAY)

    fetch_global_market(client)
    time.sleep(PAGE_DELAY)

    fetch_markets(client)
    time.sleep(PAGE_DELAY)

    fetch_exchanges(client)

    logger.info("=== [latest] Latest data ingestion complete ===")
