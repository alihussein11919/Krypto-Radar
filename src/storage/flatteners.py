"""
JSON Flatteners

Each function takes a raw CoinGecko JSON response (as a Python
dict or list) and returns a flat pandas DataFrame ready for
Parquet storage.

Why flattening is needed
------------------------
CoinGecko responses are deeply nested. For example, market_chart
returns this structure:

    {
      "prices": [[timestamp, price], [timestamp, price], ...],
      "market_caps": [[timestamp, market_cap], ...],
      "total_volumes": [[timestamp, volume], ...]
    }

That's three parallel arrays that need to be zipped into rows:

    timestamp | price | market_cap | volume
    --------- | ----- | ---------- | ------
    ...       | ...   | ...        | ...

Each flattener handles one endpoint's specific shape.
"""

import pandas as pd
from datetime import datetime, timezone


def _ms_to_dt(ms: int) -> datetime:
    """Convert a millisecond UNIX timestamp to a UTC datetime."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def flatten_markets(data: list, ingestion_time: datetime = None) -> pd.DataFrame:
    """
    Flatten /coins/markets response.

    Input: list of coin market objects
    Output: one row per coin with all market fields as columns
    """
    if ingestion_time is None:
        ingestion_time = datetime.now(timezone.utc)

    df = pd.json_normalize(data)
    df["ingestion_time"] = ingestion_time

    return df


def flatten_global_market(data: dict, ingestion_time: datetime = None) -> pd.DataFrame:
    """
    Flatten /global response.

    Input: {"data": {...}} wrapper with nested market cap/volume dicts
    Output: one row with scalar fields only (drops nested currency dicts)
    """
    if ingestion_time is None:
        ingestion_time = datetime.now(timezone.utc)

    # Unwrap the "data" key
    inner = data.get("data", data)

    # Keep only scalar fields — drop the nested currency breakdown dicts
    # (total_market_cap, total_volume, market_cap_percentage are dicts
    #  with one entry per currency — too wide to be useful as columns)
    scalar_fields = {
        k: v for k, v in inner.items()
        if not isinstance(v, dict)
    }

    scalar_fields["ingestion_time"] = ingestion_time

    return pd.DataFrame([scalar_fields])


def flatten_market_chart(data: dict, coin_id: str, ingestion_time: datetime = None) -> pd.DataFrame:
    """
    Flatten /coins/{id}/market_chart response.

    Input: {"prices": [...], "market_caps": [...], "total_volumes": [...]}
    Output: one row per timestamp with price, market_cap, volume columns
    """
    if ingestion_time is None:
        ingestion_time = datetime.now(timezone.utc)

    prices = data.get("prices", [])
    market_caps = data.get("market_caps", [])
    volumes = data.get("total_volumes", [])

    rows = []
    for (ts, price), (_, market_cap), (_, volume) in zip(prices, market_caps, volumes):
        rows.append({
            "coin_id": coin_id,
            "timestamp": _ms_to_dt(ts),
            "price_usd": price,
            "market_cap_usd": market_cap,
            "volume_usd": volume,
            "ingestion_time": ingestion_time
        })

    return pd.DataFrame(rows)


def flatten_ohlc(data: list, coin_id: str, ingestion_time: datetime = None) -> pd.DataFrame:
    """
    Flatten /coins/{id}/ohlc response.

    Input: [[timestamp, open, high, low, close], ...]
    Output: one row per candle with named columns
    """
    if ingestion_time is None:
        ingestion_time = datetime.now(timezone.utc)

    rows = []
    for candle in data:
        ts, open_, high, low, close = candle
        rows.append({
            "coin_id": coin_id,
            "timestamp": _ms_to_dt(ts),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "ingestion_time": ingestion_time
        })

    return pd.DataFrame(rows)


def flatten_exchanges(data: list, ingestion_time: datetime = None) -> pd.DataFrame:
    """
    Flatten /exchanges response.

    Input: list of exchange objects
    Output: one row per exchange
    """
    if ingestion_time is None:
        ingestion_time = datetime.now(timezone.utc)

    df = pd.json_normalize(data)
    df["ingestion_time"] = ingestion_time

    return df
