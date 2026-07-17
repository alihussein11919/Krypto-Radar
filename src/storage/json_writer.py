"""
Raw JSON Writer

Persists raw API responses to disk as JSON files.
This is the bronze layer on disk — data is written exactly
as received from the API, with no transformation.

Directory structure:
    data/raw/{source}/{dataset}/{date}/response_{timestamp}.json

Example:
    data/raw/coingecko/coins/2026-06-25/response_20260625_143022.json
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from src.utils.logger import logger


def write_json(
    data: dict | list,
    source: str,
    dataset: str,
    base_dir: str = "data/raw"
) -> str:
    """
    Write a raw API response to a JSON file.

    Parameters
    ----------
    data : dict | list
        Raw API response to persist.

    source : str
        Data source name, used as top-level folder.
        e.g. "coingecko", "binance"

    dataset : str
        Endpoint or dataset name, used as second-level folder.
        e.g. "coins", "markets", "ohlc"

    base_dir : str
        Root directory for raw data. Defaults to "data/raw".

    Returns
    -------
    str
        Absolute path of the file that was written.
    """

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    timestamp_str = now.strftime("%Y%m%d_%H%M%S")

    output_dir = Path(base_dir) / source / dataset / date_str
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"response_{timestamp_str}.json"
    filepath = output_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"[json_writer] Written: {filepath}")

    return str(filepath.resolve())
