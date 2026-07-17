"""
Bronze JSON → Parquet Converter

Reads all raw JSON files from data/raw/ and converts them
to partitioned Parquet files in data/parquet/.

This is a batch conversion step — run it after ingestion
to make the bronze data available for analytics.

Think of it as the bridge between:
    data/raw/   (immutable archive, exactly what the API returned)
    data/parquet/ (same data, queryable format)
"""

import json
from pathlib import Path
from datetime import datetime, timezone

from src.storage.flatteners import (
    flatten_markets,
    flatten_global_market,
    flatten_market_chart,
    flatten_ohlc,
    flatten_exchanges,
)
from src.storage.parquet_writer import write_parquet
from src.utils.logger import logger

RAW_BASE = Path("data/raw")
SOURCE = "coingecko"


def _load_json(path: Path) -> dict | list:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _latest_files(dataset_dir: Path) -> list[Path]:
    """
    Return all JSON files under a dataset directory, sorted by path.
    """
    return sorted(dataset_dir.rglob("*.json"))


def convert_markets() -> None:
    """
    Convert all markets JSON pages into a single Parquet file
    per snapshot date.

    Multiple pages from the same date are concatenated into
    one DataFrame before writing — so you get one file per day,
    not one file per page.
    """
    logger.info("[convert] Converting markets...")

    dataset_dir = RAW_BASE / SOURCE / "markets"
    if not dataset_dir.exists():
        logger.warning("[convert] markets: no raw data found, skipping.")
        return

    # Group files by date folder
    date_folders = sorted([d for d in dataset_dir.iterdir() if d.is_dir()])

    for date_folder in date_folders:
        date_str = date_folder.name
        pages = sorted(date_folder.glob("*.json"))

        import pandas as pd
        dfs = []
        ingestion_time = datetime.now(timezone.utc)

        for page_file in pages:
            data = _load_json(page_file)
            dfs.append(flatten_markets(data, ingestion_time=ingestion_time))

        combined = pd.concat(dfs, ignore_index=True)

        write_parquet(
            combined,
            source=SOURCE,
            dataset="markets",
            partition_col="snapshot_date",
            partition_val=date_str
        )

        logger.info(f"[convert] markets {date_str}: {len(combined)} rows written.")


def convert_global_market() -> None:
    """
    Convert global market snapshots to Parquet.
    One row per snapshot, partitioned by date.
    """
    logger.info("[convert] Converting global_market...")

    dataset_dir = RAW_BASE / SOURCE / "global_market"
    if not dataset_dir.exists():
        logger.warning("[convert] global_market: no raw data found, skipping.")
        return

    import pandas as pd
    dfs = []
    ingestion_time = datetime.now(timezone.utc)

    for json_file in _latest_files(dataset_dir):
        data = _load_json(json_file)
        dfs.append(flatten_global_market(data, ingestion_time=ingestion_time))

    combined = pd.concat(dfs, ignore_index=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    write_parquet(
        combined,
        source=SOURCE,
        dataset="global_market",
        partition_col="snapshot_date",
        partition_val=date_str
    )

    logger.info(f"[convert] global_market: {len(combined)} rows written.")


def convert_market_chart() -> None:
    """
    Convert per-coin market_chart JSONs to Parquet.
    One Parquet file per coin, partitioned by coin_id.
    """
    logger.info("[convert] Converting market_chart...")

    dataset_dir = RAW_BASE / SOURCE / "market_chart"
    if not dataset_dir.exists():
        logger.warning("[convert] market_chart: no raw data found, skipping.")
        return

    # Each subdirectory is a coin_id
    for coin_dir in sorted(dataset_dir.iterdir()):
        if not coin_dir.is_dir():
            continue

        coin_id = coin_dir.name
        json_files = list(coin_dir.rglob("*.json"))

        if not json_files:
            continue

        import pandas as pd
        dfs = []
        ingestion_time = datetime.now(timezone.utc)

        for json_file in sorted(json_files):
            data = _load_json(json_file)
            dfs.append(flatten_market_chart(data, coin_id=coin_id, ingestion_time=ingestion_time))

        combined = pd.concat(dfs, ignore_index=True)
        # Deduplicate by timestamp in case of overlapping fetches
        combined = combined.drop_duplicates(subset=["coin_id", "timestamp"])

        write_parquet(
            combined,
            source=SOURCE,
            dataset="market_chart",
            partition_col="coin_id",
            partition_val=coin_id
        )

        logger.info(f"[convert] market_chart/{coin_id}: {len(combined)} rows written.")


def convert_ohlc() -> None:
    """
    Convert per-coin OHLC JSONs to Parquet.
    One Parquet file per coin, partitioned by coin_id.
    """
    logger.info("[convert] Converting ohlc...")

    dataset_dir = RAW_BASE / SOURCE / "ohlc"
    if not dataset_dir.exists():
        logger.warning("[convert] ohlc: no raw data found, skipping.")
        return

    for coin_dir in sorted(dataset_dir.iterdir()):
        if not coin_dir.is_dir():
            continue

        coin_id = coin_dir.name
        json_files = list(coin_dir.rglob("*.json"))

        if not json_files:
            continue

        import pandas as pd
        dfs = []
        ingestion_time = datetime.now(timezone.utc)

        for json_file in sorted(json_files):
            data = _load_json(json_file)
            dfs.append(flatten_ohlc(data, coin_id=coin_id, ingestion_time=ingestion_time))

        combined = pd.concat(dfs, ignore_index=True)
        combined = combined.drop_duplicates(subset=["coin_id", "timestamp"])

        write_parquet(
            combined,
            source=SOURCE,
            dataset="ohlc",
            partition_col="coin_id",
            partition_val=coin_id
        )

        logger.info(f"[convert] ohlc/{coin_id}: {len(combined)} rows written.")


def convert_exchanges() -> None:
    """
    Convert exchanges JSON to Parquet.
    """
    logger.info("[convert] Converting exchanges...")

    dataset_dir = RAW_BASE / SOURCE / "exchanges"
    if not dataset_dir.exists():
        logger.warning("[convert] exchanges: no raw data found, skipping.")
        return

    import pandas as pd
    dfs = []
    ingestion_time = datetime.now(timezone.utc)

    for json_file in _latest_files(dataset_dir):
        data = _load_json(json_file)
        dfs.append(flatten_exchanges(data, ingestion_time=ingestion_time))

    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.drop_duplicates(subset=["id"])
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    write_parquet(
        combined,
        source=SOURCE,
        dataset="exchanges",
        partition_col="snapshot_date",
        partition_val=date_str
    )

    logger.info(f"[convert] exchanges: {len(combined)} rows written.")


def run_conversion() -> None:
    """
    Run all converters in sequence.
    Safe to re-run — overwrites existing Parquet files.
    """
    logger.info("=== [convert] Starting JSON → Parquet conversion ===")

    convert_markets()
    convert_global_market()
    convert_market_chart()
    convert_ohlc()
    convert_exchanges()

    logger.info("=== [convert] Conversion complete ===")
