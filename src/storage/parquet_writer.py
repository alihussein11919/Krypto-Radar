"""
Parquet Writer

Converts raw JSON responses into structured Parquet files.
This is the JSON → columnar conversion step — data is still
bronze (no business logic), just in an analytics-friendly format.

Partitioning strategy
---------------------
- Snapshot datasets (markets, exchanges, global):
    data/parquet/{source}/{dataset}/snapshot_date={date}/part-0.parquet

- Per-coin datasets (market_chart, ohlc):
    data/parquet/{source}/{dataset}/coin_id={coin_id}/part-0.parquet

Why Parquet?
------------
- Columnar: reads only the columns you query, not full rows
- Compressed: typically 5-10x smaller than equivalent JSON
- Typed: preserves int/float/timestamp types unlike CSV
- Universal: readable by Spark, DuckDB, pandas, Databricks
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from src.utils.logger import logger

PARQUET_BASE = Path("data/parquet")


def write_parquet(
    df: pd.DataFrame,
    source: str,
    dataset: str,
    partition_col: str,
    partition_val: str
) -> str:
    """
    Write a DataFrame to a partitioned Parquet file.

    Parameters
    ----------
    df : pd.DataFrame
        Data to write. Should already be flattened/normalized.

    source : str
        e.g. "coingecko"

    dataset : str
        e.g. "markets", "market_chart", "ohlc"

    partition_col : str
        Partition column name, e.g. "snapshot_date" or "coin_id"

    partition_val : str
        Partition value, e.g. "2026-06-24" or "bitcoin"

    Returns
    -------
    str
        Path of the written Parquet file.
    """

    output_dir = PARQUET_BASE / source / dataset / f"{partition_col}={partition_val}"
    output_dir.mkdir(parents=True, exist_ok=True)

    filepath = output_dir / "part-0.parquet"

    df.to_parquet(filepath, index=False, engine="pyarrow", compression="snappy")

    logger.info(
        f"[parquet_writer] Written: {filepath} "
        f"({len(df)} rows, {filepath.stat().st_size / 1024:.1f} KB)"
    )

    return str(filepath.resolve())
