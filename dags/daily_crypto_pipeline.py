"""
Daily Crypto Pipeline DAG

Runs every day at 06:00 UTC to:
1. Fetch latest market snapshot from CoinGecko
2. Convert bronze JSON -> silver Parquet
3. Transform silver -> gold star schema in MinIO
4. Refresh Hive Metastore partitions

Schedule: 0 6 * * * (daily at 06:00 UTC)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "crypto-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _ingest_latest():
    """Fetch latest market data from CoinGecko REST API."""
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from src.api.client import CoinGeckoClient
    from src.ingestion.latest import run_latest

    client = CoinGeckoClient()
    run_latest(client)


def _convert_to_parquet():
    """Convert bronze JSON files to silver Parquet."""
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from src.storage.convert_to_parquet import run_conversion

    run_conversion()


def _transform_gold():
    """Transform silver Parquet into gold star schema tables in MinIO using pandas + pyarrow."""
    import sys
    sys.path.insert(0, "/opt/airflow/project")

    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    import pandas as pd
    from pathlib import Path
    from src.utils.logger import logger

    SILVER_BASE = Path("/opt/airflow/project/data/parquet/coingecko")
    GOLD_BASE = "s3a://crypto-lakehouse/gold"
    S3_ENDPOINT = "http://minio:9000"
    S3_KEY = "minioadmin"
    S3_SECRET = "minioadmin"

    logger.info("[gold] Starting silver -> gold transformation")

    # --- Build dim_coins ---
    markets_path = SILVER_BASE / "markets"
    if not markets_path.exists():
        raise FileNotFoundError(f"Markets data not found at {markets_path}")

    markets_df = pd.read_parquet(markets_path)
    logger.info(f"[gold] Loaded {len(markets_df)} market rows")

    if "ingestion_time" in markets_df.columns:
        markets_df = markets_df.sort_values(["id", "ingestion_time"]).drop_duplicates(
            subset=["id"], keep="last"
        )

    dim_coins = markets_df[["id", "name", "symbol"]].copy()
    dim_coins.columns = ["coin_id", "name", "symbol"]
    if "market_cap_rank" in markets_df.columns:
        dim_coins["market_cap_rank"] = pd.to_numeric(
            markets_df["market_cap_rank"], errors="coerce"
        ).astype("Int64")
    dim_coins = dim_coins.drop_duplicates(subset=["coin_id"]).sort_values(
        "market_cap_rank", na_position="last"
    ).reset_index(drop=True)

    logger.info(f"[gold] dim_coins: {len(dim_coins)} coins")

    # --- Build fact_daily_prices ---
    chart_path = SILVER_BASE / "market_chart"
    today = pd.Timestamp.now(tz="UTC").date()
    today_ts = pd.Timestamp(today)

    # Always include today's snapshot from markets (for new coins not in historical data)
    today_snapshot = markets_df[["id", "current_price", "market_cap", "total_volume"]].copy()
    today_snapshot.columns = ["coin_id", "price_usd", "market_cap_usd", "volume_usd"]
    today_snapshot["date"] = pd.to_datetime(today)
    logger.info(f"[gold] Today's snapshot: {len(today_snapshot)} coins")

    if chart_path.exists() and any(chart_path.iterdir()):
        # Market chart data exists (from historical backfill) - merge with today's snapshot
        logger.info("[gold] Using market_chart data + today's snapshot for fact_daily_prices")
        chart_frames = []
        coin_dirs = [d for d in chart_path.iterdir() if d.is_dir()]
        for coin_dir in coin_dirs:
            coin_id = coin_dir.name
            parquet_files = list(coin_dir.glob("*.parquet"))
            for pf in parquet_files:
                df = pd.read_parquet(pf)
                if "coin_id" not in df.columns:
                    df["coin_id"] = coin_id
                chart_frames.append(df)

        chart_df = pd.concat(chart_frames, ignore_index=True)
        chart_df["date"] = pd.to_datetime(chart_df["timestamp"], unit="ms").dt.date
        chart_df["date"] = pd.to_datetime(chart_df["date"])
        chart_df = chart_df.sort_values(["coin_id", "date"]).reset_index(drop=True)

        # Merge: historical data + today's snapshot (today's data takes precedence for existing coins)
        chart_df = pd.concat([chart_df, today_snapshot], ignore_index=True)
        chart_df = chart_df.sort_values(["coin_id", "date"]).drop_duplicates(
            subset=["coin_id", "date"], keep="last"
        ).reset_index(drop=True)
        logger.info(f"[gold] Merged: {len(chart_df)} rows for {chart_df['coin_id'].nunique()} coins")
    else:
        # Daily pipeline only: build fact_daily_prices from markets snapshot
        logger.info("[gold] No market_chart data - building from markets snapshot")
        chart_df = today_snapshot

    # Build dim_date
    unique_dates = sorted(chart_df["date"].unique())
    dim_date = pd.DataFrame({"date": unique_dates})
    dim_date["date"] = pd.to_datetime(dim_date["date"])
    dim_date["date_id"] = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["date"].dt.year.astype(int)
    dim_date["month"] = dim_date["date"].dt.month.astype(int)
    dim_date["day"] = dim_date["date"].dt.day.astype(int)
    dim_date["day_of_week"] = dim_date["date"].dt.day_name()
    dim_date["is_weekend"] = dim_date["date"].dt.dayofweek.isin([5, 6])
    dim_date["month_name"] = dim_date["date"].dt.month_name()
    dim_date["quarter"] = dim_date["date"].dt.quarter.astype(int)

    logger.info(f"[gold] dim_date: {len(dim_date)} dates")

    # Build fact_daily_prices
    chart_df["date"] = pd.to_datetime(chart_df["date"])
    dim_date_lookup = dim_date[["date", "date_id"]].copy()
    fact = chart_df.merge(dim_date_lookup, on="date", how="left")

    # Compute daily_return
    fact = fact.sort_values(["coin_id", "date"])
    fact["prev_price"] = fact.groupby("coin_id")["price_usd"].shift(1)
    fact["daily_return"] = (fact["price_usd"] - fact["prev_price"]) / fact["prev_price"]
    fact["daily_return"] = fact["daily_return"].where(fact["prev_price"].notna() & (fact["prev_price"] != 0))

    fact_daily = fact[["date_id", "date", "price_usd", "market_cap_usd", "volume_usd", "daily_return", "coin_id"]].copy()
    logger.info(f"[gold] fact_daily_prices: {len(fact_daily)} rows")

    # --- Write to MinIO using PyArrow S3AFileSystem ---
    logger.info("[gold] Writing to MinIO...")

    import boto3
    from io import BytesIO
    import pyarrow.parquet as pq

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
    )
    BUCKET = "crypto-lakehouse"

    def _write_table_to_s3(table, prefix):
        buf = BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        s3.upload_fileobj(buf, BUCKET, f"gold/{prefix}/part-0.parquet")

    # Write dim_coins
    table = pa.Table.from_pandas(dim_coins)
    _write_table_to_s3(table, "dim_coins")
    logger.info("[gold] Written dim_coins")

    # Write dim_date
    table = pa.Table.from_pandas(dim_date)
    date_idx = table.schema.get_field_index("date")
    table = table.set_column(
        date_idx, "date", table.column("date").cast(pa.date32())
    )
    _write_table_to_s3(table, "dim_date")
    logger.info("[gold] Written dim_date")

    # Write fact_daily_prices as a single flat Parquet file (coin_id is a data column)
    fact_daily_export = fact_daily.copy()
    fact_daily_export["date"] = pd.to_datetime(fact_daily_export["date"])
    table = pa.Table.from_pandas(fact_daily_export)
    # Cast date column to date32 for Trino DATE type compatibility
    date_idx = table.schema.get_field_index("date")
    table = table.set_column(
        date_idx, "date", table.column("date").cast(pa.date32())
    )
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3.upload_fileobj(buf, BUCKET, "gold/fact_daily_prices/part-0.parquet")
    logger.info(f"[gold] Written fact_daily_prices ({len(fact_daily_export)} rows)")
    logger.info("[gold] Silver -> Gold transformation complete")


def _refresh_hive():
    """Refresh Hive Metastore tables using Trino Python client."""
    import sys
    sys.path.insert(0, "/opt/airflow/project")

    from trino.dbapi import connect as trino_connect
    from src.utils.logger import logger

    logger.info("[hive] Connecting to Trino for table registration...")

    conn = trino_connect(
        host="trino", port=8080, user="airflow",
        catalog="hive", schema="gold",
    )
    cursor = conn.cursor()

    # Ensure fact_daily_prices table exists (flat file, not partitioned)
    try:
        logger.info("[hive] Ensuring gold.fact_daily_prices table exists...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hive.gold.fact_daily_prices (
                coin_id VARCHAR,
                date_id INTEGER,
                date DATE,
                price_usd DOUBLE,
                market_cap_usd DOUBLE,
                volume_usd DOUBLE,
                daily_return DOUBLE
            ) WITH (
                external_location = 's3a://crypto-lakehouse/gold/fact_daily_prices',
                format = 'PARQUET'
            )
        """)
        cursor.fetchall()
        logger.info("[hive] gold.fact_daily_prices table ready")
    except Exception as e:
        logger.warning(f"[hive] CREATE TABLE fact_daily_prices failed (may already exist): {e}")

    # Ensure fact_ohlcv_1m table exists (populated by Binance WebSocket pipeline)
    try:
        logger.info("[hive] Ensuring gold.fact_ohlcv_1m table exists...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hive.gold.fact_ohlcv_1m (
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                trade_count BIGINT,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                symbol VARCHAR
            ) WITH (
                external_location = 's3a://crypto-lakehouse/gold/fact_ohlcv_1m',
                format = 'PARQUET',
                partitioned_by = ARRAY['symbol']
            )
        """)
        cursor.fetchall()
        logger.info("[hive] gold.fact_ohlcv_1m table ready")

        cursor.execute(
            "CALL system.sync_partition_metadata('gold', 'fact_ohlcv_1m', 'FULL')"
        )
        cursor.fetchall()
        logger.info("[hive] gold.fact_ohlcv_1m partitions synced")
    except Exception as e:
        logger.warning(f"[hive] CREATE TABLE fact_ohlcv_1m failed (may already exist): {e}")

    cursor.close()
    conn.close()
    logger.info("[hive] Hive refresh complete")


with DAG(
    dag_id="daily_crypto_pipeline",
    default_args=default_args,
    description="Daily crypto data ingestion and gold layer refresh",
    schedule="0 6 * * *",
    start_date=datetime(2026, 7, 17),
    catchup=False,
    tags=["crypto", "daily"],
) as dag:

    ingest_latest = PythonOperator(
        task_id="ingest_latest",
        python_callable=_ingest_latest,
        doc="Fetch latest market data from CoinGecko (coins_list, global, markets, exchanges)",
    )

    convert_to_parquet = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=_convert_to_parquet,
        doc="Convert bronze JSON responses to silver Parquet files",
    )

    transform_gold = PythonOperator(
        task_id="transform_gold",
        python_callable=_transform_gold,
        doc="Transform silver Parquet into gold star schema tables in MinIO",
    )

    refresh_hive = PythonOperator(
        task_id="refresh_hive",
        python_callable=_refresh_hive,
        doc="Tell Hive Metastore about new partitions via Trino",
    )

    ingest_latest >> convert_to_parquet >> transform_gold >> refresh_hive
