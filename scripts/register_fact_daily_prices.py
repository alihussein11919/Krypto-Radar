#!/usr/bin/env python3
"""
Rebuild fact_daily_prices from silver data (has coin_id column).
Consolidates all historical data into a single flat Parquet file.
"""
import sys
sys.path.insert(0, "/opt/airflow/project")

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from pathlib import Path
from src.utils.logger import logger

S3_ENDPOINT = "http://minio:9000"
S3_KEY = "minioadmin"
S3_SECRET = "minioadmin"
BUCKET = "crypto-lakehouse"
SILVER_BASE = Path("/opt/airflow/project/data/parquet/coingecko")

logger.info("[rebuild] Reading silver market_chart data...")

chart_path = SILVER_BASE / "market_chart"
if not chart_path.exists():
    logger.error(f"[rebuild] Market chart data not found at {chart_path}")
    sys.exit(1)

# Read all coin data
frames = []
coin_dirs = [d for d in chart_path.iterdir() if d.is_dir()]
for coin_dir in coin_dirs:
    coin_id = coin_dir.name
    # Handle coin_id=xxx directory format from historical backfill
    if coin_id.startswith("coin_id="):
        coin_id = coin_id.replace("coin_id=", "")
    parquet_files = list(coin_dir.glob("*.parquet"))
    for pf in parquet_files:
        df = pd.read_parquet(pf)
        if "coin_id" not in df.columns:
            df["coin_id"] = coin_id
        frames.append(df)

if not frames:
    logger.error("[rebuild] No data found!")
    sys.exit(1)

chart_df = pd.concat(frames, ignore_index=True)
logger.info(f"[rebuild] Loaded {len(chart_df)} rows for {chart_df['coin_id'].nunique()} coins")

# Deduplicate
if "ingestion_time" in chart_df.columns:
    chart_df = chart_df.sort_values(["coin_id", "timestamp", "ingestion_time"])
    chart_df = chart_df.drop_duplicates(subset=["coin_id", "timestamp"], keep="last")

# Convert timestamps to dates
chart_df["date"] = pd.to_datetime(chart_df["timestamp"], unit="ms").dt.date
chart_df["date"] = pd.to_datetime(chart_df["date"])
chart_df = chart_df.sort_values(["coin_id", "date"]).reset_index(drop=True)

# Build dim_date
unique_dates = sorted(chart_df["date"].unique())
dim_date = pd.DataFrame({"date": unique_dates})
dim_date["date"] = pd.to_datetime(dim_date["date"])
dim_date["date_id"] = dim_date["date"].dt.strftime("%Y%m%d").astype(int)

# Build fact_daily_prices
dim_date_lookup = dim_date[["date", "date_id"]].copy()
fact = chart_df.merge(dim_date_lookup, on="date", how="left")

# Compute daily_return
fact = fact.sort_values(["coin_id", "date"])
fact["prev_price"] = fact.groupby("coin_id")["price_usd"].shift(1)
fact["daily_return"] = (fact["price_usd"] - fact["prev_price"]) / fact["prev_price"]
fact["daily_return"] = fact["daily_return"].where(fact["prev_price"].notna() & (fact["prev_price"] != 0))

fact_daily = fact[["coin_id", "date_id", "date", "price_usd", "market_cap_usd", "volume_usd", "daily_return"]].copy()
logger.info(f"[rebuild] Built fact_daily_prices: {len(fact_daily)} rows for {fact_daily['coin_id'].nunique()} coins")

# Write to S3 as flat Parquet
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
)

# Clean up any existing data
try:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="gold/fact_daily_prices/"):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
    logger.info("[rebuild] Cleaned up old S3 data")
except Exception as e:
    logger.warning(f"[rebuild] Cleanup failed: {e}")

# Write flat Parquet
fact_daily_export = fact_daily.copy()
table = pa.Table.from_pandas(fact_daily_export)
date_idx = table.schema.get_field_index("date")
table = table.set_column(date_idx, "date", table.column("date").cast(pa.date32()))

buf = BytesIO()
pq.write_table(table, buf)
buf.seek(0)
s3.upload_fileobj(buf, BUCKET, "gold/fact_daily_prices/part-0.parquet")
logger.info(f"[rebuild] Written flat Parquet: {len(fact_daily_export)} rows")

# Register in Trino
from trino.dbapi import connect as trino_connect

conn = trino_connect(
    host="trino", port=8080, user="airflow",
    catalog="hive", schema="gold",
    request_timeout=600,
)
cursor = conn.cursor()

try:
    cursor.execute("DROP TABLE IF EXISTS hive.gold.fact_daily_prices")
    cursor.fetchall()
except Exception:
    pass

cursor.execute("""
    CREATE TABLE hive.gold.fact_daily_prices (
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
logger.info("[rebuild] Created table in Trino")

cursor.execute("SELECT count(*) as rows, count(distinct coin_id) as coins FROM hive.gold.fact_daily_prices")
result = cursor.fetchone()
logger.info(f"[rebuild] Verified: {result[0]} rows, {result[1]} coins")

cursor.close()
conn.close()
logger.info("[rebuild] Complete!")
