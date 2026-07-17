#!/usr/bin/env python3
"""
One-time script to register fact_ohlcv_1m table in Trino.
The parquet data already exists in MinIO in Hive-style partitioned layout.
"""
import sys
sys.path.insert(0, "/opt/airflow/project")

from src.utils.logger import logger
from trino.dbapi import connect as trino_connect

logger.info("[register_ohlcv] Connecting to Trino...")

conn = trino_connect(
    host="trino", port=8080, user="airflow",
    catalog="hive", schema="gold",
    request_timeout=600,
)
cursor = conn.cursor()

try:
    cursor.execute("DROP TABLE IF EXISTS hive.gold.fact_ohlcv_1m")
    cursor.fetchall()
    logger.info("[register_ohlcv] Dropped old table")
except Exception as e:
    logger.warning(f"[register_ohlcv] Drop failed: {e}")

cursor.execute("""
    CREATE TABLE hive.gold.fact_ohlcv_1m (
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
logger.info("[register_ohlcv] Table created in Trino")

cursor.execute("SELECT count(*) as rows FROM hive.gold.fact_ohlcv_1m")
result = cursor.fetchone()
logger.info(f"[register_ohlcv] After CREATE: {result[0]} rows")

if result[0] == 0:
    logger.info("[register_ohlcv] 0 rows — syncing partition metadata...")
    try:
        cursor.execute(
            "CALL system.sync_partition_metadata('gold', 'fact_ohlcv_1m', 'FULL')"
        )
        repair_result = cursor.fetchall()
        logger.info(f"[register_ohlcv] sync_partition_metadata result: {repair_result}")
    except Exception as e:
        logger.warning(f"[register_ohlcv] sync_partition_metadata failed: {e}")

    cursor.execute("SELECT count(*) as rows FROM hive.gold.fact_ohlcv_1m")
    result = cursor.fetchone()
    logger.info(f"[register_ohlcv] After sync: {result[0]} rows")

cursor.execute("SELECT distinct symbol FROM hive.gold.fact_ohlcv_1m")
symbols = [row[0] for row in cursor.fetchall()]
logger.info(f"[register_ohlcv] Symbols: {symbols}")

cursor.close()
conn.close()
logger.info("[register_ohlcv] Complete!")
