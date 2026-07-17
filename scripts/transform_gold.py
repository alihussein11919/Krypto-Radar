"""
Silver → Gold Transformation (PySpark)

Reads silver Parquet from data/parquet/coingecko/ and writes
star-schema gold tables to MinIO (s3a://crypto-lakehouse/gold/).

Usage
-----
    # Full refresh (overwrites all gold tables)
    spark-submit --master local[*] scripts/transform_gold.py --mode full

    # Incremental (appends new date partitions only)
    spark-submit --master local[*] scripts/transform_gold.py --mode incremental

Gold Tables
-----------
- dim_coins:       coin_id, name, symbol, market_cap_rank
- dim_date:        date, date_id, year, month, day, day_of_week, is_weekend, month_name, quarter
- fact_daily_prices: date_id, date, price_usd, market_cap_usd, volume_usd, daily_return (partitioned by coin_id)
- fact_ohlcv_1m:   (skipped — populated by Binance WebSocket pipeline)
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SILVER_BASE = "data/parquet/coingecko"
GOLD_BASE = "s3a://crypto-lakehouse/gold"


def get_spark(app_name: str = "crypto-gold-transform") -> SparkSession:
    """Create or get the SparkSession with S3A/MinIO configuration."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", ",".join([
            "/opt/spark/jars/hadoop-aws-3.3.4.jar",
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
        ]))
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# dim_coins
# ---------------------------------------------------------------------------
def build_dim_coins(spark: SparkSession) -> DataFrame:
    """
    Build dim_coins from the latest markets snapshot.

    Reads all partitioned markets Parquet files and takes the most
    recent snapshot's coin list as the dimension.
    """
    markets_path = f"{SILVER_BASE}/markets"
    df = spark.read.parquet(markets_path)

    # Keep latest snapshot per coin (by ingestion_time)
    from pyspark.sql.window import Window
    w = Window.partitionBy("id").orderBy(F.col("ingestion_time").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1)

    dim = df.select(
        F.col("id").alias("coin_id"),
        F.col("name").alias("name"),
        F.col("symbol").alias("symbol"),
        F.col("market_cap_rank").cast(IntegerType()).alias("market_cap_rank"),
    ).dropDuplicates(["coin_id"])

    return dim.orderBy("market_cap_rank")


# ---------------------------------------------------------------------------
# dim_date
# ---------------------------------------------------------------------------
def build_dim_date(spark: SparkSession) -> DataFrame:
    """
    Build dim_date from the date range present in market_chart data.

    Generates one row per calendar date spanning the full dataset.
    """
    chart_path = f"{SILVER_BASE}/market_chart"
    chart = spark.read.parquet(chart_path)

    # Extract unique dates from the timestamp column
    dates_df = (
        chart
        .withColumn("date", F.to_date("timestamp"))
        .select("date")
        .distinct()
        .filter(F.col("date").isNotNull())
    )

    # Add date dimension columns
    dim = dates_df.withColumn("date_id", F.date_format("date", "yyyyMMdd").cast(IntegerType()))
    dim = dim.withColumn("year", F.year("date").cast(IntegerType()))
    dim = dim.withColumn("month", F.month("date").cast(IntegerType()))
    dim = dim.withColumn("day", F.dayofmonth("date").cast(IntegerType()))
    dim = dim.withColumn("day_of_week", F.date_format("date", "EEEE"))
    dim = dim.withColumn("is_weekend", F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).cast(BooleanType()))
    dim = dim.withColumn("month_name", F.date_format("date", "MMMM"))
    dim = dim.withColumn("quarter", F.quarter("date").cast(IntegerType()))

    return dim.orderBy("date")


# ---------------------------------------------------------------------------
# fact_daily_prices
# ---------------------------------------------------------------------------
def build_fact_daily_prices(spark: SparkSession, dim_coins: DataFrame, dim_date: DataFrame) -> DataFrame:
    """
    Build fact_daily_prices from market_chart silver data.

    Joins with dim_coins and dim_date, computes daily_return.
    Partitioned by coin_id when written.
    """
    chart_path = f"{SILVER_BASE}/market_chart"
    chart = spark.read.parquet(chart_path)

    # Add date column for joining with dim_date
    chart = chart.withColumn("date", F.to_date("timestamp"))

    # Join with dim_date to get date_id
    chart = chart.join(dim_date.select("date", "date_id"), on="date", how="left")

    # Compute daily_return: (today_price - yesterday_price) / yesterday_price
    from pyspark.sql.window import Window
    w = Window.partitionBy("coin_id").orderBy("date")
    chart = chart.withColumn(
        "prev_price",
        F.lag("price_usd", 1).over(w)
    )
    chart = chart.withColumn(
        "daily_return",
        F.when(
            F.col("prev_price").isNotNull() & (F.col("prev_price") != 0),
            (F.col("price_usd") - F.col("prev_price")) / F.col("prev_price")
        ).otherwise(None)
    )

    fact = chart.select(
        "date_id",
        "date",
        "price_usd",
        "market_cap_usd",
        "volume_usd",
        "daily_return",
        "coin_id",
    )

    return fact


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------
def write_gold(df: DataFrame, table_name: str, partition_cols: list[str] = None) -> None:
    """Write a DataFrame to the gold layer in MinIO."""
    output_path = f"{GOLD_BASE}/{table_name}"

    writer = df.coalesce(4).write.mode("overwrite")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(output_path)
    print(f"[gold] Written {table_name}: {output_path}")


def refresh_hive_table(spark: SparkSession, table_name: str) -> None:
    """Run MSCK REPAIR TABLE to pick up new partitions in Hive Metastore."""
    try:
        spark.sql(f"MSCK REPAIR TABLE gold.{table_name}")
        print(f"[gold] Repaired gold.{table_name}")
    except Exception as e:
        print(f"[gold] MSCK REPAIR failed for {table_name} (may not be partitioned): {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_full(spark: SparkSession) -> None:
    """Full refresh: rebuild all gold tables from scratch."""
    print("=== [gold] Full refresh starting ===")

    dim_coins = build_dim_coins(spark)
    write_gold(dim_coins, "dim_coins")

    dim_date = build_dim_date(spark)
    write_gold(dim_date, "dim_date")

    fact_daily = build_fact_daily_prices(spark, dim_coins, dim_date)
    write_gold(fact_daily, "fact_daily_prices", partition_cols=["coin_id"])

    # fact_ohlcv_1m is populated by the Binance WebSocket pipeline, skip here

    print("=== [gold] Full refresh complete ===")


def run_incremental(spark: SparkSession) -> None:
    """
    Incremental update: rebuild dim_coins and dim_date (small tables),
    then append new date partitions to fact_daily_prices.
    """
    print("=== [gold] Incremental update starting ===")

    # Always rebuild small dimension tables
    dim_coins = build_dim_coins(spark)
    write_gold(dim_coins, "dim_coins")

    dim_date = build_dim_date(spark)
    write_gold(dim_date, "dim_date")

    # For fact_daily_prices, we overwrite the entire table since
    # CoinGecko returns full 180-day history per coin anyway.
    # True incremental (append-only) would require tracking which
    # dates are new — for now, full overwrite is simpler and safe.
    fact_daily = build_fact_daily_prices(spark, dim_coins, dim_date)
    write_gold(fact_daily, "fact_daily_prices", partition_cols=["coin_id"])

    print("=== [gold] Incremental update complete ===")


def main():
    parser = argparse.ArgumentParser(description="Silver → Gold transformation")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="full: overwrite all tables; incremental: rebuild dims, overwrite facts",
    )
    args = parser.parse_args()

    spark = get_spark()

    try:
        if args.mode == "full":
            run_full(spark)
        else:
            run_incremental(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
