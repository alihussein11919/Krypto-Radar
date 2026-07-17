CREATE DATABASE IF NOT EXISTS gold;

CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_coins (
    coin_id STRING,
    name STRING,
    symbol STRING,
    market_cap_rank INT
)
STORED AS PARQUET
LOCATION 's3a://crypto-lakehouse/gold/dim_coins/';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_date (
    date DATE,
    date_id INT,
    year INT,
    month INT,
    day INT,
    day_of_week STRING,
    is_weekend BOOLEAN,
    month_name STRING,
    quarter INT
)
STORED AS PARQUET
LOCATION 's3a://crypto-lakehouse/gold/dim_date/';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.fact_daily_prices (
    date_id INT,
    date DATE,
    price_usd DOUBLE,
    market_cap_usd DOUBLE,
    volume_usd DOUBLE,
    daily_return DOUBLE
)
PARTITIONED BY (coin_id STRING)
STORED AS PARQUET
LOCATION 's3a://crypto-lakehouse/gold/fact_daily_prices/';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.fact_ohlcv_1m (
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    trade_count BIGINT,
    window_start TIMESTAMP,
    window_end TIMESTAMP
)
PARTITIONED BY (symbol STRING)
STORED AS PARQUET
LOCATION 's3a://crypto-lakehouse/gold/fact_ohlcv_1m/';
