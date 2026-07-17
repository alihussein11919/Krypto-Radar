#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SQL_FILE="$SCRIPT_DIR/register_gold_tables.sql"
CONTAINER="crypto-metastore"

echo "Registering gold tables in Hive Metastore..."

docker cp "$SQL_FILE" "$CONTAINER:/tmp/register_gold_tables.sql"

docker exec "$CONTAINER" hive -f /tmp/register_gold_tables.sql

echo "Repairing partitions..."
docker exec "$CONTAINER" hive -e "
USE gold;
MSCK REPAIR TABLE fact_daily_prices;
MSCK REPAIR TABLE fact_ohlcv_1m;
"

echo "Gold tables registered successfully!"
