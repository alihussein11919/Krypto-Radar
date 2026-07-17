"""
Historical Backfill DAG

One-time or manually triggered DAG to backfill historical data
from CoinGecko for the top 100 coins (180 days).

This DAG is NOT on a schedule — trigger it manually from the
Airflow UI when you want to:
- Backfill historical data for the first time
- Add new coins to the dataset
- Reprocess after schema changes

Schedule: None (manual trigger only)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "crypto-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _ingest_historical():
    """Fetch 180-day historical data for top 100 coins from CoinGecko."""
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from src.api.client import CoinGeckoClient
    from src.ingestion.historical import run_historical

    client = CoinGeckoClient()
    run_historical(client, coin_limit=100)


def _convert_to_parquet():
    """Convert bronze JSON files to silver Parquet."""
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from src.storage.convert_to_parquet import run_conversion

    run_conversion()


with DAG(
    dag_id="historical_backfill",
    default_args=default_args,
    description="One-time historical data backfill from CoinGecko (180 days, top 100 coins)",
    schedule=None,
    start_date=datetime(2026, 7, 17),
    catchup=False,
    tags=["crypto", "historical", "backfill"],
) as dag:

    ingest_historical = PythonOperator(
        task_id="ingest_historical",
        python_callable=_ingest_historical,
        doc="Fetch 180-day historical market_chart and OHLC for top 100 coins",
    )

    convert_to_parquet = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=_convert_to_parquet,
        doc="Convert bronze JSON responses to silver Parquet files",
    )

    transform_gold = BashOperator(
        task_id="transform_gold",
        bash_command=(
            "spark-submit --master local[*] "
            "--driver-memory 2g "
            "--jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar "
            "/opt/airflow/project/scripts/transform_gold.py --mode full"
        ),
        doc="Full rebuild of gold star schema tables in MinIO",
    )

    refresh_hive = BashOperator(
        task_id="refresh_hive",
        bash_command=(
            "docker exec crypto-trino trino "
            "--catalog hive --schema gold "
            "--execute \"MSCK REPAIR TABLE gold.fact_daily_prices\""
        ),
        doc="Tell Hive Metastore about new partitions",
    )

    ingest_historical >> convert_to_parquet >> transform_gold >> refresh_hive
