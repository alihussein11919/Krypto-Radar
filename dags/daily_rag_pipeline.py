"""
Daily RAG Pipeline DAG

Runs after daily_crypto_pipeline to:
1. Query Trino for current price data
2. Compute technical indicators
3. Embed patterns into ChromaDB
4. Generate RAG recommendations via Ollama

Schedule: 0 7 * * * (daily at 07:00 UTC, 1 hour after data ingestion)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "crypto-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _compute_and_store_indicators():
    """Query Trino, compute indicators, embed into ChromaDB."""
    import sys
    sys.path.insert(0, "/opt/airflow/project")

    import pandas as pd
    from trino.dbapi import connect as trino_connect

    from src.analysis.indicators import compute_all_indicators, save_snapshots
    from src.analysis.embedder import store_all_snapshots
    from src.utils.logger import logger

    # Connect to Trino
    conn = trino_connect(
        host="trino", port=8080, user="airflow",
        catalog="hive", schema="gold",
    )

    # Query price data
    logger.info("[rag-dag] Querying Trino for price data...")
    price_df = pd.read_sql(
        "SELECT coin_id, date, price_usd, market_cap_usd, volume_usd, daily_return "
        "FROM fact_daily_prices ORDER BY coin_id, date",
        conn,
    )

    coins_df = pd.read_sql(
        "SELECT coin_id, name, symbol FROM dim_coins",
        conn,
    )

    conn.close()

    logger.info(f"[rag-dag] Loaded {len(price_df)} price rows, {len(coins_df)} coins")

    # Compute indicators
    logger.info("[rag-dag] Computing technical indicators...")
    snapshots = compute_all_indicators(price_df, coins_df)
    logger.info(f"[rag-dag] Computed indicators for {len(snapshots)} coins")

    # Save snapshots locally (use absolute path to match host volume mount)
    save_snapshots(snapshots, output_dir="/opt/airflow/project/data/analysis")

    # Embed and store in ChromaDB
    logger.info("[rag-dag] Embedding and storing patterns in ChromaDB...")
    stored = store_all_snapshots(snapshots)
    logger.info(f"[rag-dag] Stored {stored} patterns in ChromaDB")


def _generate_recommendations():
    """Generate RAG recommendations for top coins (limited by Ollama capacity)."""
    import sys
    sys.path.insert(0, "/opt/airflow/project")

    import pandas as pd
    from trino.dbapi import connect as trino_connect

    from src.analysis.indicators import compute_all_indicators
    from src.analysis.embedder import embed_for_retrieval, find_similar_patterns, get_ollama_client
    from src.rag.recommendation_engine import generate_all_recommendations, save_recommendations
    from src.utils.logger import logger

    MAX_COINS = 20

    # Connect to Trino
    conn = trino_connect(
        host="trino", port=8080, user="airflow",
        catalog="hive", schema="gold",
    )

    # Get top coins by market cap
    top_coins_df = pd.read_sql(
        f"SELECT coin_id, name, symbol FROM dim_coins "
        f"WHERE market_cap_rank IS NOT NULL ORDER BY market_cap_rank LIMIT {MAX_COINS}",
        conn,
    )
    top_coin_ids = top_coins_df["coin_id"].tolist()
    logger.info(f"[rag-dag] Top {len(top_coin_ids)} coins by market cap")

    placeholders = ",".join(f"'{c}'" for c in top_coin_ids)
    price_df = pd.read_sql(
        f"SELECT coin_id, date, price_usd, market_cap_usd, volume_usd, daily_return "
        f"FROM fact_daily_prices WHERE coin_id IN ({placeholders}) ORDER BY coin_id, date",
        conn,
    )
    coins_df = pd.read_sql(
        f"SELECT coin_id, name, symbol FROM dim_coins WHERE coin_id IN ({placeholders})",
        conn,
    )
    conn.close()

    # Compute indicators
    snapshots = compute_all_indicators(price_df, coins_df)
    logger.info(f"[rag-dag] Computed indicators for {len(snapshots)} coins")

    # Generate recommendations
    logger.info("[rag-dag] Generating RAG recommendations...")
    recommendations = generate_all_recommendations(snapshots, n_similar=5)

    # Save (use absolute path to match host volume mount)
    save_recommendations(recommendations, output_dir="/opt/airflow/project/data/analysis")
    logger.info(f"[rag-dag] Generated {len(recommendations)} recommendations")


with DAG(
    dag_id="daily_rag_pipeline",
    default_args=default_args,
    description="Daily RAG: compute indicators, embed patterns, generate recommendations",
    schedule="0 7 * * *",
    start_date=datetime(2026, 7, 17),
    catchup=False,
    tags=["crypto", "rag", "daily"],
) as dag:

    compute_indicators = PythonOperator(
        task_id="compute_indicators",
        python_callable=_compute_and_store_indicators,
        doc="Query Trino, compute technical indicators, embed into ChromaDB",
    )

    generate_recs = PythonOperator(
        task_id="generate_recommendations",
        python_callable=_generate_recommendations,
        doc="Generate buy/sell/hold recommendations via RAG + Ollama",
    )

    compute_indicators >> generate_recs
