# Krypto-Radar

A complete crypto data lakehouse with automated pipeline orchestration and an AI-powered RAG (Retrieval-Augmented Generation) recommendation system for buy/sell/hold signals.

## Architecture

```
┌─────────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌──────────┐
│  CoinGecko  │────▶│ Bronze  │────▶│ Silver  │────▶│  Gold   │────▶│  Hive    │
│  Binance WS │     │  (JSON) │     │(Parquet)│     │ (Star)  │     │ Metastore│
└─────────────┘     └─────────┘     └─────────┘     └─────────┘     └────┬─────┘
                                                                         │
                          ┌──────────────────────────────────────────────┘
                          │
              ┌───────────┴───────────┐
              │                       │
        ┌─────▼─────┐          ┌──────▼──────┐
        │   Trino   │          │  RAG API    │
        │  (SQL)    │          │ (FastAPI)   │
        └─────┬─────┘          └──────┬──────┘
              │                       │
    ┌─────────┴─────────┐     ┌───────┴───────┐
    │                   │     │               │
┌───▼───┐        ┌──────▼──┐  │        ┌──────▼──────┐
│Grafana│        │Streamlit│  │        │   Ollama    │
│(3 dash)│      │ (RAG)   │  │        │  + ChromaDB │
└───────┘        └─────────┘  │        └─────────────┘
                              │
                    ┌─────────▼─────────┐
                    │   Airflow         │
                    │ (DAG orchestration)│
                    └───────────────────┘
```

<img width="14773" height="4842" alt="image" src="https://github.com/user-attachments/assets/41494fa1-7618-44a7-ac7d-c089a6f8d018" />


## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Orchestration** | Apache Airflow 2.10 |
| **Compute** | Apache Spark 3.5 |
| **Storage** | MinIO (S3-compatible) |
| **Query Engine** | Trino |
| **Metastore** | Hive Metastore |
| **Visualization** | Grafana (Trino dashboards), Streamlit (RAG dashboard) |
| **AI/LLM** | Ollama (local LLM), ChromaDB (vector store) |
| **API** | FastAPI (RAG recommendations) |
| **Streaming** | Binance WebSocket (real-time trades) |
| **Containerization** | Docker Compose |

## Quick Start

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/Krypto-Radar.git
cd Krypto-Radar

# Start all services
docker-compose up -d

# Access the UIs
# - Airflow:     http://localhost:8082 (admin/admin)
# - Grafana:     http://localhost:3000 (admin/admin)
# - Streamlit:   http://localhost:8501
# - RAG API:     http://localhost:8200
# - MinIO:       http://localhost:9001 (minioadmin/minioadmin)
# - Trino:       localhost:8080
```

### Run the Pipelines

```bash
# Trigger daily crypto ingestion (CoinGecko → Bronze → Silver → Gold)
# Via Airflow UI: http://localhost:8082 → daily_crypto_pipeline → Trigger

# Trigger daily RAG recommendations (compute indicators → generate LLM signals)
# Via Airflow UI: http://localhost:8082 → daily_rag_pipeline → Trigger
```

## Directory Structure

```
Krypto-Radar/
├── app/                        # Streamlit RAG dashboard
│   ├── main.py                 # Entry point
│   ├── pages/1_RAG_Dashboard.py
│   └── utils.py
├── conf/                       # Configuration files
│   └── grafana/                # Grafana dashboards & provisioning
├── dags/                       # Airflow DAGs
│   ├── daily_crypto_pipeline.py
│   └── daily_rag_pipeline.py
├── data/analysis/              # RAG output (recommendations, indicators)
├── docker/                     # Dockerfiles
├── scripts/                    # Data processing & registration scripts
├── src/                        # Core Python modules
│   ├── analysis/               # Technical indicators (RSI, MACD, Bollinger)
│   ├── ingestion/              # CoinGecko API client
│   ├── rag/                    # RAG API (FastAPI) & LLM integration
│   ├── storage/                # S3/MinIO writers, data flatteners
│   └── streaming/              # Binance WebSocket consumer
├── docker-compose.yml
└── requirements*.txt
```

## Pipelines

### Daily Crypto Pipeline (`daily_crypto_pipeline`)
1. **Ingest** — Fetches latest 180 days of OHLCV data from CoinGecko (100 coins)
2. **Convert** — Transforms raw JSON to Parquet (Silver layer)
3. **Transform** — Builds star schema: `dim_coins`, `dim_date`, `fact_daily_prices` (Gold layer)
4. **Refresh** — Registers tables in Hive Metastore via Trino

### Daily RAG Pipeline (`daily_rag_pipeline`)
1. **Compute Indicators** — Calculates RSI, MACD, Bollinger Bands, volume ratios for 100 coins
2. **Generate Recommendations** — Sends indicators to Ollama LLM for buy/sell/hold analysis (top 20 by market cap)

### Real-time Streaming
- **Binance WebSocket** — Captures live BTC/ETH/SOL/XRP/BNB trades
- Writes raw trades to Bronze, aggregates into 1-minute OHLCV candles for Gold layer

## Grafana Dashboards

| Dashboard | Description |
|-----------|-------------|
| Crypto Overview | Market summary across all tracked coins |
| Crypto Real-Time | Live 1-minute OHLCV charts (BTC/ETH/SOL/XRP/BNB) |
| Crypto Historical | Daily price trends and volume analysis |

## Environment Variables

Set in `.env`:

```env
COINGECKO_API_KEY=your_api_key_here
BASE_URL=https://api.coingecko.com/api/v3
```
