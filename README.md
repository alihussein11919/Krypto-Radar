# Krypto-Radar: Crypto Lakehouse Pipeline

A modern end-to-end data engineering project that ingests historical and real-time cryptocurrency market data, processes it using the Medallion Architecture, and transforms it into an analytics-ready dimensional data warehouse.

---

## Overview

This project demonstrates how modern data platforms ingest, process, and model large-scale financial data using industry-standard technologies.

The pipeline combines:

- Historical market data from CoinGecko
- Real-time market streams from Binance WebSocket
- Apache Kafka for event streaming
- Apache Spark for distributed processing
- Delta Lake / Apache Iceberg for the lakehouse
- Star Schema dimensional modeling for analytics

---

## Architecture

```

CoinGecko API          Binance WebSocket
      │                       │
      └──────────┬────────────┘
                 ▼
         Python Producers
                 │
                 ▼
             Apache Kafka
                 │
                 ▼
        Apache Spark Processing
                 │
        ┌────────┼────────┐
        ▼        ▼        ▼
     Bronze   Silver    Gold
                 │
                 ▼
      Dimensional Data Warehouse
                 │
                 ▼
           MinIO / Amazon S3
```

---
<img width="3652" height="3558" alt="Architecture" src="https://github.com/user-attachments/assets/54e347a1-39db-4bf4-bb89-9676cc6d1e15" />

# Project Goals

The project aims to demonstrate:

- Batch and streaming ingestion
- Distributed stream processing
- Medallion Architecture
- Data quality and cleansing
- Dimensional modeling
- Lakehouse architecture
- Event-driven data pipelines

---

# Data Sources

## CoinGecko REST API

Historical and reference datasets

- Coins
- Coin metadata
- Historical prices
- OHLC candles
- Exchanges
- Global market statistics

---

## Binance WebSocket

Real-time streaming datasets

- Trades
- Aggregate trades
- Klines
- Book ticker
- Market updates

---

# Medallion Architecture

## Bronze Layer

The Bronze layer stores raw source data exactly as it is received.

Characteristics

- Raw data
- Immutable
- Source-oriented
- Replayable
- Minimal transformations

Each API endpoint is stored independently to preserve the original source.

---

## Silver Layer

The Silver layer transforms raw data into trusted datasets.

Typical transformations include

- Removing duplicates
- Handling missing values
- Schema validation
- Type conversion
- Timestamp normalization
- Data quality validation
- Joining lookup tables
- Normalization where appropriate

Silver provides the foundation for downstream analytics.


## How we arrive at the silver transformations

The question to ask at silver is: what does this data need to be before it's useful to analysts? Three things drive every decision:

      First, type correctness — bronze stores price as a string because Binance sends it that way. Silver casts it to double. Bronze timestamps are milliseconds since epoch. Silver converts them to proper timestamps. These aren't business decisions, they're correctness decisions.
      
      Second, deduplication and null handling — raw data has duplicates from overlapping fetches and nulls from API quirks. Silver removes them. A downstream analyst should never have to think about this.
      
      Third, join enrichment — a trade record only has symbol = "BTCUSDT". Silver joins it to the coins table so downstream queries can filter by name = "Bitcoin" or category = "layer1" without knowing the symbol mapping.
      
      For the CoinGecko silver job specifically, the transformations are: cast price/volume to proper numerics, 
      compute daily_return = (price_today - price_yesterday) / price_yesterday, join market_chart to markets on coin_id to bring in name, symbol, market_cap_rank, and category. 
      Nothing fancier than that — silver is not the place for complex business logic.

---

## Gold Layer

The Gold layer contains business-ready datasets.

Business requirements determine the final model.

This project implements a Kimball dimensional warehouse using Star Schemas.

The Gold layer contains

- Fact tables
- Conformed dimensions
- Business metrics
- Aggregated datasets

---

# Dimensional Modeling

The analytical warehouse follows the Kimball methodology.

Conformed dimensions include

- Dim Coin
- Dim Date
- Dim Exchange
- Dim Currency
- Dim Time

Fact tables include

- Fact Market
- Fact Trades
- Fact OHLC

The dimensional model is created only after the Silver layer has produced trusted, standardized data.

---

# Technologies

| Category | Technologies |
|-----------|--------------|
| Language | Python |
| Streaming | Apache Kafka |
| Processing | Apache Spark |
| Lakehouse | Delta Lake / Apache Iceberg |
| Storage | MinIO / Amazon S3 |
| API | CoinGecko |
| Streaming Source | Binance WebSocket |
| Data Modeling | Kimball Bus Architecture |
| Warehouse | Star Schema |

---

# Repository Structure

```
crypto-ingestion/

├── config/
├── data/
├── logs/
├── src/
│   ├── api/
│   ├── ingestion/
│   ├── storage/
│   └── utils/
├── tests/
└── main.py
```

---

# Data Flow

```
CoinGecko
Binance
    │
    ▼
Kafka
    │
    ▼
Spark
    │
    ▼
Bronze
(Raw)
    │
    ▼
Silver
(Clean & Standardized)
    │
    ▼
Gold
(Dimensional Warehouse)
```

---

# Future Improvements

- Airflow orchestration
- Data quality monitoring
- CI/CD pipeline
- Docker Compose deployment
- Grafana dashboards
- Trino query engine
- Iceberg catalog
- Machine learning feature store

---

# Learning Objectives

This project explores modern data engineering concepts including

- Event streaming
- Lakehouse architecture
- Medallion Architecture
- Spark Structured Streaming
- Data quality engineering
- Kimball dimensional modeling
- Distributed processing
- End-to-end data pipelines
