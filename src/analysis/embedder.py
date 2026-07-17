"""
Pattern Embedding Engine

Uses Ollama to embed technical indicator snapshots into vectors
and stores them in ChromaDB for similarity retrieval.

RAG Flow
--------
1. Take a TechnicalSnapshot → format as text → embed via Ollama
2. Store in ChromaDB with metadata (coin_id, date, signal, return)
3. Query: find similar historical patterns for a given snapshot
"""

import json
from pathlib import Path
from datetime import datetime, timezone

import chromadb
import ollama

from src.analysis.indicators import TechnicalSnapshot
from src.utils.logger import logger

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CHROMA_HOST = "chromadb"
CHROMA_PORT = 8000
OLLAMA_HOST = "ollama"
OLLAMA_PORT = 11434
EMBEDDING_MODEL = "nomic-embed-text"
COLLECTION_NAME = "crypto_patterns"


def get_chroma_client() -> chromadb.HttpClient:
    """Get ChromaDB HTTP client."""
    return chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)


def get_ollama_client() -> ollama.Client:
    """Get Ollama client."""
    return ollama.Client(host=f"http://{OLLAMA_HOST}:{OLLAMA_PORT}")


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def embed_snapshot(snapshot: TechnicalSnapshot, ollama_client: ollama.Client = None) -> list[float]:
    """
    Generate an embedding vector for a technical snapshot.

    Uses Ollama's nomic-embed-text model (768 dimensions).
    """
    if ollama_client is None:
        ollama_client = get_ollama_client()

    text = snapshot.to_prompt_text()

    response = ollama_client.embeddings(model=EMBEDDING_MODEL, prompt=text)
    return response["embedding"]


def embed_for_retrieval(snapshot: TechnicalSnapshot, ollama_client: ollama.Client = None) -> list[float]:
    """
    Generate an embedding for pattern retrieval.

    Focuses on the technical pattern (not current price) so that
    similar patterns at different price levels are matched.
    """
    if ollama_client is None:
        ollama_client = get_ollama_client()

    # Pattern-focused text (excludes absolute price, focuses on indicators)
    text = (
        f"{snapshot.symbol} pattern on {snapshot.date}: "
        f"RSI={snapshot.rsi_14}, "
        f"MACD_hist={snapshot.macd_histogram:+.6f}, "
        f"price_vs_SMA20={snapshot.price_vs_sma20:+.1f}%, "
        f"price_vs_SMA50={snapshot.price_vs_sma50:+.1f}%, "
        f"BB_pos={snapshot.bb_position:.2f}, "
        f"vol_ratio={snapshot.volume_ratio:.2f}x, "
        f"streak={snapshot.return_streak:+d}, "
        f"signal={snapshot.signal}, "
        f"daily_return={snapshot.daily_return:+.4f}"
    )

    response = ollama_client.embeddings(model=EMBEDDING_MODEL, prompt=text)
    return response["embedding"]


# ---------------------------------------------------------------------------
# ChromaDB Operations
# ---------------------------------------------------------------------------

def upsert_snapshot(
    snapshot: TechnicalSnapshot,
    embedding: list[float],
    chroma_client: chromadb.HttpClient = None,
) -> None:
    """Store a snapshot's embedding in ChromaDB."""
    if chroma_client is None:
        chroma_client = get_chroma_client()

    collection = chroma_client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )

    doc_id = f"{snapshot.coin_id}_{snapshot.date}"

    collection.upsert(
        ids=[doc_id],
        embeddings=[embedding],
        documents=[snapshot.to_prompt_text()],
        metadatas=[{
            "coin_id": snapshot.coin_id,
            "symbol": snapshot.symbol,
            "name": snapshot.name,
            "date": snapshot.date,
            "price_usd": snapshot.price_usd,
            "rsi_14": snapshot.rsi_14,
            "macd_histogram": snapshot.macd_histogram,
            "signal": snapshot.signal,
            "confidence": snapshot.confidence,
            "daily_return": snapshot.daily_return,
            "volume_ratio": snapshot.volume_ratio,
        }],
    )


def find_similar_patterns(
    snapshot: TechnicalSnapshot,
    embedding: list[float],
    n_results: int = 5,
    exclude_same_coin: bool = False,
    chroma_client: chromadb.HttpClient = None,
) -> list[dict]:
    """
    Find historical patterns similar to the current snapshot.

    Returns list of dicts with pattern metadata and outcomes.
    """
    if chroma_client is None:
        chroma_client = get_chroma_client()

    collection = chroma_client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )

    # Build where filter to exclude current coin if requested
    where_filter = None
    if exclude_same_coin:
        where_filter = {"coin_id": {"$ne": snapshot.coin_id}}

    results = collection.query(
        query_embeddings=[embedding],
        n_results=n_results + 5,  # fetch extra to account for filtering
        where=where_filter,
        include=["documents", "metadatas", "distances"],
    )

    patterns = []
    if results and results["metadatas"]:
        for doc, meta, dist in zip(
            results["documents"][0],
            results["metadatas"][0],
            results["distances"][0],
        ):
            patterns.append({
                "document": doc,
                "metadata": meta,
                "similarity": 1 - dist,  # cosine distance → similarity
            })

    # Sort by similarity and take top N
    patterns.sort(key=lambda x: x["similarity"], reverse=True)
    return patterns[:n_results]


def get_pattern_count(chroma_client: chromadb.HttpClient = None) -> int:
    """Return total number of patterns stored."""
    if chroma_client is None:
        chroma_client = get_chroma_client()

    collection = chroma_client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )
    return collection.count()


# ---------------------------------------------------------------------------
# Batch Operations
# ---------------------------------------------------------------------------

def store_all_snapshots(snapshots: list[TechnicalSnapshot]) -> int:
    """
    Embed and store all snapshots in ChromaDB.

    Returns the number of snapshots stored.
    """
    ollama_client = get_ollama_client()
    chroma_client = get_chroma_client()

    stored = 0
    for snapshot in snapshots:
        try:
            embedding = embed_for_retrieval(snapshot, ollama_client)
            upsert_snapshot(snapshot, embedding, chroma_client)
            stored += 1
        except Exception as e:
            logger.error(f"[embedder] Failed to store {snapshot.coin_id}_{snapshot.date}: {e}")

    logger.info(f"[embedder] Stored {stored}/{len(snapshots)} patterns in ChromaDB")
    return stored
