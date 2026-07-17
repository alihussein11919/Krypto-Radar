"""
Checkpoint Manager

Tracks which coin IDs have already been fetched for a given
dataset so that interrupted runs can resume without re-fetching.

Storage format:
    data/checkpoint/{dataset}.json
    → { "completed": ["bitcoin", "ethereum", ...] }
"""

import json
from pathlib import Path

from src.utils.logger import logger

CHECKPOINT_DIR = Path("data/checkpoint")


def _get_path(dataset: str) -> Path:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return CHECKPOINT_DIR / f"{dataset}.json"


def load_completed(dataset: str) -> set:
    """
    Return the set of coin IDs already fetched for this dataset.
    Returns an empty set if no checkpoint exists yet.
    """
    path = _get_path(dataset)

    if not path.exists():
        return set()

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    completed = set(data.get("completed", []))
    logger.info(f"[checkpoint] {dataset}: {len(completed)} already completed.")
    return completed


def mark_completed(dataset: str, coin_id: str) -> None:
    """
    Mark a single coin ID as completed for this dataset.
    Appends to the existing checkpoint file.
    """
    path = _get_path(dataset)
    completed = load_completed(dataset)
    completed.add(coin_id)

    with open(path, "w", encoding="utf-8") as f:
        json.dump({"completed": list(completed)}, f, indent=2)


def reset(dataset: str) -> None:
    """
    Clear the checkpoint for a dataset.
    Use this if you want to force a full re-fetch.
    """
    path = _get_path(dataset)
    if path.exists():
        path.unlink()
        logger.info(f"[checkpoint] {dataset}: checkpoint cleared.")
