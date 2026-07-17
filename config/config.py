"""
Application configuration.

Loads environment variables and exposes them to the rest of the project.
"""

import os
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

# -----------------------------
# CoinGecko Configuration
# -----------------------------

API_KEY = os.getenv("COINGECKO_API_KEY")
BASE_URL = os.getenv("BASE_URL")

# -----------------------------
# Networking
# -----------------------------

TIMEOUT = 30
MAX_RETRIES = 3

# -----------------------------
# Output Directories
# -----------------------------

RAW_DATA_DIR = "data/raw"
FAILED_DATA_DIR = "data/failed"
CHECKPOINT_DIR = "data/checkpoint"

# -----------------------------
# Logging
# -----------------------------

LOG_FILE = "logs/ingestion.log"
LOG_LEVEL = "INFO"
