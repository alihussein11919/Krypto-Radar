"""
CoinGecko API endpoint definitions.

All endpoint paths are defined here so they can be reused
throughout the project.
"""

# ======================================================
# COINS
# ======================================================

COINS_LIST = "/coins/list"

COIN_DETAILS = "/coins/{id}"

COIN_MARKETS = "/coins/markets"

COIN_HISTORY = "/coins/{id}/history"

COIN_MARKET_CHART = "/coins/{id}/market_chart"

COIN_MARKET_CHART_RANGE = "/coins/{id}/market_chart/range"

COIN_OHLC = "/coins/{id}/ohlc"

# ======================================================
# GLOBAL
# ======================================================

GLOBAL = "/global"

# ======================================================
# EXCHANGES
# ======================================================

EXCHANGES = "/exchanges"

EXCHANGE_DETAILS = "/exchanges/{id}"