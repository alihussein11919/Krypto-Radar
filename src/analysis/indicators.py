"""
Technical Indicator Computation

Reads daily price data from Trino/Parquet and computes
technical indicators for each coin.

Indicators
----------
- RSI (14-period)
- MACD (12, 26, 9)
- SMA (20, 50)
- Bollinger Bands (20, 2)
- Volume trend (20-day average vs current)
- Daily return streak
- Composite signal: bullish / bearish / neutral + confidence

Usage
-----
    from src.analysis.indicators import compute_all_indicators
    snapshots = compute_all_indicators()
"""

import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Technical indicator functions (pure pandas/numpy, no TA-Lib dependency)
# ---------------------------------------------------------------------------

def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """MACD line, signal line, histogram."""
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def compute_sma(series: pd.Series, period: int) -> pd.Series:
    """Simple Moving Average."""
    return series.rolling(window=period, min_periods=period).mean()


def compute_bollinger(series: pd.Series, period: int = 20, std_dev: float = 2.0):
    """Bollinger Bands: upper, middle (SMA), lower."""
    middle = compute_sma(series, period)
    rolling_std = series.rolling(window=period, min_periods=period).std()
    upper = middle + std_dev * rolling_std
    lower = middle - std_dev * rolling_std
    return upper, middle, lower


def compute_volume_trend(volume: pd.Series, period: int = 20) -> pd.Series:
    """Volume ratio vs 20-day average. >1 = above average, <1 = below."""
    avg_vol = volume.rolling(window=period, min_periods=period).mean()
    return volume / avg_vol.replace(0, np.nan)


def compute_return_streak(returns: pd.Series) -> int:
    """Count consecutive positive or negative days from the end."""
    if len(returns) == 0 or pd.isna(returns.iloc[-1]):
        return 0
    streak = 0
    direction = 1 if returns.iloc[-1] > 0 else -1
    for r in reversed(returns.values):
        if pd.isna(r):
            break
        if (r > 0 and direction == 1) or (r < 0 and direction == -1):
            streak += direction
        else:
            break
    return streak


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class TechnicalSnapshot:
    """Technical indicator snapshot for a single coin on a given date."""
    coin_id: str
    symbol: str
    name: str
    date: str
    price_usd: float
    market_cap_usd: float
    volume_usd: float
    daily_return: float

    # Trend
    rsi_14: float = 0.0
    macd_line: float = 0.0
    macd_signal: float = 0.0
    macd_histogram: float = 0.0
    sma_20: float = 0.0
    sma_50: float = 0.0
    price_vs_sma20: float = 0.0  # percentage above/below SMA20
    price_vs_sma50: float = 0.0

    # Volatility
    bb_upper: float = 0.0
    bb_middle: float = 0.0
    bb_lower: float = 0.0
    bb_position: float = 0.0  # 0=at lower band, 1=at upper band

    # Volume
    volume_ratio: float = 0.0  # vs 20-day avg

    # Momentum
    return_streak: int = 0

    # Composite signal
    signal: str = "neutral"  # bullish / bearish / neutral
    confidence: int = 50     # 0-100

    def to_prompt_text(self) -> str:
        """Format as human-readable text for LLM prompt."""
        return (
            f"{self.name} ({self.symbol.upper()}) on {self.date}:\n"
            f"  Price: ${self.price_usd:,.2f} (daily return: {self.daily_return:+.2%})\n"
            f"  RSI(14): {self.rsi_14:.1f}\n"
            f"  MACD: {self.macd_line:.4f} (signal: {self.macd_signal:.4f}, histogram: {self.macd_histogram:+.4f})\n"
            f"  SMA20: ${self.sma_20:,.2f} (price is {self.price_vs_sma20:+.1f}% vs SMA20)\n"
            f"  SMA50: ${self.sma_50:,.2f} (price is {self.price_vs_sma50:+.1f}% vs SMA50)\n"
            f"  Bollinger: position {self.bb_position:.2f} (0=lower, 1=upper)\n"
            f"  Volume: {self.volume_ratio:.2f}x 20-day average\n"
            f"  Return streak: {self.return_streak:+d} days\n"
            f"  Signal: {self.signal.upper()} (confidence: {self.confidence}%)"
        )

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Take the latest row per coin per date (handles duplicate ingestion)."""
    sort_cols = ["coin_id", "date"]
    if "ingestion_time" in df.columns:
        sort_cols.append("ingestion_time")
    df = df.sort_values(sort_cols)
    return df.drop_duplicates(subset=["coin_id", "date"], keep="last")


def compute_indicators_for_coin(df: pd.DataFrame, coin_meta: dict) -> TechnicalSnapshot | None:
    """Compute all indicators for a single coin's time series."""
    if len(df) < 30:
        return None  # Not enough data for meaningful indicators

    df = df.sort_values("date").reset_index(drop=True)

    # Compute indicators on full series
    rsi = compute_rsi(df["price_usd"])
    macd_line, macd_signal_line, macd_hist = compute_macd(df["price_usd"])
    sma20 = compute_sma(df["price_usd"], 20)
    sma50 = compute_sma(df["price_usd"], 50)
    bb_upper, bb_middle, bb_lower = compute_bollinger(df["price_usd"])
    vol_ratio = compute_volume_trend(df["volume_usd"])

    # Take the latest values
    latest = df.iloc[-1]
    idx = len(df) - 1

    # Compute composite signal
    signal, confidence = _compute_signal(
        rsi_val=rsi.iloc[idx],
        macd_h=macd_hist.iloc[idx],
        macd_cross=macd_hist.iloc[idx] > 0 and (idx == 0 or macd_hist.iloc[idx - 1] <= 0),
        price_vs_sma20=((latest["price_usd"] / sma20.iloc[idx]) - 1) * 100 if sma20.iloc[idx] else 0,
        price_vs_sma50=((latest["price_usd"] / sma50.iloc[idx]) - 1) * 100 if sma50.iloc[idx] else 0,
        bb_pos=((latest["price_usd"] - bb_lower.iloc[idx]) / (bb_upper.iloc[idx] - bb_lower.iloc[idx]))
        if bb_upper.iloc[idx] != bb_lower.iloc[idx] else 0.5,
        vol_ratio_val=vol_ratio.iloc[idx],
    )

    # BB position (0 to 1)
    bb_range = bb_upper.iloc[idx] - bb_lower.iloc[idx]
    bb_position = ((latest["price_usd"] - bb_lower.iloc[idx]) / bb_range) if bb_range > 0 else 0.5

    return TechnicalSnapshot(
        coin_id=latest["coin_id"],
        symbol=coin_meta.get("symbol", latest["coin_id"][:3]),
        name=coin_meta.get("name", latest["coin_id"].title()),
        date=str(latest["date"]),
        price_usd=latest["price_usd"],
        market_cap_usd=latest.get("market_cap_usd", 0),
        volume_usd=latest.get("volume_usd", 0),
        daily_return=latest.get("daily_return", 0),
        rsi_14=round(rsi.iloc[idx], 2) if not pd.isna(rsi.iloc[idx]) else 50.0,
        macd_line=round(macd_line.iloc[idx], 6) if not pd.isna(macd_line.iloc[idx]) else 0.0,
        macd_signal=round(macd_signal_line.iloc[idx], 6) if not pd.isna(macd_signal_line.iloc[idx]) else 0.0,
        macd_histogram=round(macd_hist.iloc[idx], 6) if not pd.isna(macd_hist.iloc[idx]) else 0.0,
        sma_20=round(sma20.iloc[idx], 2) if not pd.isna(sma20.iloc[idx]) else latest["price_usd"],
        sma_50=round(sma50.iloc[idx], 2) if not pd.isna(sma50.iloc[idx]) else latest["price_usd"],
        price_vs_sma20=round(((latest["price_usd"] / sma20.iloc[idx]) - 1) * 100, 2)
        if not pd.isna(sma20.iloc[idx]) and sma20.iloc[idx] != 0 else 0.0,
        price_vs_sma50=round(((latest["price_usd"] / sma50.iloc[idx]) - 1) * 100, 2)
        if not pd.isna(sma50.iloc[idx]) and sma50.iloc[idx] != 0 else 0.0,
        bb_upper=round(bb_upper.iloc[idx], 2) if not pd.isna(bb_upper.iloc[idx]) else latest["price_usd"],
        bb_middle=round(bb_middle.iloc[idx], 2) if not pd.isna(bb_middle.iloc[idx]) else latest["price_usd"],
        bb_lower=round(bb_lower.iloc[idx], 2) if not pd.isna(bb_lower.iloc[idx]) else latest["price_usd"],
        bb_position=round(bb_position, 3),
        volume_ratio=round(vol_ratio.iloc[idx], 2) if not pd.isna(vol_ratio.iloc[idx]) else 1.0,
        return_streak=compute_return_streak(df["daily_return"]),
        signal=signal,
        confidence=confidence,
    )


def _compute_signal(
    rsi_val: float, macd_h: float, macd_cross: bool,
    price_vs_sma20: float, price_vs_sma50: float,
    bb_pos: float, vol_ratio_val: float,
) -> tuple[str, int]:
    """Compute composite signal from individual indicators."""
    score = 0  # -100 to +100 scale

    # RSI: oversold = bullish, overbought = bearish
    if rsi_val < 30:
        score += 25
    elif rsi_val < 40:
        score += 10
    elif rsi_val > 70:
        score -= 25
    elif rsi_val > 60:
        score -= 10

    # MACD
    if macd_h > 0:
        score += 15
    elif macd_h < 0:
        score -= 15
    if macd_cross:
        score += 10  # bullish crossover bonus

    # Price vs SMA
    if price_vs_sma20 > 0:
        score += 10
    else:
        score -= 10
    if price_vs_sma50 > 0:
        score += 10
    else:
        score -= 10

    # Bollinger position
    if bb_pos < 0.2:
        score += 15  # near lower band = potential bounce
    elif bb_pos > 0.8:
        score -= 15  # near upper band = potential pullback

    # Volume confirmation
    if vol_ratio_val > 1.5:
        # High volume confirms the direction of the move
        if score > 0:
            score += 10
        else:
            score -= 10

    # Normalize to 0-100 confidence
    confidence = min(100, max(0, abs(score) + 30))

    if score > 15:
        return "bullish", confidence
    elif score < -15:
        return "bearish", confidence
    else:
        return "neutral", confidence


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_all_indicators(
    price_data: pd.DataFrame,
    coins_meta: pd.DataFrame,
) -> list[TechnicalSnapshot]:
    """
    Compute technical indicators for all coins.

    Parameters
    ----------
    price_data : pd.DataFrame
        From fact_daily_prices with columns: coin_id, date, price_usd,
        market_cap_usd, volume_usd, daily_return

    coins_meta : pd.DataFrame
        From dim_coins with columns: coin_id, name, symbol

    Returns
    -------
    list[TechnicalSnapshot]
        One snapshot per coin with all computed indicators.
    """
    price_data = _deduplicate(price_data)

    # Build coin metadata lookup
    coin_lookup = {}
    for _, row in coins_meta.iterrows():
        coin_lookup[row["coin_id"]] = {
            "name": row.get("name", row["coin_id"].title()),
            "symbol": row.get("symbol", row["coin_id"][:3]),
        }

    snapshots = []
    for coin_id in price_data["coin_id"].unique():
        coin_df = price_data[price_data["coin_id"] == coin_id].copy()
        meta = coin_lookup.get(coin_id, {"name": coin_id.title(), "symbol": coin_id[:3]})

        snapshot = compute_indicators_for_coin(coin_df, meta)
        if snapshot:
            snapshots.append(snapshot)

    return snapshots


def save_snapshots(snapshots: list[TechnicalSnapshot], output_dir: str = "data/analysis") -> str:
    """Save snapshots to JSON for downstream consumption."""
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filepath = path / f"indicators_{date_str}.json"

    data = [s.to_dict() for s in snapshots]
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)

    return str(filepath)
