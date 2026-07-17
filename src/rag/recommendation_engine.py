"""
RAG Recommendation Engine

Orchestrates the full retrieval-augmented generation pipeline:
1. Compute technical indicators for current data
2. Retrieve similar historical patterns from ChromaDB
3. Generate buy/sell/hold recommendation via Ollama LLM
"""

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import ollama

from src.analysis.indicators import TechnicalSnapshot
from src.analysis.embedder import (
    embed_for_retrieval,
    find_similar_patterns,
    get_ollama_client,
)
from src.utils.logger import logger

GENERATION_MODEL = "llama3.2:3b"

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Recommendation:
    """Structured recommendation for a single coin."""
    coin_id: str
    symbol: str
    name: str
    action: str           # BUY / SELL / HOLD
    confidence: int       # 0-100
    reasoning: str
    risk_level: str       # LOW / MEDIUM / HIGH
    timeframe: str        # short-term (1-7 days) / medium-term (1-4 weeks)
    similar_patterns: int # number of similar patterns found
    generated_at: str

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a cryptocurrency technical analyst. You analyze price patterns 
and technical indicators to provide actionable recommendations.

IMPORTANT RULES:
- Always respond with valid JSON only, no extra text
- Never guarantee profits — acknowledge uncertainty
- Consider risk management in your recommendations
- Base recommendations on technical patterns, not hype
- Acknowledge when data is insufficient for a strong signal

Response format:
{
  "action": "BUY" | "SELL" | "HOLD",
  "confidence": <int 0-100>,
  "reasoning": "<2-3 sentence explanation>",
  "risk_level": "LOW" | "MEDIUM" | "HIGH",
  "timeframe": "short-term" | "medium-term"
}"""

USER_PROMPT_TEMPLATE = """Analyze the following cryptocurrency and provide a recommendation.

CURRENT STATE:
{current_snapshot}

SIMILAR HISTORICAL PATTERNS:
{patterns_text}

Based on the current technical indicators and the outcomes of similar historical patterns, 
provide your recommendation for {symbol}."""

# ---------------------------------------------------------------------------
# Core engine
# ---------------------------------------------------------------------------

def build_patterns_text(patterns: list[dict]) -> str:
    """Format similar patterns for the LLM prompt."""
    if not patterns:
        return "No similar historical patterns found."

    lines = []
    for i, p in enumerate(patterns, 1):
        meta = p.get("metadata", {})
        sim = p.get("similarity", 0)
        lines.append(
            f"Pattern {i} (similarity: {sim:.2f}):\n"
            f"  {meta.get('symbol', '?')} on {meta.get('date', '?')} — "
            f"RSI: {meta.get('rsi_14', '?')}, "
            f"MACD hist: {meta.get('macd_histogram', '?')}, "
            f"Signal: {meta.get('signal', '?')}, "
            f"Daily return: {meta.get('daily_return', 0):+.2%}\n"
            f"  Volume ratio: {meta.get('volume_ratio', 1):.2f}x"
        )

    return "\n".join(lines)


def generate_recommendation(
    snapshot: TechnicalSnapshot,
    patterns: list[dict],
    ollama_client: ollama.Client = None,
) -> Recommendation:
    """
    Generate a buy/sell/hold recommendation using RAG.

    Parameters
    ----------
    snapshot : TechnicalSnapshot
        Current technical indicators for the coin.

    patterns : list[dict]
        Similar historical patterns from ChromaDB.

    ollama_client : ollama.Client
        Ollama client for LLM generation.
    """
    if ollama_client is None:
        ollama_client = get_ollama_client()

    # Build the prompt
    user_prompt = USER_PROMPT_TEMPLATE.format(
        current_snapshot=snapshot.to_prompt_text(),
        patterns_text=build_patterns_text(patterns),
        symbol=snapshot.symbol.upper(),
    )

    # Call Ollama
    try:
        response = ollama_client.chat(
            model=GENERATION_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            options={
                "temperature": 0.3,
                "num_predict": 300,
            },
        )

        content = response["message"]["content"].strip()

        # Parse JSON from response (handle markdown code blocks)
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        parsed = json.loads(content)

        return Recommendation(
            coin_id=snapshot.coin_id,
            symbol=snapshot.symbol.upper(),
            name=snapshot.name,
            action=parsed.get("action", "HOLD").upper(),
            confidence=min(100, max(0, int(parsed.get("confidence", 50)))),
            reasoning=parsed.get("reasoning", "Insufficient data for analysis."),
            risk_level=parsed.get("risk_level", "MEDIUM").upper(),
            timeframe=parsed.get("timeframe", "short-term"),
            similar_patterns=len(patterns),
            generated_at=datetime.now(timezone.utc).isoformat(),
        )

    except json.JSONDecodeError as e:
        logger.warning(f"[rag] Failed to parse LLM response for {snapshot.coin_id}: {e}")
        return _fallback_recommendation(snapshot, patterns)

    except Exception as e:
        logger.error(f"[rag] LLM call failed for {snapshot.coin_id}: {e}")
        return _fallback_recommendation(snapshot, patterns)


def _fallback_recommendation(
    snapshot: TechnicalSnapshot,
    patterns: list[dict],
) -> Recommendation:
    """Generate a rule-based fallback recommendation when LLM fails."""
    action = "HOLD"
    confidence = 40
    reasoning = "LLM unavailable — using rule-based fallback."

    if snapshot.rsi_14 < 30:
        action = "BUY"
        confidence = 55
        reasoning = f"RSI at {snapshot.rsi_14:.0f} indicates oversold conditions."
    elif snapshot.rsi_14 > 70:
        action = "SELL"
        confidence = 55
        reasoning = f"RSI at {snapshot.rsi_14:.0f} indicates overbought conditions."

    return Recommendation(
        coin_id=snapshot.coin_id,
        symbol=snapshot.symbol.upper(),
        name=snapshot.name,
        action=action,
        confidence=confidence,
        reasoning=reasoning,
        risk_level="MEDIUM",
        timeframe="short-term",
        similar_patterns=len(patterns),
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


# ---------------------------------------------------------------------------
# Batch recommendations
# ---------------------------------------------------------------------------

def generate_all_recommendations(
    snapshots: list[TechnicalSnapshot],
    n_similar: int = 5,
) -> list[Recommendation]:
    """
    Generate recommendations for all coins.

    Parameters
    ----------
    snapshots : list[TechnicalSnapshot]
        Current technical snapshots for all coins.

    n_similar : int
        Number of similar patterns to retrieve per coin.
    """
    ollama_client = get_ollama_client()
    recommendations = []

    for snapshot in snapshots:
        logger.info(f"[rag] Generating recommendation for {snapshot.symbol}...")

        # Embed current pattern
        embedding = embed_for_retrieval(snapshot, ollama_client)

        # Find similar patterns
        patterns = find_similar_patterns(snapshot, embedding, n_results=n_similar)

        # Generate recommendation
        rec = generate_recommendation(snapshot, patterns, ollama_client)
        recommendations.append(rec)

        logger.info(
            f"[rag] {rec.symbol}: {rec.action} "
            f"(confidence: {rec.confidence}%, risk: {rec.risk_level})"
        )

    return recommendations


def save_recommendations(recommendations: list[Recommendation], output_dir: str = "data/analysis") -> str:
    """Save recommendations to JSON."""
    from pathlib import Path
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filepath = path / f"recommendations_{date_str}.json"

    data = [r.to_dict() for r in recommendations]
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    return str(filepath)
