"""
RAG Recommendation API

FastAPI service that exposes recommendation endpoints.
Grafana queries this to populate the recommendation dashboard.

Endpoints
---------
GET  /api/recommendations        → all current recommendations
GET  /api/recommendations/{coin} → single coin recommendation
GET  /api/indicators             → all current technical indicators
GET  /api/summary                → summary dashboard data
GET  /api/health                 → service health check

SimpleJSON Protocol (for grafana-simple-json-datasource)
GET  /api/search                 → list available metrics
POST /api/query                  → return metric data
GET  /api/annotations            → annotations (empty)

Simpod JSON Protocol (for simpod-json-datasource)
GET  /search                     → list available metrics
POST /query                      → return metric data
GET  /tag-keys                   → list tag keys
GET  /tag-values                 → list tag values
"""

import json
from pathlib import Path
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from src.analysis.indicators import TechnicalSnapshot
from src.rag.recommendation_engine import Recommendation

app = FastAPI(
    title="Crypto RAG Recommendations",
    description="AI-powered crypto buy/sell/hold recommendations",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ANALYSIS_DIR = Path("data/analysis")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_latest_json(prefix: str) -> list[dict]:
    """Load the most recent JSON file matching a prefix."""
    if not ANALYSIS_DIR.exists():
        return []

    files = sorted(ANALYSIS_DIR.glob(f"{prefix}_*.json"), reverse=True)
    if not files:
        return []

    with open(files[0], "r") as f:
        return json.load(f)


def _get_summary_data() -> dict:
    """Compute summary data from recommendations."""
    recs = _load_latest_json("recommendations")
    if not recs:
        return {}

    buy_count = sum(1 for r in recs if r.get("action") == "BUY")
    sell_count = sum(1 for r in recs if r.get("action") == "SELL")
    hold_count = sum(1 for r in recs if r.get("action") == "HOLD")

    if buy_count > sell_count * 2:
        sentiment = "BULLISH"
    elif sell_count > buy_count * 2:
        sentiment = "BEARISH"
    else:
        sentiment = "NEUTRAL"

    sorted_recs = sorted(recs, key=lambda x: x.get("confidence", 0), reverse=True)
    top_buys = [r for r in sorted_recs if r.get("action") == "BUY"][:3]
    top_sells = [r for r in sorted_recs if r.get("action") == "SELL"][:3]

    return {
        "market_sentiment": sentiment,
        "signal_counts": {"buy": buy_count, "sell": sell_count, "hold": hold_count},
        "top_buys": top_buys,
        "top_sells": top_sells,
        "total_coins": len(recs),
        "generated_at": recs[0].get("generated_at") if recs else None,
    }


def _resolve_metric_value(data: dict, metric: str):
    """Resolve a dotted metric path like 'signal_counts.buy' from summary data."""
    parts = metric.split(".")
    val = data
    for p in parts:
        if isinstance(val, dict):
            val = val.get(p)
        else:
            return None
    return val


# ---------------------------------------------------------------------------
# SimpleJSON Protocol Endpoints (for Grafana SimpleJSON plugin)
# ---------------------------------------------------------------------------

@app.get("/api/search")
def search():
    """SimpleJSON /api/search: list available metrics/targets."""
    summary = _get_summary_data()
    targets = []

    if summary:
        targets.extend([
            {"target": "market_sentiment", "type": "string"},
            {"target": "total_coins", "type": "number"},
            {"target": "signal_counts.buy", "type": "number"},
            {"target": "signal_counts.sell", "type": "number"},
            {"target": "signal_counts.hold", "type": "number"},
        ])

    # Table targets
    targets.extend([
        {"target": "recommendations", "type": "table"},
        {"target": "indicators", "type": "table"},
        {"target": "top_buys", "type": "table"},
        {"target": "top_sells", "type": "table"},
    ])

    return targets


@app.post("/api/query")
async def query(request: Request):
    """SimpleJSON /api/query: return data for requested targets."""
    body = await request.json()
    targets = body.get("targets", [])

    summary = _get_summary_data()
    recs = _load_latest_json("recommendations")
    indicators = _load_latest_json("indicators")

    results = []

    for t in targets:
        target = t.get("target", "")
        ref_id = t.get("refId", "A")

        # Scalar summary metrics
        if target in ("market_sentiment", "total_coins",
                       "signal_counts.buy", "signal_counts.sell", "signal_counts.hold"):
            val = _resolve_metric_value(summary, target)
            if val is not None:
                ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                results.append({
                    "target": target,
                    "refId": ref_id,
                    "datapoints": [[val, ts_ms]],
                })
            else:
                results.append({"target": target, "refId": ref_id, "datapoints": []})

        # Table targets
        elif target == "recommendations" and recs:
            columns = [
                {"text": "coin_id", "type": "string"},
                {"text": "symbol", "type": "string"},
                {"text": "name", "type": "string"},
                {"text": "action", "type": "string"},
                {"text": "confidence", "type": "number"},
                {"text": "risk_level", "type": "string"},
                {"text": "reasoning", "type": "string"},
                {"text": "generated_at", "type": "time"},
            ]
            rows = []
            for r in recs:
                rows.append([
                    r.get("coin_id", ""),
                    r.get("symbol", ""),
                    r.get("name", ""),
                    r.get("action", ""),
                    r.get("confidence", 0),
                    r.get("risk_level", ""),
                    r.get("reasoning", ""),
                    r.get("generated_at", ""),
                ])
            results.append({
                "type": "table",
                "columns": columns,
                "rows": rows,
                "refId": ref_id,
            })

        elif target == "indicators" and indicators:
            columns = [
                {"text": "coin_id", "type": "string"},
                {"text": "symbol", "type": "string"},
                {"text": "name", "type": "string"},
                {"text": "price_usd", "type": "number"},
                {"text": "rsi_14", "type": "number"},
                {"text": "macd_line", "type": "number"},
                {"text": "macd_signal", "type": "number"},
                {"text": "macd_histogram", "type": "number"},
                {"text": "sma_20", "type": "number"},
                {"text": "sma_50", "type": "number"},
                {"text": "bb_position", "type": "number"},
                {"text": "volume_ratio", "type": "number"},
                {"text": "signal", "type": "string"},
                {"text": "confidence", "type": "number"},
            ]
            rows = []
            for ind in indicators:
                rows.append([
                    ind.get("coin_id", ""),
                    ind.get("symbol", ""),
                    ind.get("name", ""),
                    ind.get("price_usd", 0),
                    ind.get("rsi_14", 50),
                    ind.get("macd_line", 0),
                    ind.get("macd_signal", 0),
                    ind.get("macd_histogram", 0),
                    ind.get("sma_20", 0),
                    ind.get("sma_50", 0),
                    ind.get("bb_position", 0.5),
                    ind.get("volume_ratio", 1.0),
                    ind.get("signal", "neutral"),
                    ind.get("confidence", 50),
                ])
            results.append({
                "type": "table",
                "columns": columns,
                "rows": rows,
                "refId": ref_id,
            })

        elif target == "top_buys":
            top = summary.get("top_buys", [])
            columns = [
                {"text": "coin_id", "type": "string"},
                {"text": "symbol", "type": "string"},
                {"text": "action", "type": "string"},
                {"text": "confidence", "type": "number"},
                {"text": "risk_level", "type": "string"},
                {"text": "reasoning", "type": "string"},
            ]
            rows = [[r.get("coin_id", ""), r.get("symbol", ""), r.get("action", ""),
                      r.get("confidence", 0), r.get("risk_level", ""), r.get("reasoning", "")]
                     for r in top]
            results.append({
                "type": "table",
                "columns": columns,
                "rows": rows,
                "refId": ref_id,
            })

        elif target == "top_sells":
            top = summary.get("top_sells", [])
            columns = [
                {"text": "coin_id", "type": "string"},
                {"text": "symbol", "type": "string"},
                {"text": "action", "type": "string"},
                {"text": "confidence", "type": "number"},
                {"text": "risk_level", "type": "string"},
                {"text": "reasoning", "type": "string"},
            ]
            rows = [[r.get("coin_id", ""), r.get("symbol", ""), r.get("action", ""),
                      r.get("confidence", 0), r.get("risk_level", ""), r.get("reasoning", "")]
                     for r in top]
            results.append({
                "type": "table",
                "columns": columns,
                "rows": rows,
                "refId": ref_id,
            })

        else:
            results.append({"target": target, "refId": ref_id, "datapoints": []})

    return results


@app.get("/api/annotations")
def annotations():
    """SimpleJSON /api/annotations: return empty list."""
    return []


# ---------------------------------------------------------------------------
# Simpod JSON Protocol Endpoints (for simpod-json-datasource)
# ---------------------------------------------------------------------------

@app.get("/search")
def simpod_search():
    """Simpod /search: list available metrics/targets."""
    summary = _get_summary_data()
    targets = []

    if summary:
        targets.extend([
            {"target": "market_sentiment", "type": "string"},
            {"target": "total_coins", "type": "number"},
            {"target": "signal_counts.buy", "type": "number"},
            {"target": "signal_counts.sell", "type": "number"},
            {"target": "signal_counts.hold", "type": "number"},
        ])

    targets.extend([
        {"target": "recommendations", "type": "table"},
        {"target": "indicators", "type": "table"},
        {"target": "top_buys", "type": "table"},
        {"target": "top_sells", "type": "table"},
    ])

    return targets


@app.post("/query")
async def simpod_query(request: Request):
    """Simpod /query: return data for requested targets."""
    body = await request.json()
    targets = body.get("targets", [])

    summary = _get_summary_data()
    recs = _load_latest_json("recommendations")
    indicators = _load_latest_json("indicators")

    results = []

    for t in targets:
        target = t.get("target", "")
        ref_id = t.get("refId", "A")
        target_type = t.get("type", "timeserie")

        if target in ("market_sentiment", "total_coins",
                       "signal_counts.buy", "signal_counts.sell", "signal_counts.hold"):
            val = _resolve_metric_value(summary, target)
            if val is not None:
                ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                results.append({
                    "target": target,
                    "refId": ref_id,
                    "datapoints": [[val, ts_ms]],
                })
            else:
                results.append({"target": target, "refId": ref_id, "datapoints": []})

        elif target == "recommendations" and recs:
            columns = [
                {"text": "coin_id", "type": "string"},
                {"text": "symbol", "type": "string"},
                {"text": "name", "type": "string"},
                {"text": "action", "type": "string"},
                {"text": "confidence", "type": "number"},
                {"text": "risk_level", "type": "string"},
                {"text": "reasoning", "type": "string"},
                {"text": "generated_at", "type": "time"},
            ]
            rows = []
            for r in recs:
                rows.append([
                    r.get("coin_id", ""),
                    r.get("symbol", ""),
                    r.get("name", ""),
                    r.get("action", ""),
                    r.get("confidence", 0),
                    r.get("risk_level", ""),
                    r.get("reasoning", ""),
                    r.get("generated_at", ""),
                ])
            results.append({
                "type": "table",
                "columns": columns,
                "rows": rows,
                "refId": ref_id,
            })

        elif target == "indicators" and indicators:
            columns = [
                {"text": "coin_id", "type": "string"},
                {"text": "symbol", "type": "string"},
                {"text": "name", "type": "string"},
                {"text": "price_usd", "type": "number"},
                {"text": "rsi_14", "type": "number"},
                {"text": "macd_line", "type": "number"},
                {"text": "macd_signal", "type": "number"},
                {"text": "macd_histogram", "type": "number"},
                {"text": "sma_20", "type": "number"},
                {"text": "sma_50", "type": "number"},
                {"text": "bb_position", "type": "number"},
                {"text": "volume_ratio", "type": "number"},
                {"text": "signal", "type": "string"},
                {"text": "confidence", "type": "number"},
            ]
            rows = []
            for ind in indicators:
                rows.append([
                    ind.get("coin_id", ""),
                    ind.get("symbol", ""),
                    ind.get("name", ""),
                    ind.get("price_usd", 0),
                    ind.get("rsi_14", 50),
                    ind.get("macd_line", 0),
                    ind.get("macd_signal", 0),
                    ind.get("macd_histogram", 0),
                    ind.get("sma_20", 0),
                    ind.get("sma_50", 0),
                    ind.get("bb_position", 0.5),
                    ind.get("volume_ratio", 1.0),
                    ind.get("signal", "neutral"),
                    ind.get("confidence", 50),
                ])
            results.append({
                "type": "table",
                "columns": columns,
                "rows": rows,
                "refId": ref_id,
            })

        elif target == "top_buys":
            top = summary.get("top_buys", [])
            columns = [
                {"text": "coin_id", "type": "string"},
                {"text": "symbol", "type": "string"},
                {"text": "action", "type": "string"},
                {"text": "confidence", "type": "number"},
                {"text": "risk_level", "type": "string"},
                {"text": "reasoning", "type": "string"},
            ]
            rows = [[r.get("coin_id", ""), r.get("symbol", ""), r.get("action", ""),
                      r.get("confidence", 0), r.get("risk_level", ""), r.get("reasoning", "")]
                     for r in top]
            results.append({
                "type": "table",
                "columns": columns,
                "rows": rows,
                "refId": ref_id,
            })

        elif target == "top_sells":
            top = summary.get("top_sells", [])
            columns = [
                {"text": "coin_id", "type": "string"},
                {"text": "symbol", "type": "string"},
                {"text": "action", "type": "string"},
                {"text": "confidence", "type": "number"},
                {"text": "risk_level", "type": "string"},
                {"text": "reasoning", "type": "string"},
            ]
            rows = [[r.get("coin_id", ""), r.get("symbol", ""), r.get("action", ""),
                      r.get("confidence", 0), r.get("risk_level", ""), r.get("reasoning", "")]
                     for r in top]
            results.append({
                "type": "table",
                "columns": columns,
                "rows": rows,
                "refId": ref_id,
            })

        else:
            results.append({"target": target, "refId": ref_id, "datapoints": []})

    return results


@app.get("/tag-keys")
def simpod_tag_keys():
    """Simpod /tag-keys: return available tag keys."""
    return [
        {"type": "string", "text": "coin_id"},
        {"type": "string", "text": "signal"},
    ]


@app.get("/tag-values")
def simpod_tag_values():
    """Simpod /tag-values: return available tag values."""
    indicators = _load_latest_json("indicators")
    recs = _load_latest_json("recommendations")

    coin_ids = list({i.get("coin_id") for i in indicators} | {r.get("coin_id") for r in recs})
    signals = list({i.get("signal") for i in indicators})

    return [
        {"text": cid} for cid in sorted(coin_ids)
    ] + [
        {"text": s} for s in sorted(signals)
    ]


# ---------------------------------------------------------------------------
# Standard API Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health():
    """Service health check."""
    recs = _load_latest_json("recommendations")
    indicators = _load_latest_json("indicators")
    return {
        "status": "ok",
        "recommendations_loaded": len(recs),
        "indicators_loaded": len(indicators),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/recommendations")
def get_recommendations():
    """Get all current recommendations."""
    data = _load_latest_json("recommendations")
    if not data:
        return {"recommendations": [], "message": "No recommendations available yet. Run the RAG pipeline first."}
    return {"recommendations": data}


@app.get("/api/recommendations/{coin_id}")
def get_recommendation(coin_id: str):
    """Get recommendation for a specific coin."""
    data = _load_latest_json("recommendations")
    for rec in data:
        if rec.get("coin_id") == coin_id:
            return {"recommendation": rec}
    raise HTTPException(status_code=404, detail=f"No recommendation found for {coin_id}")


@app.get("/api/indicators")
def get_indicators():
    """Get all current technical indicators."""
    data = _load_latest_json("indicators")
    if not data:
        return {"indicators": [], "message": "No indicators available yet. Run the analysis pipeline first."}
    return {"indicators": data}


@app.get("/api/indicators/{coin_id}")
def get_indicator(coin_id: str):
    """Get indicators for a specific coin."""
    data = _load_latest_json("indicators")
    for ind in data:
        if ind.get("coin_id") == coin_id:
            return {"indicator": ind}
    raise HTTPException(status_code=404, detail=f"No indicators found for {coin_id}")


@app.get("/api/summary")
def get_summary():
    """Get a summary dashboard data: top signals, market sentiment."""
    summary = _get_summary_data()
    if not summary:
        return {"message": "No data available"}
    return summary


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
