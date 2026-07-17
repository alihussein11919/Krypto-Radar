"""Crypto RAG Dashboard - Streamlit entry point."""

import streamlit as st
from utils import check_api_health

st.set_page_config(
    page_title="Crypto RAG Dashboard",
    page_icon="\U0001F4CA",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Sidebar ---
with st.sidebar:
    st.title("\U0001F4CA Crypto RAG")
    st.divider()

    health = check_api_health()
    if health.get("status") == "ok":
        st.success("RAG API: Connected")
        recs_count = health.get("recommendations_loaded", 0)
        inds_count = health.get("indicators_loaded", 0)
        st.caption(f"Recommendations: {recs_count} | Indicators: {inds_count}")
        st.caption(f"Data timestamp: {health.get('timestamp', 'N/A')}")
    else:
        st.error("RAG API: Unreachable")
        st.caption("Make sure rag-api service is running")

    st.divider()
    if st.button("\U0001F504 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.caption("Grafana dashboards: [localhost:3000](http://localhost:3000)")
    st.caption("Airflow UI: [localhost:8082](http://localhost:8082)")

# --- Main area ---
st.title("\U0001F4CA Crypto RAG Recommendations")

st.info(
    "Use the sidebar to navigate. The RAG pipeline generates daily buy/sell/hold "
    "recommendations for the top cryptocurrencies using technical analysis + LLM."
)

# Quick overview on main page
if health.get("status") == "ok":
    st.subheader("Quick Overview")

    from utils import fetch_summary

    summary = fetch_summary()
    if summary and "signal_counts" in summary:
        c1, c2, c3, c4 = st.columns(4)
        sentiment = summary.get("market_sentiment", "N/A")
        c1.metric("Market Sentiment", sentiment)
        c2.metric("Buy Signals", summary["signal_counts"].get("buy", 0))
        c3.metric("Sell Signals", summary["signal_counts"].get("sell", 0))
        c4.metric("Hold Signals", summary["signal_counts"].get("hold", 0))

        st.caption(
            f"Tracking {summary.get('total_coins', 0)} coins | "
            f"Generated: {summary.get('generated_at', 'N/A')}"
        )
