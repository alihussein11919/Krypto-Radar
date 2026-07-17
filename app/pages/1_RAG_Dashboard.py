"""RAG Recommendations Dashboard - Full view."""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from utils import fetch_summary, fetch_recommendations, fetch_indicators

st.set_page_config(page_title="RAG Dashboard", page_icon="\U0001F4CA", layout="wide")

st.title("\U0001F4CA RAG Recommendations Dashboard")

# --- Section A: Market Overview ---
st.header("\U0001F4C8 Market Overview")

summary = fetch_summary()
if not summary or "signal_counts" not in summary:
    st.warning("No recommendation data available. Run the daily RAG pipeline first.")
    st.stop()

col1, col2, col3, col4 = st.columns(4)

sentiment = summary.get("market_sentiment", "N/A")
sentiment_color = {"BULLISH": "normal", "BEARISH": "inverse", "NEUTRAL": "normal"}
col1.metric("Market Sentiment", sentiment)
col2.metric("Buy Signals", summary["signal_counts"]["buy"])
col3.metric("Sell Signals", summary["signal_counts"]["sell"])
col4.metric("Hold Signals", summary["signal_counts"]["hold"])

st.caption(
    f"Tracking **{summary.get('total_coins', 0)}** coins | "
    f"Last generated: **{summary.get('generated_at', 'N/A')}**"
)

st.divider()

# --- Section B: All Recommendations ---
st.header("\U0001F4CB All Recommendations")

recommendations = fetch_recommendations()
if not recommendations:
    st.info("No recommendations available.")
else:
    recs_df = pd.DataFrame(recommendations)

    # Format confidence as percentage
    if "confidence" in recs_df.columns:
        recs_df["confidence"] = recs_df["confidence"].apply(lambda x: f"{x}%")

    # Color-code action column (cell-only, not full row)
    def style_action(val):
        actions = {
            "BUY":  "background-color: #d3f9d8; color: #087f5b; font-weight: bold",
            "SELL": "background-color: #ffe3e3; color: #c92a2a; font-weight: bold",
            "HOLD": "background-color: #fff3bf; color: #e67700; font-weight: bold",
        }
        return actions.get(str(val).upper(), "")

    display_cols = ["coin_id", "symbol", "name", "action", "confidence",
                    "risk_level", "reasoning", "generated_at"]
    existing_cols = [c for c in display_cols if c in recs_df.columns]

    styled_recs = recs_df[existing_cols].style
    if "action" in existing_cols:
        styled_recs = styled_recs.map(style_action, subset=["action"])

    st.dataframe(
        styled_recs,
        use_container_width=True,
        height=400,
    )

st.divider()

# --- Section C: Top Picks ---
st.header("\U0001F3C6 Top Picks")

top_buys = summary.get("top_buys", [])
top_sells = summary.get("top_sells", [])

col_buys, col_sells = st.columns(2)

with col_buys:
    st.subheader("\U0001F7E2 Top BUY Recommendations")
    if not top_buys:
        st.info("No buy recommendations")
    else:
        for rec in top_buys:
            with st.expander(f"**{rec['symbol'].upper()}** - {rec['name']} ({rec.get('confidence', 0)}%)"):
                st.write(f"**Action:** {rec['action']}")
                st.write(f"**Confidence:** {rec.get('confidence', 0)}%")
                st.write(f"**Risk Level:** {rec.get('risk_level', 'N/A')}")
                st.write(f"**Timeframe:** {rec.get('timeframe', 'N/A')}")
                st.write(f"**Similar Patterns:** {rec.get('similar_patterns', 0)}")
                st.write(f"**Reasoning:** {rec.get('reasoning', 'N/A')}")

        # Bar chart of buy confidence
        buy_symbols = [r["symbol"].upper() for r in top_buys]
        buy_confidence = [r.get("confidence", 0) for r in top_buys]
        fig_buy = go.Figure(data=[go.Bar(
            x=buy_symbols, y=buy_confidence,
            marker_color="#40c057",
            text=buy_confidence, textposition="auto",
        )])
        fig_buy.update_layout(
            yaxis_title="Confidence %", xaxis_title="Coin",
            height=250, margin=dict(t=10, b=10)
        )
        st.plotly_chart(fig_buy, use_container_width=True)

with col_sells:
    st.subheader("\U0001F534 Top SELL Recommendations")
    if not top_sells:
        st.info("No sell recommendations")
    else:
        for rec in top_sells:
            with st.expander(f"**{rec['symbol'].upper()}** - {rec['name']} ({rec.get('confidence', 0)}%)"):
                st.write(f"**Action:** {rec['action']}")
                st.write(f"**Confidence:** {rec.get('confidence', 0)}%")
                st.write(f"**Risk Level:** {rec.get('risk_level', 'N/A')}")
                st.write(f"**Timeframe:** {rec.get('timeframe', 'N/A')}")
                st.write(f"**Similar Patterns:** {rec.get('similar_patterns', 0)}")
                st.write(f"**Reasoning:** {rec.get('reasoning', 'N/A')}")

        # Bar chart of sell confidence
        sell_symbols = [r["symbol"].upper() for r in top_sells]
        sell_confidence = [r.get("confidence", 0) for r in top_sells]
        fig_sell = go.Figure(data=[go.Bar(
            x=sell_symbols, y=sell_confidence,
            marker_color="#fa5252",
            text=sell_confidence, textposition="auto",
        )])
        fig_sell.update_layout(
            yaxis_title="Confidence %", xaxis_title="Coin",
            height=250, margin=dict(t=10, b=10)
        )
        st.plotly_chart(fig_sell, use_container_width=True)

st.divider()

# --- Section D: Technical Indicators ---
st.header("\U0001F4CA Technical Indicators")

indicators = fetch_indicators()
if not indicators:
    st.info("No indicator data available.")
else:
    inds_df = pd.DataFrame(indicators)

    # Filter controls
    col_filter1, col_filter2 = st.columns(2)
    with col_filter1:
        signal_filter = st.multiselect(
            "Filter by Signal",
            options=["bullish", "bearish", "neutral"],
            default=["bullish", "bearish", "neutral"],
        )
    with col_filter2:
        sort_by = st.selectbox(
            "Sort by",
            options=["coin_id", "rsi_14", "confidence", "price_usd", "volume_ratio"],
            index=0,
        )

    # Apply filters
    if "signal" in inds_df.columns:
        filtered = inds_df[inds_df["signal"].isin(signal_filter)]
    else:
        filtered = inds_df

    # Sort
    if sort_by in filtered.columns:
        filtered = filtered.sort_values(sort_by)

    # Format for display
    display_df = filtered.copy()
    for col in ["price_usd", "rsi_14", "macd_histogram", "sma_20", "sma_50",
                "bb_position", "volume_ratio", "confidence"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].round(4)

    # Conditional formatting for RSI
    def style_rsi(val):
        try:
            v = float(val)
            if v > 70:
                return "background-color: #ffe3e3; color: #c92a2a; font-weight: bold"
            elif v < 30:
                return "background-color: #d3f9d8; color: #087f5b; font-weight: bold"
        except (ValueError, TypeError):
            pass
        return ""

    def style_signal(val):
        colors = {
            "bullish": "background-color: #d3f9d8; color: #087f5b; font-weight: bold",
            "bearish": "background-color: #ffe3e3; color: #c92a2a; font-weight: bold",
            "neutral": "background-color: #fff3bf; color: #e67700; font-weight: bold",
        }
        return colors.get(str(val).lower(), "")

    show_cols = ["coin_id", "symbol", "name", "price_usd", "rsi_14",
                 "macd_histogram", "sma_20", "sma_50", "bb_position",
                 "volume_ratio", "signal", "confidence"]
    existing = [c for c in show_cols if c in display_df.columns]

    styled = display_df[existing].style
    if "rsi_14" in existing:
        styled = styled.map(style_rsi, subset=["rsi_14"])
    if "signal" in existing:
        styled = styled.map(style_signal, subset=["signal"])

    st.dataframe(styled, use_container_width=True, height=500)

    # RSI distribution chart
    if "rsi_14" in inds_df.columns:
        st.subheader("RSI Distribution")
        fig_rsi = go.Figure(data=[go.Histogram(
            x=inds_df["rsi_14"].dropna(),
            nbinsx=20,
            marker_color="#868e96",
        )])
        fig_rsi.add_vline(x=30, line_dash="dash", line_color="green", annotation_text="Oversold (30)")
        fig_rsi.add_vline(x=70, line_dash="dash", line_color="red", annotation_text="Overbought (70)")
        fig_rsi.update_layout(
            xaxis_title="RSI (14)", yaxis_title="Count",
            height=300, margin=dict(t=30, b=30),
        )
        st.plotly_chart(fig_rsi, use_container_width=True)

    # Signal distribution pie chart
    if "signal" in inds_df.columns:
        st.subheader("Signal Distribution")
        signal_counts = inds_df["signal"].value_counts()
        fig_pie = go.Figure(data=[go.Pie(
            labels=signal_counts.index,
            values=signal_counts.values,
            marker_colors=["#40c057", "#fa5252", "#fab005"],
            textinfo="label+value",
        )])
        fig_pie.update_layout(height=300, margin=dict(t=30, b=30))
        st.plotly_chart(fig_pie, use_container_width=True)
