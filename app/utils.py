"""Shared helpers for the Streamlit RAG dashboard."""

import os
import requests
import streamlit as st


def get_rag_api_url() -> str:
    """Return the RAG API base URL based on environment."""
    return os.getenv("RAG_API_URL", "http://rag-api:8000")


def get_airflow_api_url() -> str:
    """Return the Airflow REST API base URL."""
    return os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080")


RAG_BASE = get_rag_api_url()
AIRFLOW_BASE = get_airflow_api_url()


@st.cache_data(ttl=60)
def fetch_summary() -> dict:
    """Fetch /api/summary from the RAG API."""
    try:
        r = requests.get(f"{RAG_BASE}/api/summary", timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        st.warning(f"Cannot reach RAG API: {e}")
        return {}


@st.cache_data(ttl=60)
def fetch_recommendations() -> list[dict]:
    """Fetch /api/recommendations from the RAG API."""
    try:
        r = requests.get(f"{RAG_BASE}/api/recommendations", timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("recommendations", [])
    except requests.RequestException:
        return []


@st.cache_data(ttl=60)
def fetch_indicators() -> list[dict]:
    """Fetch /api/indicators from the RAG API."""
    try:
        r = requests.get(f"{RAG_BASE}/api/indicators", timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("indicators", [])
    except requests.RequestException:
        return []


def check_api_health() -> dict:
    """Ping /api/health and return the response."""
    try:
        r = requests.get(f"{RAG_BASE}/api/health", timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return {}
