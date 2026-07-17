"""
CoinGecko HTTP Client

Provides a reusable HTTP client for interacting with the
CoinGecko REST API.

Features
--------
- Persistent HTTP session
- Automatic retries
- Configurable timeout
- API key authentication
- Centralized error handling
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.config import (
    API_KEY,
    BASE_URL,
    TIMEOUT,
    MAX_RETRIES
)


class CoinGeckoClient:
    """
    Reusable HTTP client for CoinGecko.
    """

    def __init__(self):

        self.base_url = BASE_URL

        self.session = requests.Session()

        retries = Retry(
            total=MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[
                429,
                500,
                502,
                503,
                504
            ],
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(max_retries=retries)

        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.session.headers.update({
            "x-cg-demo-api-key": API_KEY,
            "Accept": "application/json"
        })

    def get(self, endpoint, params=None):
        """
        Perform a GET request.

        Parameters
        ----------
        endpoint : str
            API endpoint

        params : dict
            Query parameters

        Returns
        -------
        dict
            JSON response
        """

        url = f"{self.base_url}{endpoint}"

        response = self.session.get(
            url,
            params=params,
            timeout=TIMEOUT
        )

        response.raise_for_status()

        return response.json()