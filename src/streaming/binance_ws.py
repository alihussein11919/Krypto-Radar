"""
Binance WebSocket Trade Stream

Connects to Binance's public WebSocket API and listens for
live trade events across multiple symbols simultaneously.

No API key required — these are public market data streams.

How it works
------------
Binance provides a "combined stream" endpoint that lets you
subscribe to multiple streams over a single WebSocket connection:

    wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade

Each message pushed by Binance looks like:
    {
      "stream": "btcusdt@trade",
      "data": {
        "e": "trade",       # event type
        "E": 1719270000000, # event time (ms)
        "s": "BTCUSDT",     # symbol
        "t": 123456789,     # trade ID
        "p": "105976.00",   # price
        "q": "0.00142",     # quantity
        "T": 1719270000000, # trade time (ms)
        "m": false          # was buyer the market maker?
      }
    }

Buffering strategy
------------------
Writing one file per trade would create millions of tiny files.
Instead we buffer messages in memory and flush to disk either:
  - Every FLUSH_INTERVAL seconds, OR
  - When the buffer reaches BUFFER_SIZE messages

This gives us reasonably-sized files while keeping latency low.

Why asyncio?
------------
WebSocket listening is I/O-bound — most of the time we're just
waiting for the next message to arrive. asyncio lets Python do
other things (buffer management, disk writes) while waiting,
without needing multiple threads.
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import websockets

from src.storage.json_writer import write_json
from src.utils.logger import logger

# Symbols to subscribe to — top 5 by volume
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"]

# Binance combined stream URL
WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)

# Flush to disk every N seconds
FLUSH_INTERVAL = 10

# Or flush when buffer reaches this size
BUFFER_SIZE = 500

# How long to run the stream (seconds). None = run forever.
RUN_DURATION = 60


class BinanceTradeStream:
    """
    Listens to Binance trade streams and buffers messages
    before writing them to the bronze layer.
    """

    def __init__(self):
        self.buffer: list[dict] = []
        self.last_flush = datetime.now(timezone.utc)
        self.total_received = 0
        self.total_written = 0

    def _should_flush(self) -> bool:
        """Return True if we should flush the buffer to disk."""
        elapsed = (datetime.now(timezone.utc) - self.last_flush).total_seconds()
        return len(self.buffer) >= BUFFER_SIZE or elapsed >= FLUSH_INTERVAL

    def _flush(self) -> None:
        """Write the current buffer to disk and reset it."""
        if not self.buffer:
            return

        # Group messages by symbol so each symbol gets its own file
        by_symbol: dict[str, list] = {}
        for msg in self.buffer:
            symbol = msg.get("s", "unknown").lower()
            by_symbol.setdefault(symbol, []).append(msg)

        for symbol, messages in by_symbol.items():
            write_json(
                messages,
                source="binance",
                dataset=f"trades/{symbol}"
            )
            self.total_written += len(messages)

        logger.info(
            f"[binance_ws] Flushed {len(self.buffer)} messages "
            f"({len(by_symbol)} symbols). "
            f"Total written: {self.total_written}"
        )

        self.buffer = []
        self.last_flush = datetime.now(timezone.utc)

    async def _listen(self, duration: int | None = None) -> None:
        """
        Open the WebSocket connection and listen for messages.

        Parameters
        ----------
        duration : int | None
            How many seconds to listen. None means run forever.
        """
        start = datetime.now(timezone.utc)

        logger.info(f"[binance_ws] Connecting to: {WS_URL}")
        logger.info(f"[binance_ws] Symbols: {SYMBOLS}")
        logger.info(
            f"[binance_ws] Will run for {duration}s" if duration
            else "[binance_ws] Running indefinitely (Ctrl+C to stop)"
        )

        async with websockets.connect(WS_URL) as ws:
            logger.info("[binance_ws] Connected.")

            async for raw_message in ws:
                # Check duration limit
                if duration:
                    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
                    if elapsed >= duration:
                        logger.info(f"[binance_ws] Duration {duration}s reached, stopping.")
                        break

                # Parse and buffer the message
                try:
                    msg = json.loads(raw_message)
                    trade_data = msg.get("data", {})
                    self.buffer.append(trade_data)
                    self.total_received += 1
                except json.JSONDecodeError as e:
                    logger.warning(f"[binance_ws] Failed to parse message: {e}")
                    continue

                # Flush if needed
                if self._should_flush():
                    self._flush()

        # Final flush on exit
        self._flush()
        logger.info(
            f"[binance_ws] Stream ended. "
            f"Received: {self.total_received}, Written: {self.total_written}"
        )

    def run(self, duration: int | None = RUN_DURATION) -> None:
        """
        Start the WebSocket stream.

        Parameters
        ----------
        duration : int | None
            Seconds to run. Defaults to RUN_DURATION (60s for testing).
            Pass None to run until Ctrl+C.
        """
        asyncio.run(self._listen(duration=duration))


def run_stream(duration: int | None = RUN_DURATION) -> None:
    """Entry point called from main.py."""
    stream = BinanceTradeStream()
    stream.run(duration=duration)
