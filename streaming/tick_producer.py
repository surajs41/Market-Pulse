"""
Simulated live market tick producer for MarketPulse.

IMPORTANT: This is NOT a real live feed. It replays the last 5 days of
1-minute intraday OHLCV history from Yahoo Finance in a continuous loop,
sleeping between messages to simulate real-time tick delivery. Suitable
for local development and portfolio demos — not for production trading.

Standalone usage (from project root, with Redpanda running):

    python streaming/tick_producer.py
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
import yfinance as yf
from dotenv import load_dotenv
from kafka.errors import KafkaError
from kafka.producer.kafka import KafkaProducer

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TICKERS_PATH = PROJECT_ROOT / "config" / "tickers.yaml"
TOPIC = "market-ticks"

logger = logging.getLogger(__name__)


def load_tickers(tickers_path: Path | None = None) -> list[str]:
    """Load ticker symbols from config/tickers.yaml."""
    path = tickers_path or DEFAULT_TICKERS_PATH
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    tickers = data.get("tickers", [])
    if not tickers:
        raise ValueError(f"No tickers found in {path}")
    return tickers


def fetch_intraday_data(ticker: str) -> pd.DataFrame:
    """Fetch the last 5 days of 1-minute OHLCV bars for a ticker."""
    logger.info("Fetching 5d/1m history for %s", ticker)
    try:
        df = yf.Ticker(ticker).history(
            period="5d",
            interval="1m",
            auto_adjust=False,
        )
    except Exception:
        logger.warning("Failed to fetch intraday data for %s", ticker, exc_info=True)
        return pd.DataFrame()

    if df is None or df.empty:
        logger.warning("No intraday data returned for %s", ticker)
        return pd.DataFrame()

    df = df.reset_index()
    rename_map = {
        "Datetime": "timestamp",
        "Date": "timestamp",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    }
    return df.rename(columns=rename_map)


def row_to_message(ticker: str, row: pd.Series, produced_at: datetime) -> dict:
    """Build a JSON-serialisable tick message from a DataFrame row."""
    ts = row["timestamp"]
    if hasattr(ts, "to_pydatetime"):
        ts = ts.to_pydatetime()
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)

    return {
        "ticker": ticker,
        "timestamp": ts.isoformat(),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row["volume"]),
        "produced_at": produced_at.isoformat(),
    }


def create_producer(brokers: str) -> KafkaProducer:
    """Return a Kafka producer configured for Redpanda."""
    return KafkaProducer(
        bootstrap_servers=[b.strip() for b in brokers.split(",") if b.strip()],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )


def run_producer() -> None:
    """Continuously replay intraday history as simulated live ticks."""
    load_dotenv(PROJECT_ROOT / ".env")

    brokers = os.getenv("REDPANDA_BROKERS", "localhost:9092")
    tick_interval = float(os.getenv("TICK_INTERVAL_SECONDS", "0.5"))

    tickers = load_tickers()
    logger.info(
        "Starting tick producer for %d tickers → topic %s (brokers=%s, interval=%ss)",
        len(tickers),
        TOPIC,
        brokers,
        tick_interval,
    )
    logger.info(
        "Simulated feed: replaying recent 5d/1m Yahoo Finance history in a loop "
        "(not a real live market stream)."
    )

    producer = create_producer(brokers)
    message_count = 0

    try:
        while True:
            for ticker in tickers:
                df = fetch_intraday_data(ticker)
                if df.empty:
                    continue

                for _, row in df.iterrows():
                    produced_at = datetime.now(timezone.utc)
                    message = row_to_message(ticker, row, produced_at)

                    try:
                        future = producer.send(TOPIC, value=message)
                        future.get(timeout=10)
                        message_count += 1
                        if message_count % 10 == 0:
                            logger.info(
                                "Produced %d messages (latest: %s @ %s)",
                                message_count,
                                ticker,
                                message["timestamp"],
                            )
                    except KafkaError:
                        logger.warning(
                            "Failed to produce message for %s",
                            ticker,
                            exc_info=True,
                        )

                    time.sleep(tick_interval)

            logger.info(
                "Completed replay cycle for all tickers (%d messages so far); restarting",
                message_count,
            )
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user — shutting down")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed after %d messages", message_count)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    run_producer()


if __name__ == "__main__":
    main()
