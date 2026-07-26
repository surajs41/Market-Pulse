"""
MarketPulse streaming consumer — reads ticks from Redpanda and persists to Postgres.

Standalone usage (from project root, with Redpanda + Postgres running):

    python streaming/tick_consumer.py
"""

from __future__ import annotations

import json
import logging
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from kafka.consumer.group import KafkaConsumer

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TOPIC = "market-ticks"
GROUP_ID = "marketpulse-consumer"
COMMIT_BATCH_SIZE = 10
LOG_EVERY_N = 50

logger = logging.getLogger(__name__)

_shutdown_requested = False


def _handle_shutdown(signum: int, _frame: object) -> None:
    global _shutdown_requested
    logger.info("Shutdown signal received (%s); finishing current batch…", signum)
    _shutdown_requested = True


def get_db_connection():
    """Open a psycopg2 connection using environment variables."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "marketpulse"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        dbname=os.getenv("POSTGRES_DB", "marketpulse"),
    )


def ensure_schema_and_table(conn) -> None:
    """Create the streaming schema and market_ticks table if missing."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS streaming")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS streaming.market_ticks (
                id          SERIAL PRIMARY KEY,
                ticker      VARCHAR(32) NOT NULL,
                ts          TIMESTAMPTZ NOT NULL,
                open        NUMERIC,
                high        NUMERIC,
                low         NUMERIC,
                close       NUMERIC,
                volume      NUMERIC,
                produced_at TIMESTAMPTZ,
                consumed_at TIMESTAMPTZ NOT NULL
            )
            """
        )
    conn.commit()
    logger.info("Ensured schema streaming.market_ticks exists")


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp string to a timezone-aware datetime."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def insert_tick(cur, message: dict, consumed_at: datetime) -> None:
    """Insert a single tick row."""
    cur.execute(
        """
        INSERT INTO streaming.market_ticks
            (ticker, ts, open, high, low, close, volume, produced_at, consumed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            message["ticker"],
            parse_timestamp(message["timestamp"]),
            message["open"],
            message["high"],
            message["low"],
            message["close"],
            message["volume"],
            parse_timestamp(message["produced_at"]),
            consumed_at,
        ),
    )


def run_consumer() -> None:
    """Consume ticks from Redpanda and batch-insert into Postgres."""
    load_dotenv(PROJECT_ROOT / ".env")

    brokers = os.getenv("REDPANDA_BROKERS", "localhost:9092")

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    conn = get_db_connection()
    ensure_schema_and_table(conn)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[b.strip() for b in brokers.split(",") if b.strip()],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    logger.info(
        "Consumer started (topic=%s, group=%s, brokers=%s)",
        TOPIC,
        GROUP_ID,
        brokers,
    )

    total_consumed = 0
    pending_since_commit = 0

    try:
        for record in consumer:
            if _shutdown_requested:
                break

            consumed_at = datetime.now(timezone.utc)
            try:
                with conn.cursor() as cur:
                    insert_tick(cur, record.value, consumed_at)
                pending_since_commit += 1
                total_consumed += 1

                if pending_since_commit >= COMMIT_BATCH_SIZE:
                    conn.commit()
                    consumer.commit()
                    pending_since_commit = 0

                if total_consumed % LOG_EVERY_N == 0:
                    logger.info("Consumed %d messages so far", total_consumed)

            except Exception:
                conn.rollback()
                logger.warning(
                    "Failed to insert tick for %s",
                    record.value.get("ticker", "unknown")
                    if isinstance(record.value, dict)
                    else "unknown",
                    exc_info=True,
                )

        if pending_since_commit:
            conn.commit()
            consumer.commit()
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user — shutting down")
        if pending_since_commit:
            conn.commit()
            consumer.commit()
    finally:
        logger.info("Shutting down — total messages consumed: %d", total_consumed)
        consumer.close()
        conn.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    # kafka-python is very chatty at INFO — keep our logs readable
    logging.getLogger("kafka").setLevel(logging.WARNING)
    run_consumer()


if __name__ == "__main__":
    main()
