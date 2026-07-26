"""
Load batch Parquet files from MinIO into Postgres raw.daily_prices.

Reads all Parquet objects under s3://marketpulse-raw/raw/equities/ and
upserts rows idempotently (ON CONFLICT DO NOTHING on ticker + date).

Standalone usage (from project root, with Postgres + MinIO running):

    python ingestion/parquet_loader.py
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

try:
    from ingestion.utils import get_minio_client
except ImportError:
    from utils import get_minio_client

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BUCKET = "marketpulse-raw"
DEFAULT_PREFIX = "raw/equities/"


def _postgres_host() -> str:
    """Use Docker service name when running inside Airflow containers."""
    explicit = os.getenv("POSTGRES_HOST")
    if explicit:
        return explicit
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "")
    if minio_endpoint.startswith("minio"):
        return "postgres"
    return "localhost"


def get_db_connection():
    """Open a psycopg2 connection using environment variables."""
    return psycopg2.connect(
        host=_postgres_host(),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "marketpulse"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        dbname=os.getenv("POSTGRES_DB", "marketpulse"),
    )


def ensure_schema_and_table(conn) -> None:
    """Create raw schema, table, and unique constraint if missing."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw.daily_prices (
                ticker      VARCHAR(32) NOT NULL,
                date        DATE NOT NULL,
                open        NUMERIC,
                high        NUMERIC,
                low         NUMERIC,
                close       NUMERIC,
                adj_close   NUMERIC,
                volume      BIGINT,
                ingested_at TIMESTAMPTZ,
                UNIQUE (ticker, date)
            )
            """
        )
    conn.commit()
    logger.info("Ensured raw.daily_prices exists")


def list_parquet_keys(bucket: str, prefix: str) -> list[str]:
    """Return all Parquet object keys under the given MinIO prefix."""
    client = get_minio_client()
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)
    logger.info("Found %d Parquet file(s) under %s", len(keys), prefix)
    return keys


def read_parquet_object(bucket: str, key: str) -> pd.DataFrame:
    """Download a Parquet object from MinIO and return as DataFrame."""
    client = get_minio_client()
    response = client.get_object(Bucket=bucket, Key=key)
    try:
        return pd.read_parquet(io.BytesIO(response["Body"].read()))
    finally:
        response["Body"].close()


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Map Parquet columns to raw.daily_prices schema."""
    rename_map = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    df = df.rename(columns=rename_map)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True).dt.date

    if "adj_close" not in df.columns:
        df["adj_close"] = df["close"] if "close" in df.columns else None

    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    required = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume", "ingested_at"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Parquet file missing required columns: {missing}")

    df = df[df["close"].notna() & (df["close"] > 0)]
    return df[required]


def insert_rows(conn, rows: list[tuple]) -> int:
    """Insert rows idempotently; return count of newly inserted rows."""
    if not rows:
        return 0

    with conn.cursor() as cur:
        sql = """
            INSERT INTO raw.daily_prices
                (ticker, date, open, high, low, close, adj_close, volume, ingested_at)
            VALUES %s
            ON CONFLICT (ticker, date) DO NOTHING
        """
        execute_values(cur, sql, rows, page_size=500)
        inserted = cur.rowcount
    conn.commit()
    return inserted


def load_parquet_key(conn, bucket: str, key: str) -> tuple[int, int]:
    """Load one Parquet file; return (rows_read, rows_inserted)."""
    logger.info("Loading %s", key)
    df = read_parquet_object(bucket, key)
    if df.empty:
        logger.warning("Skipping empty file: %s", key)
        return 0, 0

    df = normalize_dataframe(df)
    rows = [
        (
            row.ticker,
            row.date,
            float(row.open) if pd.notna(row.open) else None,
            float(row.high) if pd.notna(row.high) else None,
            float(row.low) if pd.notna(row.low) else None,
            float(row.close) if pd.notna(row.close) else None,
            float(row.adj_close) if pd.notna(row.adj_close) else None,
            int(row.volume) if pd.notna(row.volume) else None,
            row.ingested_at.to_pydatetime() if hasattr(row.ingested_at, "to_pydatetime") else row.ingested_at,
        )
        for row in df.itertuples(index=False)
    ]
    inserted = insert_rows(conn, rows)
    logger.info("Loaded %s: %d row(s) read, %d inserted", key, len(rows), inserted)
    return len(rows), inserted


def run_loader(
    bucket: str = DEFAULT_BUCKET,
    prefix: str = DEFAULT_PREFIX,
) -> dict[str, int]:
    """Load all Parquet files from MinIO into raw.daily_prices."""
    load_dotenv(PROJECT_ROOT / ".env")

    conn = get_db_connection()
    ensure_schema_and_table(conn)

    keys = list_parquet_keys(bucket, prefix)
    total_read = 0
    total_inserted = 0
    files_processed = 0

    for key in keys:
        try:
            read_count, inserted_count = load_parquet_key(conn, bucket, key)
            total_read += read_count
            total_inserted += inserted_count
            files_processed += 1
        except Exception:
            logger.exception("Failed to load %s", key)

    conn.close()

    summary = {
        "files_found": len(keys),
        "files_processed": files_processed,
        "rows_read": total_read,
        "rows_inserted": total_inserted,
    }
    logger.info(
        "Parquet load complete: %d file(s), %d row(s) read, %d inserted",
        files_processed,
        total_read,
        total_inserted,
    )
    return summary


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    run_loader()


if __name__ == "__main__":
    main()
