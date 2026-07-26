"""
Batch ingestion: pull daily OHLCV history from Yahoo Finance and upload to MinIO.

Fetches the last 2 years of daily bars for every ticker in config/tickers.yaml,
writes each result as Parquet, and uploads to the marketpulse-raw bucket.

Standalone usage (from project root, with Postgres + MinIO running):

    python ingestion/batch_pull.py

When run from inside Airflow's Docker network, set MINIO_ENDPOINT=minio:9000 in .env.
When run from the host machine, use MINIO_ENDPOINT=localhost:9000 (default).
"""

from __future__ import annotations

import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
import yfinance as yf
from dotenv import load_dotenv

try:
    from ingestion.utils import ensure_bucket, get_minio_client
except ImportError:
    from utils import ensure_bucket, get_minio_client

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TICKERS_PATH = PROJECT_ROOT / "config" / "tickers.yaml"
DEFAULT_BUCKET = "marketpulse-raw"
OHLCV_COLUMNS = ["open", "high", "low", "close", "volume"]


def load_tickers(tickers_path: Path | None = None) -> list[str]:
    """Load ticker symbols from YAML config."""
    path = tickers_path or DEFAULT_TICKERS_PATH
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    tickers = data.get("tickers", [])
    if not tickers:
        raise ValueError(f"No tickers found in {path}")

    return tickers


def fetch_ohlcv(ticker: str, period: str = "2y") -> pd.DataFrame | None:
    """Fetch daily OHLCV history for a single ticker."""
    logger.info("Fetching %s (period=%s)", ticker, period)
    try:
        df = yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=False)
    except Exception:
        logger.exception("Failed to fetch %s", ticker)
        return None

    if df is None or df.empty:
        logger.warning("No data returned for %s", ticker)
        return None

    df = df.reset_index()
    rename_map = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    }
    df = df.rename(columns=rename_map)

    missing = [col for col in OHLCV_COLUMNS if col not in df.columns]
    if missing:
        logger.warning("Missing columns for %s: %s", ticker, missing)
        return None

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True)

    return df[["date", *OHLCV_COLUMNS]]


def enrich_and_save_parquet(
    df: pd.DataFrame,
    ticker: str,
    ingested_at: datetime,
    output_path: Path,
) -> None:
    """Add metadata columns and write Parquet to disk."""
    enriched = df.copy()
    enriched["ticker"] = ticker
    enriched["ingested_at"] = ingested_at
    enriched.to_parquet(output_path, index=False)


def upload_to_minio(
    local_path: Path,
    ticker: str,
    run_date: str,
    bucket: str = DEFAULT_BUCKET,
) -> str:
    """Upload a local Parquet file to MinIO and return the object key."""
    object_key = f"raw/equities/{ticker}/{run_date}/{ticker}_{run_date}.parquet"
    client = get_minio_client()
    client.upload_file(str(local_path), bucket, object_key)
    return object_key


def process_ticker(
    ticker: str,
    run_date: str,
    ingested_at: datetime,
    temp_dir: Path,
    bucket: str = DEFAULT_BUCKET,
) -> bool:
    """Fetch, save, and upload data for one ticker. Returns True on success."""
    logger.info("Starting ingestion for %s", ticker)

    df = fetch_ohlcv(ticker)
    if df is None:
        return False

    logger.info("Fetched %d rows for %s", len(df), ticker)

    local_filename = f"{ticker}_{run_date}.parquet"
    local_path = temp_dir / local_filename

    try:
        enrich_and_save_parquet(df, ticker, ingested_at, local_path)
        object_key = upload_to_minio(local_path, ticker, run_date, bucket=bucket)
        logger.info("Uploaded %s to s3://%s/%s", ticker, bucket, object_key)
        return True
    except Exception:
        logger.exception("Failed to process %s", ticker)
        return False
    finally:
        if local_path.exists():
            local_path.unlink()


def run_batch_pull(
    tickers: list[str] | None = None,
    tickers_path: Path | None = None,
    bucket: str = DEFAULT_BUCKET,
) -> dict[str, int | list[str]]:
    """
    Run batch ingestion for the given tickers (or all tickers from config).

    Returns a summary dict with keys: total, succeeded, failed, failed_tickers.
    """
    load_dotenv(PROJECT_ROOT / ".env")

    if tickers is None:
        tickers = load_tickers(tickers_path)
    ingested_at = datetime.now(timezone.utc)
    run_date = ingested_at.strftime("%Y-%m-%d")

    logger.info(
        "Starting batch pull for %d tickers (run_date=%s)",
        len(tickers),
        run_date,
    )

    ensure_bucket(bucket)

    succeeded: list[str] = []
    failed: list[str] = []

    with tempfile.TemporaryDirectory(prefix="marketpulse_batch_") as temp_dir:
        temp_path = Path(temp_dir)
        for ticker in tickers:
            if process_ticker(ticker, run_date, ingested_at, temp_path, bucket=bucket):
                succeeded.append(ticker)
            else:
                failed.append(ticker)

    logger.info(
        "Batch pull complete: %d succeeded, %d failed",
        len(succeeded),
        len(failed),
    )
    if failed:
        logger.warning("Failed tickers: %s", ", ".join(failed))

    return {
        "total": len(tickers),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "failed_tickers": failed,
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    summary = run_batch_pull()
    if summary["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
