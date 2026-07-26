"""
Feature engineering for MarketPulse ML forecasting.

Joins dbt mart tables into a unified feature matrix with next-day targets.

Standalone usage (from project root, with Postgres + dbt marts populated):

    python ml/feature_store.py
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

FEATURE_QUERY = """
SELECT
    dr.ticker,
    dr.date,
    rp.close,
    rp.volume,
    dr.daily_return,
    dr.prev_close,
    ma.ma_7,
    ma.ma_20,
    ma.ma_50,
    ma.price_vs_ma20,
    vm.rolling_std_20,
    vm.rolling_std_5,
    vm.high_low_range,
    vm.avg_volume_20
FROM analytics.daily_returns AS dr
INNER JOIN analytics.moving_averages AS ma
    ON dr.ticker = ma.ticker
    AND dr.date = ma.date
INNER JOIN analytics.volatility_metrics AS vm
    ON dr.ticker = vm.ticker
    AND dr.date = vm.date
INNER JOIN raw.daily_prices AS rp
    ON dr.ticker = rp.ticker
    AND dr.date = rp.date
ORDER BY dr.ticker, dr.date
"""

LATEST_FEATURE_QUERY = """
SELECT
    dr.ticker,
    dr.date,
    rp.close,
    rp.volume,
    dr.daily_return,
    dr.prev_close,
    ma.ma_7,
    ma.ma_20,
    ma.ma_50,
    ma.price_vs_ma20,
    vm.rolling_std_20,
    vm.rolling_std_5,
    vm.high_low_range,
    vm.avg_volume_20
FROM analytics.daily_returns AS dr
INNER JOIN analytics.moving_averages AS ma
    ON dr.ticker = ma.ticker
    AND dr.date = ma.date
INNER JOIN analytics.volatility_metrics AS vm
    ON dr.ticker = vm.ticker
    AND dr.date = vm.date
INNER JOIN raw.daily_prices AS rp
    ON dr.ticker = rp.ticker
    AND dr.date = rp.date
WHERE dr.ticker = %s
  AND dr.date = (
      SELECT MAX(dr2.date)
      FROM analytics.daily_returns AS dr2
      WHERE dr2.ticker = %s
  )
"""

FEATURE_COLUMNS = [
    "close",
    "volume",
    "daily_return",
    "prev_close",
    "ma_7",
    "ma_20",
    "ma_50",
    "price_vs_ma20",
    "rolling_std_20",
    "rolling_std_5",
    "high_low_range",
    "avg_volume_20",
]


def get_db_connection():
    """Open a psycopg2 connection using environment variables."""
    load_dotenv(PROJECT_ROOT / ".env")
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "marketpulse"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        dbname=os.getenv("POSTGRES_DB", "marketpulse"),
    )


def _add_targets(df: pd.DataFrame) -> pd.DataFrame:
    """Add next-day return and binary direction target per ticker."""
    df = df.sort_values(["ticker", "date"]).copy()
    df["next_day_return"] = df.groupby("ticker")["daily_return"].shift(-1)
    df["target_direction"] = (df["next_day_return"] > 0).astype("Int64")
    return df


def build_feature_table() -> pd.DataFrame:
    """
    Build the unified ML feature table from dbt marts in Postgres.

    Returns a dataframe with feature columns plus next_day_return and
    target_direction. Rows with null targets or null features are dropped.
    """
    logger.info("Loading feature data from Postgres (dbt marts)")
    conn = get_db_connection()
    try:
        df = pd.read_sql(FEATURE_QUERY, conn)
    finally:
        conn.close()

    logger.info("Loaded %d rows from analytics marts", len(df))

    df["date"] = pd.to_datetime(df["date"])
    df = _add_targets(df)

    before = len(df)
    df = df.dropna(subset=["next_day_return"])
    logger.info("Dropped %d rows with null target (last day per ticker)", before - len(df))

    before = len(df)
    df = df.dropna(subset=FEATURE_COLUMNS)
    logger.info("Dropped %d rows with null features (warm-up window)", before - len(df))

    logger.info("Final feature table: %d rows, %d tickers", len(df), df["ticker"].nunique())
    return df


def get_latest_features(ticker: str) -> pd.DataFrame:
    """Return the most recent feature row for a single ticker."""
    conn = get_db_connection()
    try:
        df = pd.read_sql(LATEST_FEATURE_QUERY, conn, params=(ticker, ticker))
    finally:
        conn.close()

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    return df.dropna(subset=FEATURE_COLUMNS)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    df = build_feature_table()
    print(f"\nFeature table shape: {df.shape}")
    print("\nFirst 5 rows:")
    print(df.head().to_string(index=False))

    class_counts = df["target_direction"].value_counts().sort_index()
    print("\nClass balance (target_direction):")
    print(f"  DOWN (0): {class_counts.get(0, 0)}")
    print(f"  UP   (1): {class_counts.get(1, 0)}")


if __name__ == "__main__":
    main()
