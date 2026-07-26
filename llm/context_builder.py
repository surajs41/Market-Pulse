"""
Build structured market context for LLM explanations.

Queries dbt marts and ML predictions from local Postgres — no external APIs.

Standalone usage (from project root):

    python -c "from llm.context_builder import build_market_context; import json; print(json.dumps(build_market_context('NVDA'), indent=2, default=str))"
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ml.predictor import predict_next_day

logger = logging.getLogger(__name__)

LAST_5_DAYS_QUERY = """
SELECT
    dr.date,
    rp.close,
    dr.daily_return,
    rp.volume
FROM analytics.daily_returns AS dr
INNER JOIN raw.daily_prices AS rp
    ON dr.ticker = rp.ticker
    AND dr.date = rp.date
WHERE dr.ticker = %s
ORDER BY dr.date DESC
LIMIT 5
"""

LATEST_TECHNICALS_QUERY = """
SELECT ma_7, ma_20, ma_50, price_vs_ma20
FROM analytics.moving_averages
WHERE ticker = %s
ORDER BY date DESC
LIMIT 1
"""

LATEST_VOLATILITY_QUERY = """
SELECT rolling_std_20, rolling_std_5, high_low_range
FROM analytics.volatility_metrics
WHERE ticker = %s
ORDER BY date DESC
LIMIT 1
"""

SUMMARY_STATS_QUERY = """
SELECT
    AVG(dr.daily_return) AS avg_return_30d,
    STDDEV(dr.daily_return) AS vol_30d
FROM analytics.daily_returns AS dr
WHERE dr.ticker = %s
  AND dr.date >= (
      SELECT MAX(date) - INTERVAL '30 days'
      FROM analytics.daily_returns
      WHERE ticker = %s
  )
"""

RECENT_RETURNS_QUERY = """
SELECT daily_return
FROM analytics.daily_returns
WHERE ticker = %s
ORDER BY date DESC
LIMIT 3
"""


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


def _compute_recent_trend(recent_returns: list[float]) -> str:
    """Classify last 3 daily returns as bullish, bearish, or mixed."""
    if len(recent_returns) < 3:
        return "mixed"
    if all(r > 0 for r in recent_returns):
        return "bullish"
    if all(r < 0 for r in recent_returns):
        return "bearish"
    return "mixed"


def build_market_context(ticker: str) -> dict:
    """
    Query Postgres and assemble structured context for LLM explanation.

    Returns a dict with last_5_days, latest_technicals, latest_volatility,
    ml_prediction, and summary_stats.
    """
    logger.info("Building market context for %s", ticker)
    conn = get_db_connection()

    try:
        last_5_df = pd.read_sql(LAST_5_DAYS_QUERY, conn, params=(ticker,))
        last_5_df = last_5_df.sort_values("date")
        last_5_days = [
            {
                "date": str(row.date),
                "close": float(row.close),
                "daily_return": float(row.daily_return),
                "volume": int(row.volume) if pd.notna(row.volume) else None,
            }
            for row in last_5_df.itertuples(index=False)
        ]

        tech_df = pd.read_sql(LATEST_TECHNICALS_QUERY, conn, params=(ticker,))
        latest_technicals = None
        if not tech_df.empty:
            row = tech_df.iloc[0]
            latest_technicals = {
                "ma_7": float(row.ma_7),
                "ma_20": float(row.ma_20),
                "ma_50": float(row.ma_50),
                "price_vs_ma20": float(row.price_vs_ma20),
            }

        vol_df = pd.read_sql(LATEST_VOLATILITY_QUERY, conn, params=(ticker,))
        latest_volatility = None
        if not vol_df.empty:
            row = vol_df.iloc[0]
            latest_volatility = {
                "rolling_std_20": float(row.rolling_std_20),
                "rolling_std_5": float(row.rolling_std_5),
                "high_low_range": float(row.high_low_range),
            }

        stats_df = pd.read_sql(SUMMARY_STATS_QUERY, conn, params=(ticker, ticker))
        recent_df = pd.read_sql(RECENT_RETURNS_QUERY, conn, params=(ticker,))

        avg_return_30d = None
        vol_30d = None
        price_vs_ma20 = None

        if not stats_df.empty:
            row = stats_df.iloc[0]
            avg_return_30d = float(row.avg_return_30d) if pd.notna(row.avg_return_30d) else None
            vol_30d = float(row.vol_30d) if pd.notna(row.vol_30d) else None

        price_vs_ma20 = latest_technicals["price_vs_ma20"] if latest_technicals else None

        recent_returns = [
            float(r) for r in recent_df["daily_return"].tolist() if pd.notna(r)
        ]
        above_ma20 = None
        if price_vs_ma20 is not None:
            above_ma20 = price_vs_ma20 > 1.0

        summary_stats = {
            "avg_daily_return_30d": avg_return_30d,
            "return_volatility_30d": vol_30d,
            "price_above_ma20": above_ma20,
            "recent_trend": _compute_recent_trend(recent_returns),
        }

    finally:
        conn.close()

    ml_prediction = predict_next_day(ticker)

    return {
        "ticker": ticker,
        "last_5_days": last_5_days,
        "latest_technicals": latest_technicals,
        "latest_volatility": latest_volatility,
        "ml_prediction": ml_prediction,
        "summary_stats": summary_stats,
    }
