"""
MarketPulse batch ingestion DAG.

Pulls daily OHLCV history from Yahoo Finance for all configured tickers,
uploads Parquet files to MinIO (marketpulse-raw bucket), then loads them
into Postgres raw.daily_prices for dbt transformations.

Schedule: daily at 7:00 PM IST (13:30 UTC) — after US and Indian markets close.

Task chain: load_tickers → run_batch_ingestion → load_raw_to_postgres

Manual test (once Airflow is running):

    docker compose exec airflow-scheduler airflow dags test marketpulse_batch_ingestion 2025-06-20
"""

from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task
from pendulum import datetime

# 7:00 PM IST = 19:00 Asia/Kolkata = 13:30 UTC (IST is UTC+5:30).
# Airflow schedules in UTC by default, so 19:00 IST → cron minute 30, hour 13.
SCHEDULE_CRON = "30 13 * * *"


@dag(
    dag_id="marketpulse_batch_ingestion",
    schedule=SCHEDULE_CRON,
    # Fixed past date — never use datetime.now() as start_date. Airflow uses
    # start_date to bound backfills and determine the first eligible run; a
    # dynamic value would shift on every DAG parse and cause unpredictable scheduling.
    start_date=datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["marketpulse", "batch", "ingestion"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=__doc__,
)
def marketpulse_batch_ingestion():
    """
    Daily batch pipeline: Yahoo Finance → Parquet → MinIO → Postgres raw layer.

    Three-task chain so each stage can fail/retry independently:
      - load_tickers: read config/tickers.yaml
      - run_batch_ingestion: fetch OHLCV and upload Parquet to MinIO
      - load_raw_to_postgres: load Parquet into raw.daily_prices for dbt
    """

    @task
    def load_tickers() -> list[str]:
        from ingestion.batch_pull import load_tickers as _load_tickers

        tickers = _load_tickers()
        return tickers

    @task
    def run_batch_ingestion(tickers: list[str]) -> dict:
        from ingestion.batch_pull import run_batch_pull

        summary = run_batch_pull(tickers=tickers)
        if summary["failed"]:
            raise RuntimeError(
                f"Batch ingestion failed for {summary['failed']} ticker(s): "
                f"{summary['failed_tickers']}"
            )
        return summary

    @task
    def load_raw_to_postgres(_ingestion_summary: dict) -> dict:
        from ingestion.parquet_loader import run_loader

        return run_loader()

    load_raw_to_postgres(run_batch_ingestion(load_tickers()))


marketpulse_batch_ingestion()
