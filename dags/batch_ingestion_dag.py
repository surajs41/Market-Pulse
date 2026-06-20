"""
MarketPulse batch ingestion DAG.

Pulls daily OHLCV history from Yahoo Finance for all configured tickers and
uploads Parquet files to MinIO (marketpulse-raw bucket).

Schedule: daily at 7:00 PM IST (13:30 UTC) — after US and Indian markets close.

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
    Daily batch pull: Yahoo Finance → Parquet → MinIO.

    Split into two tasks rather than one monolithic task so that:
      - load_tickers can be unit-tested and retried independently of the pull
      - failures in the fetch/upload step don't re-read config unnecessarily
      - the Airflow graph view shows clear stage boundaries (config → ingest)
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

    run_batch_ingestion(load_tickers())


marketpulse_batch_ingestion()
