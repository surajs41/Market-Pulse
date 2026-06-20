# MarketPulse

MarketPulse is a local, Docker-based, zero-cost end-to-end data engineering pipeline for financial market data. It ingests batch and streaming market data, stores it in Postgres and MinIO, orchestrates workflows with Apache Airflow, and layers on ML forecasting plus LLM-generated plain-English explanations of price movements.

**Status: Week 1 — In Progress**

## Architecture

<!-- TODO: diagram and component overview -->

## Setup

### Prerequisites

- Docker Desktop (Postgres + MinIO running via `docker compose up -d postgres minio`)
- Python 3.11+

### Python environment

From the project root:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Copy environment variables and adjust credentials if needed:

```powershell
Copy-Item .env.example .env
```

Ensure MinIO has the `marketpulse-raw` bucket created (via the MinIO console at http://localhost:9001).

### Run batch ingestion (standalone test)

With Postgres and MinIO up, run from the project root:

```powershell
python ingestion/batch_pull.py
```

The script reads tickers from `config/tickers.yaml`, fetches 2 years of daily OHLCV from Yahoo Finance, and uploads Parquet files to:

`s3://marketpulse-raw/raw/equities/{ticker}/{date}/{ticker}_{date}.parquet`

Use `MINIO_ENDPOINT=localhost:9000` when running on the host. When this script runs inside Airflow containers later, set `MINIO_ENDPOINT=minio:9000`.
