# MarketPulse

MarketPulse is a local, Docker-based, zero-cost end-to-end data engineering pipeline for financial market data. It ingests batch and streaming market data, stores it in Postgres and MinIO, orchestrates workflows with Apache Airflow, and layers on ML forecasting plus LLM-generated plain-English explanations of price movements.

**Status: Week 1 — In Progress**

## Architecture

### Orchestration

The `marketpulse_batch_ingestion` DAG (`dags/batch_ingestion_dag.py`) orchestrates the daily batch pull:

1. **load_tickers** — reads `config/tickers.yaml` and passes the symbol list via XCom
2. **run_batch_ingestion** — fetches 2 years of daily OHLCV per ticker and uploads Parquet to MinIO

**Schedule:** daily at 7:00 PM IST (13:30 UTC), after US and Indian markets have closed.

Once Airflow is running, open the UI at http://localhost:8080 to view the DAG graph, enable it, or trigger a manual run.

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

Use `MINIO_ENDPOINT=localhost:9000` when running on the host. Airflow containers use `minio:9000` automatically (set in `docker-compose.yml`).

### Start Airflow

Generate a Fernet key and add it to `.env`:

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste the output into AIRFLOW_FERNET_KEY in .env
```

Bring up the full stack (Postgres, MinIO, Airflow init + webserver + scheduler):

```powershell
docker compose up -d
```

Wait for all services to become healthy, then open http://localhost:8080 and log in with the credentials from `.env`:

- Username: `_AIRFLOW_WWW_USER_USERNAME` (default: `admin`)
- Password: `_AIRFLOW_WWW_USER_PASSWORD`

In the UI, unpause the `marketpulse_batch_ingestion` DAG to enable the daily schedule, or trigger it manually.

CLI test (runs the full DAG for a specific date without waiting for the scheduler):

```powershell
docker compose exec airflow-scheduler airflow dags test marketpulse_batch_ingestion 2025-06-20
```
