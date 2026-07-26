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

## Streaming Layer

### Why Redpanda?

[Redpanda](https://redpanda.com/) is a Kafka-compatible streaming platform written in C++. It exposes the same producer/consumer API as Apache Kafka, but runs as a single lightweight binary with no ZooKeeper dependency — ideal for local development and portfolio projects where you want real streaming semantics without the operational overhead of a full Kafka cluster.

### Architecture

```
tick_producer.py  →  Redpanda (market-ticks topic)  →  tick_consumer.py  →  Postgres (streaming.market_ticks)
```

- **Producer** (`streaming/tick_producer.py`) — fetches the last 5 days of 1-minute OHLCV bars per ticker from Yahoo Finance and publishes JSON tick messages to the `market-ticks` topic, sleeping between messages to simulate a live feed.
- **Consumer** (`streaming/tick_consumer.py`) — reads from `market-ticks` and batch-inserts rows into `streaming.market_ticks` in Postgres (commits every 10 messages).

**Important:** The tick data is **simulated**, not a real live market feed. The producer replays recent intraday history in a continuous loop. This is intentional for local dev — it lets you build and test the full streaming pipeline without a paid market data API.

Both scripts run on the **host machine** (not inside Docker) and connect to Redpanda via `localhost:9092` (`REDPANDA_BROKERS` in `.env`). Airflow / Docker services use the internal listener `redpanda:29092` (set in `docker-compose.yml`).

### Start Redpanda

Pull and start Redpanda only:

```powershell
docker compose up -d redpanda
```

Verify it is healthy:

```powershell
docker compose ps redpanda
docker compose exec redpanda curl -f http://localhost:9644/v1/status/ready
```

Create the `market-ticks` topic (3 partitions, replication factor 1):

```powershell
docker compose up redpanda-init
```

Confirm the topic exists:

```powershell
docker compose exec redpanda rpk topic list --brokers redpanda:29092
```

### Run producer and consumer

Install Python dependencies (if not already done):

```powershell
pip install -r requirements.txt
```

> **Note:** `requirements.txt` pins `kafka-python` to 2.x. Version 3.x pulls in `botocore` at import time and can hang on startup when you only need local Redpanda.

Ensure `.env` is configured (copy from `.env.example` if needed). Postgres must be running:

```powershell
docker compose up -d postgres
```

**Terminal 1 — start the consumer:**

```powershell
python streaming/tick_consumer.py
```

**Terminal 2 — start the producer:**

```powershell
python streaming/tick_producer.py
```

Wait a few seconds, then verify rows are landing in Postgres:

```powershell
docker compose exec postgres psql -U marketpulse -d marketpulse -c "SELECT COUNT(*) FROM streaming.market_ticks;"
docker compose exec postgres psql -U marketpulse -d marketpulse -c "SELECT ticker, ts, close, consumed_at FROM streaming.market_ticks ORDER BY id DESC LIMIT 10;"
```

Stop either script with `Ctrl+C`. The consumer commits every 10 messages and shuts down cleanly on interrupt.
