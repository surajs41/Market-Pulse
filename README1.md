Run MarketPulse — Day 2 Checklist
Follow these steps in order. Do them from PowerShell.

Step 0: Open the correct folder
cd C:\Users\suraj\OneDrive\Desktop\MarketPulse\Market-Pulse
All commands below run from this folder.

Step 1: Start Docker Desktop
Open Docker Desktop from the Start menu.
Wait until it says "Docker Desktop is running" (whale icon in system tray).
Verify:

docker info
If you see system info (not a connection error), continue.

Step 2: Activate Python environment
If you already created a venv on day 1:

.\venv\Scripts\Activate.ps1
If you never created one:

python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
If .env doesn't exist yet:

Copy-Item .env.example .env
Step 3: Start all Docker services
docker compose up -d
Wait ~30–60 seconds, then check status:

docker compose ps
You want these healthy or running:

marketpulse-postgres
marketpulse-minio
marketpulse-redpanda
marketpulse-airflow-webserver
marketpulse-airflow-scheduler
Create the streaming topic (only needed once, or after a fresh Redpanda volume):

docker compose up redpanda-init
Step 4: Run the batch pipeline (daily data → MinIO)
Option A — Airflow UI (recommended)

Open http://localhost:8080
Login: admin / change_me_airflow
Find marketpulse_batch_ingestion
Click the play button to trigger a manual run
Wait until both tasks turn green
Option B — Run directly without Airflow

python ingestion/batch_pull.py
Check MinIO: http://localhost:9001
Login: minioadmin / change_me_minio
Look for bucket marketpulse-raw.

Step 5: Run the streaming pipeline (ticks → Postgres)
Use two terminals, both in Market-Pulse/ with venv activated.

Terminal 1 — Consumer (start first, leave running):

cd C:\Users\suraj\OneDrive\Desktop\MarketPulse\Market-Pulse
.\venv\Scripts\Activate.ps1
python streaming/tick_consumer.py
Terminal 2 — Producer:

cd C:\Users\suraj\OneDrive\Desktop\MarketPulse\Market-Pulse
.\venv\Scripts\Activate.ps1
python streaming/tick_producer.py
You should see:

Producer: Produced 10 messages...
Consumer: Consumed 50 messages so far
Step 6: Verify data landed
Streaming (Postgres):

docker compose exec postgres psql -U marketpulse -d marketpulse -c "SELECT COUNT(*) FROM streaming.market_ticks;"
Batch (MinIO): browse http://localhost:9001 → bucket marketpulse-raw.

Quick reference
What	URL / Command
Airflow
http://localhost:8080
MinIO
http://localhost:9001
Stop everything
docker compose down
Stop producer/consumer
Ctrl+C in each terminal
If something fails
Problem	Fix
no configuration file provided
You're in the wrong folder — use Market-Pulse
dockerDesktopLinuxEngine error
Start Docker Desktop and wait until it's fully running
Producer hangs on startup
Run pip install -r requirements.txt (needs kafka-python 2.x)
0 rows in Postgres
Start consumer before producer
Airflow login fails
Check .env for _AIRFLOW_WWW_USER_USERNAME and _AIRFLOW_WWW_USER_PASSWORD
Minimum daily workflow: Start Docker → docker compose up -d → run consumer + producer (or trigger the Airflow DAG). That's it.