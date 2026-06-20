"""Shared helpers for MarketPulse ingestion scripts."""

import os

import boto3
from dotenv import load_dotenv

load_dotenv()


def get_minio_client():
    """
    Return a boto3 S3 client configured for the local MinIO instance.

    Endpoint is controlled by MINIO_ENDPOINT:
      - localhost:9000  when scripts run on the host machine
      - minio:9000      when scripts run inside Airflow/Docker (docker-compose sets this)
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    endpoint_url = endpoint if endpoint.startswith("http") else f"http://{endpoint}"

    access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD")

    if not access_key or not secret_key:
        raise ValueError(
            "MinIO credentials not set. Define MINIO_ACCESS_KEY/MINIO_SECRET_KEY "
            "or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD in .env"
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
