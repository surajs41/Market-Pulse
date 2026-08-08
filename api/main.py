"""
MarketPulse FastAPI serving layer.

Run from project root:

    uvicorn api.main:app --host 0.0.0.0 --port 8001 --reload
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.db import get_db_connection
from api.schemas import (
    ExplainResponse,
    ForecastResponse,
    HealthResponse,
    PricePoint,
    PriceResponse,
    TickersResponse,
)
from llm.explainer import explain_ticker
from ml.predictor import load_tickers, predict_next_day

FORECAST_DISCLAIMER = (
    "This is not financial advice. ML predictions are "
    "probabilistic and may be wrong."
)

# TODO: replace with Redis for production — in-memory cache resets on restart
_explain_cache: dict[str, tuple[datetime, dict]] = {}
EXPLAIN_CACHE_TTL = timedelta(hours=1)

PRICE_HISTORY_DAYS = 30

PRICE_QUERY = """
SELECT
    rp.date,
    rp.open,
    rp.high,
    rp.low,
    rp.close,
    rp.volume,
    dr.daily_return
FROM raw.daily_prices AS rp
LEFT JOIN analytics.daily_returns AS dr
    ON rp.ticker = dr.ticker
    AND rp.date = dr.date
WHERE rp.ticker = %s
ORDER BY rp.date DESC
LIMIT %s
"""

app = FastAPI(
    title="MarketPulse API",
    description=(
        "Financial market data pipeline with ML forecasting "
        "and LLM-powered explanations"
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _error(status_code: int, message: str) -> JSONResponse:
    """Return a consistent JSON error body."""
    return JSONResponse(status_code=status_code, content={"error": message})


def _validate_ticker(ticker: str) -> str | JSONResponse:
    """Return uppercase ticker if valid, else a 404 JSONResponse."""
    tickers = load_tickers()
    if ticker not in tickers:
        return _error(status_code=404, message=f"Ticker '{ticker}' not found")
    return ticker


@app.exception_handler(Exception)
async def unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    return _error(status_code=500, message=str(exc))


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Liveness check — no DB or external calls."""
    return HealthResponse(
        status="ok",
        timestamp=datetime.now(timezone.utc),
    )


@app.get("/tickers", response_model=TickersResponse)
def get_tickers() -> TickersResponse:
    """Return configured ticker symbols from config/tickers.yaml."""
    tickers = load_tickers()
    return TickersResponse(tickers=tickers, count=len(tickers))


@app.get("/price/{ticker}", response_model=PriceResponse)
def get_price_history(ticker: str) -> PriceResponse | JSONResponse:
    """Return last 30 days of daily OHLCV + daily return for a ticker."""
    validated = _validate_ticker(ticker)
    if isinstance(validated, JSONResponse):
        return validated
    ticker = validated

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(PRICE_QUERY, (ticker, PRICE_HISTORY_DAYS))
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return _error(
            status_code=404,
            message=f"No price data found for ticker '{ticker}'",
        )

    data = [
        PricePoint(
            date=row[0],
            open=float(row[1]) if row[1] is not None else None,
            high=float(row[2]) if row[2] is not None else None,
            low=float(row[3]) if row[3] is not None else None,
            close=float(row[4]) if row[4] is not None else None,
            volume=int(row[5]) if row[5] is not None else None,
            daily_return=float(row[6]) if row[6] is not None else None,
        )
        for row in rows
    ]
    data.sort(key=lambda point: point.date)

    return PriceResponse(ticker=ticker, data=data, count=len(data))


@app.get("/forecast/{ticker}", response_model=ForecastResponse)
def get_forecast(ticker: str) -> ForecastResponse | JSONResponse:
    """Return ML next-day direction prediction for a ticker."""
    validated = _validate_ticker(ticker)
    if isinstance(validated, JSONResponse):
        return validated
    ticker = validated

    result = predict_next_day(ticker)

    if "error" in result:
        message = result["error"]
        if "Model artifacts not found" in message or "train_model.py" in message:
            return _error(status_code=503, message=message)
        return _error(status_code=404, message=message)

    return ForecastResponse(
        ticker=result["ticker"],
        feature_date=result.get("feature_date"),
        prediction_date=result.get("prediction_date"),
        direction=result.get("direction"),
        confidence=result.get("confidence"),
        features_used=result.get("features_used"),
        disclaimer=FORECAST_DISCLAIMER,
    )


@app.get("/explain/{ticker}", response_model=ExplainResponse)
def get_explanation(ticker: str) -> ExplainResponse | JSONResponse:
    """Return LLM-generated plain-English market explanation for a ticker."""
    validated = _validate_ticker(ticker)
    if isinstance(validated, JSONResponse):
        return validated
    ticker = validated

    now = datetime.now(timezone.utc)
    cached_entry = _explain_cache.get(ticker)
    if cached_entry:
        cached_at, cached_result = cached_entry
        if now - cached_at < EXPLAIN_CACHE_TTL:
            return ExplainResponse(cached=True, **cached_result)

    result = explain_ticker(ticker)

    if result.get("error"):
        return _error(status_code=503, message=result["error"])

    if not result.get("explanation"):
        return _error(
            status_code=503,
            message="LLM returned an empty explanation",
        )

    payload = {
        "ticker": result["ticker"],
        "explanation": result["explanation"],
        "context": result.get("context"),
        "model_used": result.get("model_used"),
        "provider": result.get("provider"),
        "generated_at": result.get("generated_at"),
    }
    _explain_cache[ticker] = (now, payload)

    return ExplainResponse(cached=False, **payload)
