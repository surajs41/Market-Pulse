"""Pydantic response models for the MarketPulse API."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime


class TickersResponse(BaseModel):
    tickers: list[str]
    count: int


class PricePoint(BaseModel):
    date: date
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: int | None = None
    daily_return: float | None = None


class PriceResponse(BaseModel):
    ticker: str
    data: list[PricePoint]
    count: int


class ForecastResponse(BaseModel):
    ticker: str
    feature_date: str | None = None
    prediction_date: str | None = None
    direction: str | None = None
    confidence: float | None = None
    features_used: dict[str, float] | None = None
    disclaimer: str = Field(
        default=(
            "This is not financial advice. ML predictions are "
            "probabilistic and may be wrong."
        )
    )


class ExplainResponse(BaseModel):
    ticker: str
    explanation: str | None = None
    context: dict[str, Any] | None = None
    model_used: str | None = None
    provider: str | None = None
    generated_at: str | None = None
    cached: bool = False
