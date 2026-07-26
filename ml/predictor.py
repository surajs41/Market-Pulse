"""
Inference for MarketPulse next-day direction forecasts.

Standalone usage (from project root, after training):

    python ml/predictor.py
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import joblib
import numpy as np
import pandas as pd
import yaml

from ml.feature_store import get_latest_features

logger = logging.getLogger(__name__)

MODEL_DIR = Path(__file__).resolve().parent / "model"
TICKERS_PATH = PROJECT_ROOT / "config" / "tickers.yaml"

MODEL_PATH = MODEL_DIR / "xgb_model.pkl"
SCALER_PATH = MODEL_DIR / "scaler.pkl"
FEATURES_PATH = MODEL_DIR / "features.json"


def _load_artifacts() -> tuple[object, object, list[str]]:
    """Load trained model, scaler, and feature column list."""
    if not MODEL_PATH.exists() or not SCALER_PATH.exists() or not FEATURES_PATH.exists():
        raise FileNotFoundError(
            "Model artifacts not found. Run `python ml/train_model.py` first."
        )
    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    feature_cols = json.loads(FEATURES_PATH.read_text(encoding="utf-8"))
    return model, scaler, feature_cols


def predict_next_day(ticker: str) -> dict:
    """
    Predict next-day direction for a single ticker.

    Returns a dict with ticker, prediction_date, direction, confidence,
    and features_used. On missing data, returns an error dict.
    """
    try:
        model, scaler, feature_cols = _load_artifacts()
    except FileNotFoundError as exc:
        return {"ticker": ticker, "error": str(exc)}

    df = get_latest_features(ticker)
    if df.empty:
        return {
            "ticker": ticker,
            "error": f"No feature data available for {ticker}. "
            "Ensure dbt marts are built and raw data is loaded.",
        }

    row = df.iloc[0]
    feature_date = pd.Timestamp(row["date"]).date()
    prediction_date = feature_date + timedelta(days=1)

    features = row[feature_cols].astype(float)
    X = scaler.transform(features.values.reshape(1, -1))

    proba = model.predict_proba(X)[0]
    predicted_class = int(model.predict(X)[0])
    confidence = float(proba[predicted_class])

    return {
        "ticker": ticker,
        "feature_date": str(feature_date),
        "prediction_date": str(prediction_date),
        "direction": "UP" if predicted_class == 1 else "DOWN",
        "confidence": round(confidence, 4),
        "features_used": {col: float(features[col]) for col in feature_cols},
    }


def load_tickers() -> list[str]:
    """Load ticker symbols from config/tickers.yaml."""
    with TICKERS_PATH.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("tickers", [])


def _print_predictions_table(results: list[dict]) -> None:
    """Print predictions in a readable table."""
    print(f"\n{'Ticker':<14} {'Predict For':<12} {'Direction':<8} {'Confidence':<10} Status")
    print("-" * 60)
    for result in results:
        if "error" in result:
            print(f"{result['ticker']:<14} {'—':<12} {'—':<8} {'—':<10} ERROR: {result['error']}")
            continue
        print(
            f"{result['ticker']:<14} "
            f"{result['prediction_date']:<12} "
            f"{result['direction']:<8} "
            f"{result['confidence']:<10.2%} "
            f"OK"
        )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    tickers = load_tickers()
    logger.info("Running predictions for %d tickers", len(tickers))

    results = [predict_next_day(ticker) for ticker in tickers]
    _print_predictions_table(results)


if __name__ == "__main__":
    main()
