"""
Train the MarketPulse next-day direction classifier (XGBoost).

Standalone usage (from project root, with dbt marts populated):

    python ml/train_model.py
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import joblib
import pandas as pd
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from ml.feature_store import FEATURE_COLUMNS, build_feature_table

logger = logging.getLogger(__name__)

MODEL_DIR = Path(__file__).resolve().parent / "model"

MODEL_PATH = MODEL_DIR / "xgb_model.pkl"
SCALER_PATH = MODEL_DIR / "scaler.pkl"
FEATURES_PATH = MODEL_DIR / "features.json"

TEST_FRACTION = 0.2


def time_based_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split by calendar date — last 20% of dates go to test.

    We use a time-based split instead of random shuffling because financial
    data is ordered: random splits leak future returns into training and
    inflate accuracy with patterns that don't exist at prediction time.
    """
    dates = sorted(df["date"].unique())
    split_idx = int(len(dates) * (1 - TEST_FRACTION))
    train_dates = set(dates[:split_idx])
    test_dates = set(dates[split_idx:])

    train_df = df[df["date"].isin(train_dates)].copy()
    test_df = df[df["date"].isin(test_dates)].copy()

    logger.info(
        "Time-based split: %d train rows (%d dates), %d test rows (%d dates)",
        len(train_df),
        len(train_dates),
        len(test_df),
        len(test_dates),
    )
    return train_df, test_df


def train_and_evaluate() -> None:
    """Build features, train XGBoost, evaluate, and save artifacts."""
    logger.info("Starting model training pipeline")

    df = build_feature_table()
    feature_cols = FEATURE_COLUMNS

    train_df, test_df = time_based_split(df)

    X_train = train_df[feature_cols]
    y_train = train_df["target_direction"].astype(int)
    X_test = test_df[feature_cols]
    y_test = test_df["target_direction"].astype(int)

    logger.info("Scaling features with StandardScaler")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    logger.info("Training XGBClassifier")
    model = XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss",
        random_state=42,
    )
    model.fit(X_train_scaled, y_train)

    logger.info("Evaluating on held-out test set")
    y_pred = model.predict(X_test_scaled)
    accuracy = accuracy_score(y_test, y_pred)

    print("\n=== Model Evaluation ===")
    print(f"Accuracy: {accuracy:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=["DOWN", "UP"]))
    print("Confusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    importances = pd.Series(model.feature_importances_, index=feature_cols)
    importances = importances.sort_values(ascending=False)
    print("\nTop 10 Feature Importances:")
    for feature, score in importances.head(10).items():
        print(f"  {feature}: {score:.4f}")

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)
    FEATURES_PATH.write_text(json.dumps(feature_cols, indent=2), encoding="utf-8")

    logger.info("Saved model to %s", MODEL_PATH)
    logger.info("Saved scaler to %s", SCALER_PATH)
    logger.info("Saved feature list to %s", FEATURES_PATH)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    train_and_evaluate()


if __name__ == "__main__":
    main()
