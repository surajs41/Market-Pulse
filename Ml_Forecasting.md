The ML forecasting layer is in place and verified. Here's what was built and how to run it.

What was built
File	Purpose
ml/feature_store.py
Joins dbt marts → unified feature matrix + targets
ml/train_model.py
Time-based split, XGBoost training, evaluation, artifact save
ml/predictor.py
Loads model and predicts UP/DOWN for all 12 tickers
ml/model/
Saved artifacts (gitignored): xgb_model.pkl, scaler.pkl, features.json
Features (12): close, volume, daily_return, prev_close, ma_7, ma_20, ma_50, price_vs_ma20, rolling_std_20, rolling_std_5, high_low_range, avg_volume_20

Target: target_direction — will tomorrow's return be positive?

Verified results
Feature store: 5,969 rows × 16 columns, 12 tickers, balanced classes (2,953 DOWN / 3,016 UP)

Training: 4,758 train / 1,211 test rows (time-based split), 48.3% accuracy — near random for next-day direction, which is normal for this problem without more features or tuning.

Commands to run (from Market-Pulse/)
1. Install dependencies

.\venv\Scripts\Activate.ps1
pip install xgboost scikit-learn joblib
2. Inspect features

python ml/feature_store.py
3. Train model

python ml/train_model.py
Expected output includes:

=== Model Evaluation ===
Accuracy: 0.4831
Classification Report:
              precision    recall  f1-score   support
        DOWN       0.51      0.35      0.42       635
          UP       0.47      0.63      0.54       576
Confusion Matrix:
[[223 412]
 [214 362]]
Top 10 Feature Importances:
  ma_7: 0.0883
  ...
4. Run predictions

python ml/predictor.py
Expected output:

Ticker         Predict For  Direction Confidence Status
------------------------------------------------------------
AAPL           2026-07-25   UP       52.00%     OK
NVDA           2026-07-25   DOWN     61.00%     OK
...
Data flow
dbt marts (analytics.*)  →  feature_store.py  →  train_model.py  →  ml/model/*.pkl
                                                          ↓
                                                   predictor.py  →  UP/DOWN + confidence
Prerequisites: Postgres running, dbt marts built (dbt run --project-dir dbt). No external APIs — all data comes from local Postgres.