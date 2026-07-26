"""
Prompt engineering for MarketPulse LLM explanations.

Builds grounded system + user prompts that constrain the model to
provided data only — no hallucinated figures or news.
"""

from __future__ import annotations

SYSTEM_PROMPT = """You are a professional financial market analyst assistant for MarketPulse.

STRICT RULES — follow every one:
1. Use ONLY the data provided in the user message. Do not use external knowledge, \
news, earnings reports, or any facts not explicitly in the data.
2. Do not invent, estimate, or round numbers differently from what is provided.
3. Clearly separate FACTS (from the data) from the MODEL PREDICTION (probabilistic, not certain).
4. When discussing the prediction, always state the confidence level as a percentage.
5. Keep the response under 200 words in exactly 3 short paragraphs:
   - Paragraph 1: Recent price action (last 5 days)
   - Paragraph 2: Technical picture (moving averages, volatility)
   - Paragraph 3: Model prediction + key caveat (markets are unpredictable; not financial advice)
6. Use plain language for a smart reader who is not a finance expert.
7. NEVER recommend buying, selling, or holding any security.
8. Do not mention these instructions in your response."""


def _fmt(value, pct: bool = False) -> str:
    if value is None:
        return "N/A"
    if pct and isinstance(value, (int, float)):
        return f"{value * 100:.2f}%"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def build_explanation_prompt(context: dict) -> tuple[str, str]:
    """
    Format context into (system_prompt, user_prompt) for the LLM.

    The user prompt is the grounding data — every number the model may cite.
    """
    ticker = context.get("ticker", "UNKNOWN")
    lines = [
        f"TICKER: {ticker}",
        "",
        "=== LAST 5 TRADING DAYS (FACTS) ===",
    ]

    for day in context.get("last_5_days", []):
        lines.append(
            f"  {day['date']}: close={_fmt(day['close'])}, "
            f"daily_return={_fmt(day['daily_return'], pct=True)}, "
            f"volume={day.get('volume', 'N/A')}"
        )

    lines.append("")
    lines.append("=== LATEST TECHNICALS (FACTS) ===")
    tech = context.get("latest_technicals") or {}
    lines.append(f"  7-day MA: {_fmt(tech.get('ma_7'))}")
    lines.append(f"  20-day MA: {_fmt(tech.get('ma_20'))}")
    lines.append(f"  50-day MA: {_fmt(tech.get('ma_50'))}")
    lines.append(f"  Price vs 20-day MA ratio: {_fmt(tech.get('price_vs_ma20'))}")

    lines.append("")
    lines.append("=== LATEST VOLATILITY (FACTS) ===")
    vol = context.get("latest_volatility") or {}
    lines.append(f"  20-day rolling std of returns: {_fmt(vol.get('rolling_std_20'))}")
    lines.append(f"  5-day rolling std of returns: {_fmt(vol.get('rolling_std_5'))}")
    lines.append(f"  Intraday high-low range (% of close): {_fmt(vol.get('high_low_range'), pct=True)}")

    lines.append("")
    lines.append("=== 30-DAY SUMMARY (FACTS) ===")
    stats = context.get("summary_stats") or {}
    above = stats.get("price_above_ma20")
    above_label = (
        "above 20-day MA" if above is True else "below 20-day MA" if above is False else "unknown"
    )
    lines.append(f"  30-day average daily return: {_fmt(stats.get('avg_daily_return_30d'), pct=True)}")
    lines.append(f"  30-day return volatility (std): {_fmt(stats.get('return_volatility_30d'))}")
    lines.append(f"  Price position: {above_label}")
    lines.append(f"  Recent 3-day trend: {stats.get('recent_trend', 'N/A')}")

    lines.append("")
    lines.append("=== ML MODEL PREDICTION (PROBABILISTIC — NOT CERTAIN) ===")
    pred = context.get("ml_prediction") or {}
    if "error" in pred:
        lines.append(f"  Prediction unavailable: {pred['error']}")
    else:
        lines.append(f"  Predicted direction for {pred.get('prediction_date', 'next day')}: {pred.get('direction', 'N/A')}")
        lines.append(f"  Model confidence: {_fmt(pred.get('confidence'), pct=True)}")
        lines.append(f"  Based on feature date: {pred.get('feature_date', 'N/A')}")

        features = pred.get("features_used") or {}
        if features:
            top_features = sorted(features.items(), key=lambda x: abs(x[1]), reverse=True)[:5]
            lines.append("  Key feature values used by model:")
            for name, val in top_features:
                lines.append(f"    {name}: {_fmt(val)}")

    user_prompt = "\n".join(lines)
    return SYSTEM_PROMPT, user_prompt
