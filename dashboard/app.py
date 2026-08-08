"""MarketPulse Streamlit dashboard.

This UI is intentionally API-only: price history, forecasts, and LLM analysis
are all requested from the FastAPI service rather than local databases or models.
"""

from __future__ import annotations

import os
from html import escape
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

# The local FastAPI default; set MARKETPULSE_API_URL to override this value.
API_BASE_URL = os.getenv("MARKETPULSE_API_URL", "http://localhost:8001")
REQUEST_TIMEOUT_SECONDS = 15
LOGO_PATH = Path(__file__).parent / "assets" / "marketpulse_logo.svg"


def api_get(path: str, timeout: int = REQUEST_TIMEOUT_SECONDS) -> tuple[dict | None, str | None]:
    """Get a JSON response from FastAPI and return a user-safe error on failure."""
    try:
        response = requests.get(f"{API_BASE_URL}{path}", timeout=timeout)
        if response.ok:
            return response.json(), None

        try:
            message = response.json().get("error")
        except ValueError:
            message = None
        return None, message or f"API request failed (HTTP {response.status_code})."
    except requests.RequestException as exc:
        return None, str(exc)


def api_is_online() -> bool:
    """Check FastAPI availability without surfacing request details in the UI."""
    response, _ = api_get("/health", timeout=3)
    return bool(response and response.get("status") == "ok")


@st.cache_data(ttl=300, show_spinner=False)
def fetch_price_data(ticker: str) -> tuple[dict | None, str | None]:
    """Load price history; it changes slowly enough for a five-minute cache."""
    return api_get(f"/price/{ticker}")


def fetch_tickers() -> tuple[list[str], str | None]:
    payload, error = api_get("/tickers", timeout=5)
    if error or not payload:
        return [], error or "No ticker data returned."
    return payload.get("tickers", []), None


def price_frame(payload: dict) -> pd.DataFrame:
    """Normalize API price rows and calculate the requested moving averages."""
    frame = pd.DataFrame(payload.get("data", []))
    if frame.empty:
        return frame

    frame["date"] = pd.to_datetime(frame["date"])
    for column in ("open", "high", "low", "close", "volume", "daily_return"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    frame = frame.sort_values("date").reset_index(drop=True)
    frame["daily_return"] = frame["daily_return"].fillna(frame["close"].pct_change())
    for period in (7, 20, 50):
        frame[f"ma{period}"] = frame["close"].rolling(period, min_periods=1).mean()
    return frame


def price_chart(ticker: str, frame: pd.DataFrame) -> go.Figure:
    """Create the interactive 30-day OHLC chart and moving-average overlays."""
    chart = go.Figure(
        go.Candlestick(
            x=frame["date"],
            open=frame["open"],
            high=frame["high"],
            low=frame["low"],
            close=frame["close"],
            name="OHLC",
        )
    )
    for column, label, color in (
        ("ma7", "MA7", "#2563EB"),
        ("ma20", "MA20", "#F59E0B"),
        ("ma50", "MA50", "#DC2626"),
    ):
        chart.add_trace(
            go.Scatter(
                x=frame["date"], y=frame[column], mode="lines", name=label,
                line={"color": color, "width": 2},
            )
        )
    chart.update_layout(
        title=f"{ticker} — Last 30 Days",
        height=500,
        margin={"l": 20, "r": 20, "t": 55, "b": 20},
        xaxis_rangeslider_visible=False,
        legend={"orientation": "h", "y": 1.02, "x": 1, "xanchor": "right"},
    )
    return chart


def render_sidebar(api_online: bool) -> tuple[str | None, bool]:
    """Render controls. Ticker listing is the only pre-analysis API data request."""
    with st.sidebar:
        st.image(str(LOGO_PATH), width=280)
        st.markdown("<p class='sidebar-tagline'>AI-powered financial market insights</p>", unsafe_allow_html=True)

        if api_online:
            st.markdown("<span class='api-online'>● API Online ✅</span>", unsafe_allow_html=True)
            tickers, ticker_error = fetch_tickers()
            if ticker_error:
                st.warning(f"Could not load tickers: {ticker_error}")
                tickers = []
        else:
            st.markdown("<span class='api-offline'>● API Offline ❌</span>", unsafe_allow_html=True)
            tickers = []

        selected = st.selectbox("Ticker", tickers, disabled=not tickers)
        analyze = st.button("Analyze", type="primary", use_container_width=True, disabled=not tickers)

        st.markdown(
            """<div class='sidebar-about'>
            <div class='sidebar-about-title'>About</div>
            <div>Built with Python, Airflow, dbt, XGBoost &amp; Llama 3.1</div>
            </div>""",
            unsafe_allow_html=True,
        )
    return selected, analyze


def render_forecast(ticker: str) -> None:
    """Render a fresh forecast; forecast API results are never Streamlit-cached."""
    left, right = st.columns((1, 1), gap="large")
    with st.spinner("Loading ML forecast..."):
        forecast, error = api_get(f"/forecast/{ticker}")

    with left:
        st.subheader("ML Forecast")
        if error or not forecast:
            st.warning(f"Forecast unavailable: {error or 'Unknown error'}")
        else:
            direction = str(forecast.get("direction") or "UNKNOWN").upper()
            confidence = max(0.0, min(float(forecast.get("confidence") or 0), 1.0))
            badge_class = "up-badge" if direction == "UP" else "down-badge"
            arrow = "⬆️" if direction == "UP" else "⬇️"
            st.markdown(
                f"<div class='{badge_class}'>{arrow} {direction}</div>",
                unsafe_allow_html=True,
            )
            st.progress(confidence)
            st.caption(f"Confidence: {confidence:.0%}")
            st.caption(f"Prediction for: {forecast.get('prediction_date') or 'next trading day'}")

    with right:
        st.subheader("Model Input Features")
        features = (forecast or {}).get("features_used") or {}
        if features:
            feature_table = (
                pd.DataFrame(features.items(), columns=["Feature", "Value"])
                .head(5)
            )
            st.dataframe(feature_table, hide_index=True, use_container_width=True)
        else:
            st.caption("Feature values are unavailable.")


def render_explanation(ticker: str) -> None:
    """Render an API-generated explanation, including its API cache status."""
    st.subheader("🧠 AI Analysis")
    with st.spinner("Generating AI analysis..."):
        explanation, error = api_get(f"/explain/{ticker}", timeout=60)
    if error or not explanation or not explanation.get("explanation"):
        st.warning("AI explanation unavailable — try again shortly")
        return

    st.markdown(
        f"<div class='ai-explanation'>{escape(str(explanation['explanation']))}</div>",
        unsafe_allow_html=True,
    )
    cache_label = "⚡ Cached" if explanation.get("cached") else "🔄 Fresh"
    st.caption(f"{cache_label}  |  Generated by Llama 3.1 via Groq | Grounded in real market data only")


def render_recent_data(frame: pd.DataFrame) -> None:
    """Show the requested ten most recent rows with colorized daily returns."""
    st.subheader("📋 Recent Price History")
    table = frame.tail(10).copy()
    table["daily_return"] *= 100
    table = table[["date", "open", "high", "low", "close", "volume", "daily_return"]].rename(
        columns={
            "date": "Date", "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume", "daily_return": "Daily Return %",
        }
    )
    table["Date"] = table["Date"].dt.strftime("%Y-%m-%d")

    def return_color(value: float) -> str:
        if value > 0:
            return "color: #15803d; font-weight: 600"
        if value < 0:
            return "color: #dc2626; font-weight: 600"
        return ""

    styled = table.style.format(
        {"Open": "{:.2f}", "High": "{:.2f}", "Low": "{:.2f}", "Close": "{:.2f}",
         "Volume": "{:,.0f}", "Daily Return %": "{:+.2f}%"},
        na_rep="—",
    ).map(return_color, subset=["Daily Return %"])
    st.dataframe(styled, hide_index=True, use_container_width=True)


def main() -> None:
    st.set_page_config(
        page_title="MarketPulse",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(
        """<style>
        .stApp, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
          background: #0b1220; font-family: "Inter", "Segoe UI", sans-serif;
        }
        section[data-testid="stSidebar"] > div { background: #111c2f; }
        [data-testid="stHeader"] { border-bottom: 1px solid #1e3a5f; }
        section[data-testid="stSidebar"] { border-right: 1px solid #1e3a5f; }
        section[data-testid="stSidebar"] [data-testid="stImage"] { display: flex; justify-content: center; }
        [data-testid="stMain"] [data-testid="stImage"] { display: flex; justify-content: center; }
        .sidebar-tagline { color: #ffffff; font-size: 0.95rem; font-weight: 500; margin: .5rem 0 1.75rem; }
        .sidebar-about { position: fixed; bottom: 1.5rem; left: 1.5rem; width: 19.5rem;
          border-top: 1px solid #2b4b73; color: #cbd5e1; font-size: .9rem; line-height: 1.7;
          padding-top: 1.25rem; }
        .sidebar-about-title { font-size: 1.05rem; font-weight: 700; margin-bottom: .75rem; }
        .up-badge, .down-badge { display: inline-block; color: white; border-radius: 8px;
          padding: 0.55rem 1rem; font-size: 1.35rem; font-weight: 700; margin-bottom: 1rem; }
        .up-badge { background: #15803d; } .down-badge { background: #dc2626; }
        .ai-explanation { background: #f1f5f9; border-left: 4px solid #3b82f6; border-radius: 4px;
          color: #1e293b; line-height: 1.65; padding: 1rem 1.25rem; }
        .api-online { color: #15803d; font-size: 0.9rem; font-weight: 600; }
        .api-offline { color: #dc2626; font-size: 0.9rem; font-weight: 600; }
        [data-testid="stAlert"] { background: #163657; border: 1px solid #285b89; color: #e2e8f0; }
        </style>""",
        unsafe_allow_html=True,
    )

    online = api_is_online()
    ticker, analyze = render_sidebar(online)
    if not online:
        st.error("❌ MarketPulse API is offline. Please start the API first: uvicorn api.main:app --port 8001 --reload")
        st.stop()

    if analyze:
        st.session_state["active_ticker"] = ticker
    active_ticker = st.session_state.get("active_ticker")
    if not active_ticker:
        _, welcome_column, _ = st.columns((1, 1.4, 1))
        with welcome_column:
            st.image(str(LOGO_PATH), width=900)
            st.markdown("""
            <div style="text-align:center;padding:20px 0">
              <p style="
                color:#F9FAFB;
                font-size:16px;
                font-weight:700;
                margin:0;
              ">← Select a ticker in the sidebar and click Analyze to load market insights.</p>
            </div>
            """, unsafe_allow_html=True)
        return

    with st.spinner("Loading price history..."):
        price_payload, price_error = fetch_price_data(active_ticker)
    if price_error or not price_payload:
        st.error(f"Price data unavailable: {price_error or 'Unknown error'}")
        return

    frame = price_frame(price_payload)
    if frame.empty:
        st.warning(f"No price data is available for {active_ticker}.")
        return

    latest = frame.iloc[-1]
    st.title(active_ticker)
    price_col, return_col, _ = st.columns((1, 1, 2))
    price_col.metric("Current Price", f"{latest['close']:,.2f}")
    return_col.metric("Daily Return", f"{latest['daily_return']:+.2%}", delta=f"{latest['daily_return']:+.2%}")

    st.divider()
    st.subheader("Price Chart")
    st.plotly_chart(price_chart(active_ticker, frame), use_container_width=True)

    st.divider()
    render_forecast(active_ticker)

    st.divider()
    render_explanation(active_ticker)

    st.divider()
    render_recent_data(frame)


if __name__ == "__main__":
    main()
