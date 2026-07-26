"""
LLM-powered plain-English market explanations for MarketPulse.

Grounds every explanation in Postgres mart data + ML predictions.
Supports Groq (default) or local Ollama via LLM_PROVIDER env var.

Standalone usage (from project root, with GROQ_API_KEY in .env):

    python llm/explainer.py
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from llm.context_builder import build_market_context
from llm.prompt_builder import build_explanation_prompt

logger = logging.getLogger(__name__)

GROQ_MODEL = "llama-3.1-8b-instant"
OLLAMA_MODEL = "llama3.1"


def _call_groq(system_prompt: str, user_prompt: str) -> str:
    """Call Groq chat completions API."""
    from groq import Groq

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise ValueError("GROQ_API_KEY is not set in .env")

    client = Groq(api_key=api_key)
    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_tokens=400,
        temperature=0.3,
    )
    return response.choices[0].message.content.strip()


def _call_ollama(system_prompt: str, user_prompt: str) -> str:
    """Call local Ollama generate API."""
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
    prompt = f"{system_prompt}\n\n{user_prompt}"

    response = requests.post(
        f"{base_url}/api/generate",
        json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.3, "num_predict": 400},
        },
        timeout=120,
    )
    response.raise_for_status()
    return response.json()["response"].strip()


def explain_ticker(ticker: str) -> dict:
    """
    Generate a plain-English explanation for a ticker's recent action
    and ML prediction.

    Returns dict with ticker, explanation, context, model_used, generated_at.
    On failure, explanation is null and error is set.
    """
    load_dotenv(PROJECT_ROOT / ".env")
    provider = os.getenv("LLM_PROVIDER", "groq").lower()
    generated_at = datetime.now(timezone.utc).isoformat()

    logger.info("Building context for %s", ticker)
    context = build_market_context(ticker)
    system_prompt, user_prompt = build_explanation_prompt(context)

    model_used = GROQ_MODEL if provider == "groq" else OLLAMA_MODEL

    try:
        logger.info("Calling LLM provider: %s", provider)
        if provider == "groq":
            explanation = _call_groq(system_prompt, user_prompt)
        elif provider == "ollama":
            explanation = _call_ollama(system_prompt, user_prompt)
        else:
            raise ValueError(f"Unknown LLM_PROVIDER: {provider}. Use 'groq' or 'ollama'.")

        return {
            "ticker": ticker,
            "explanation": explanation,
            "context": context,
            "model_used": model_used,
            "provider": provider,
            "generated_at": generated_at,
        }
    except Exception as exc:
        logger.exception("LLM call failed for %s", ticker)
        return {
            "ticker": ticker,
            "explanation": None,
            "error": str(exc),
            "context": context,
            "model_used": model_used,
            "provider": provider,
            "generated_at": generated_at,
        }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    ticker = "NVDA"
    result = explain_ticker(ticker)

    print(f"\n{'=' * 60}")
    print(f"  MarketPulse Explanation — {result['ticker']}")
    print(f"  Model: {result.get('model_used')} ({result.get('provider')})")
    print(f"  Generated: {result.get('generated_at')}")
    print(f"{'=' * 60}\n")

    if result.get("error"):
        print(f"ERROR: {result['error']}\n")
        return

    print(result["explanation"])
    print(f"\n{'=' * 60}")

    pred = result.get("context", {}).get("ml_prediction", {})
    if "direction" in pred:
        print(
            f"ML Prediction: {pred['direction']} "
            f"(confidence: {pred.get('confidence', 0):.1%})"
        )


if __name__ == "__main__":
    main()
