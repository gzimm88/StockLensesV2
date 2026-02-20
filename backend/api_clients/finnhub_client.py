"""
Finnhub API client.

Mirrors gatedFetch + rate-limit logic from runFinnhubFundamentalsEtl.ts
(Extract2).

Rate limiting:
  REQUEST_INTERVAL_MS = 1100  (1.1 s between requests, enforced globally)
  maxRetries = 3
  On 429: exponential backoff = 2^attempt * 1000 + random(1000) ms
"""

import asyncio
import logging
import os
import random
import time

import httpx

logger = logging.getLogger(__name__)

FINNHUB_API_KEY: str = os.environ.get("FINNHUB_API_KEY", "")
_BASE_URL: str = "https://finnhub.io/api/v1"
_REQUEST_INTERVAL_MS: int = 1100
_MAX_RETRIES: int = 3

_last_request_time_ms: float = 0.0
_gate_lock = asyncio.Lock()


async def gated_fetch(url: str) -> dict:
    """
    Rate-limited fetch with retry on 429.
    Mirrors gatedFetch from Extract2:
      - enforces 1100 ms between requests (global)
      - on 429: backoff = 2^attempt * 1000 + random(1000) ms, up to maxRetries
      - on other errors: raise immediately after maxRetries
    """
    global _last_request_time_ms

    for attempt in range(1, _MAX_RETRIES + 1):
        async with _gate_lock:
            now_ms = time.time() * 1000
            lag = now_ms - _last_request_time_ms
            if lag < _REQUEST_INTERVAL_MS:
                wait_s = (_REQUEST_INTERVAL_MS - lag) / 1000
                await asyncio.sleep(wait_s)
            _last_request_time_ms = time.time() * 1000

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, timeout=30)

            remaining = resp.headers.get("x-ratelimit-remaining")
            reset_ts = resp.headers.get("x-ratelimit-reset")
            if remaining is not None:
                logger.debug("[Finnhub][RateLimit] remaining=%s reset=%s", remaining, reset_ts)

            if resp.status_code == 429:
                backoff_ms = (2 ** attempt) * 1000 + random.random() * 1000
                logger.warning(
                    "[Finnhub][429] backing off %.0fms (attempt %d/%d)",
                    backoff_ms, attempt, _MAX_RETRIES,
                )
                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(backoff_ms / 1000)
                    continue
                raise RuntimeError(f"Rate limited after {_MAX_RETRIES} attempts")

            if not resp.is_success:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.reason_phrase}")

            return resp.json()

        except RuntimeError:
            raise
        except Exception as exc:
            logger.warning("[Finnhub] attempt %d/%d failed: %s", attempt, _MAX_RETRIES, exc)
            if attempt == _MAX_RETRIES:
                raise RuntimeError(f"Finnhub fetch failed after {_MAX_RETRIES} attempts: {exc}") from exc

    raise RuntimeError("gated_fetch: exhausted all attempts")


def _build_url(path: str, **params: str) -> str:
    from urllib.parse import urlencode
    qs = urlencode(params)
    return f"{_BASE_URL}/{path}?{qs}"


async def fetch_quarterly_financials(ticker: str) -> dict:
    """GET /stock/financials-reported?symbol={ticker}&freq=quarterly"""
    if not FINNHUB_API_KEY:
        raise RuntimeError("FINNHUB_API_KEY environment variable is not set")
    url = _build_url(
        "stock/financials-reported",
        symbol=ticker,
        freq="quarterly",
        token=FINNHUB_API_KEY,
    )
    return await gated_fetch(url)


async def fetch_annual_financials(ticker: str) -> dict | None:
    """GET /stock/financials-reported?symbol={ticker}&freq=annual — returns None on failure."""
    if not FINNHUB_API_KEY:
        raise RuntimeError("FINNHUB_API_KEY environment variable is not set")
    url = _build_url(
        "stock/financials-reported",
        symbol=ticker,
        freq="annual",
        token=FINNHUB_API_KEY,
    )
    try:
        return await gated_fetch(url)
    except Exception as exc:
        logger.warning("[Finnhub] Annual data fetch failed: %s", exc)
        return None
