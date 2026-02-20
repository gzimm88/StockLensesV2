"""
Yahoo Finance API client.

Mirrors the session management, token bucket rate limiting and signed fetch
logic found in syncHistoricalPricesYahoo.ts / runYahooEtlPipeline.ts
(Extract5 / Extract2).

Session bootstrap:
  1. GET https://fc.yahoo.com/              → set-cookie header
  2. GET /v1/test/getcrumb (query1 then query2) → crumb text
  TTL = 60 minutes.  Single-flight protection via asyncio.Lock.

Token bucket:
  maxTokens=2, refillRate=1 token per 600 ms.

signedFetch:
  3 attempts, base delay 700 ms * 2^(attempt-1) ± 20 % jitter.
  On 401/403: invalidate session and re-acquire before retry.
  On 429/999/5xx: backoff and retry.
  On HTML response: treat as transient and retry.
"""

import asyncio
import logging
import math
import random
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants (mirrored from extracts)
# ---------------------------------------------------------------------------
SESSION_TTL_MS: int = 60 * 60 * 1000          # 60 minutes
YAHOO_USER_AGENT: str = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
)

_TOKEN_MAX: int = 2
_TOKEN_REFILL_RATE: float = 1 / 600           # 1 token per 600 ms
_CRUMB_MAX_ATTEMPTS: int = 6
_CRUMB_BASE_DELAY_MS: int = 1000             # base 1000 ms * 1.5^(attempt-1)
_FETCH_MAX_ATTEMPTS: int = 3
_FETCH_BASE_DELAY_MS: int = 700              # 700 ms, 1400 ms, 2800 ms


# ---------------------------------------------------------------------------
# Session state (module-level singleton, matches JS module-scope pattern)
# ---------------------------------------------------------------------------
@dataclass
class _YahooSession:
    cookie: str | None = None
    crumb: str | None = None
    acquired_at_ms: float = 0.0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def is_fresh(self) -> bool:
        return (
            self.cookie is not None
            and self.crumb is not None
            and (time.time() * 1000 - self.acquired_at_ms) < SESSION_TTL_MS
        )

    def invalidate(self) -> None:
        self.cookie = None
        self.crumb = None
        self.acquired_at_ms = 0.0


_session = _YahooSession()


# ---------------------------------------------------------------------------
# Token bucket (module-level, mirrors JS tokenBucket)
# ---------------------------------------------------------------------------
@dataclass
class _TokenBucket:
    tokens: float = float(_TOKEN_MAX)
    last_refill_ms: float = field(default_factory=lambda: time.time() * 1000)
    max_tokens: int = _TOKEN_MAX
    refill_rate: float = _TOKEN_REFILL_RATE   # tokens per ms


_bucket = _TokenBucket()


def _refill_tokens() -> None:
    now_ms = time.time() * 1000
    elapsed = now_ms - _bucket.last_refill_ms
    tokens_to_add = math.floor(elapsed * _bucket.refill_rate)
    if tokens_to_add > 0:
        _bucket.tokens = min(_bucket.max_tokens, _bucket.tokens + tokens_to_add)
        _bucket.last_refill_ms = now_ms


async def _wait_for_token() -> None:
    while _bucket.tokens < 1:
        _refill_tokens()
        if _bucket.tokens < 1:
            await asyncio.sleep(0.1)
    _bucket.tokens -= 1
    _refill_tokens()


# ---------------------------------------------------------------------------
# Jitter helper (±20 %)
# ---------------------------------------------------------------------------
def _add_jitter(delay_ms: float) -> float:
    jitter = 0.2 * delay_ms * (random.random() - 0.5) * 2
    return max(0.0, delay_ms + jitter)


# ---------------------------------------------------------------------------
# Session bootstrap (fetchCrumbWithBackoff + ensureSession)
# ---------------------------------------------------------------------------
async def _fetch_crumb_with_backoff(client: httpx.AsyncClient) -> None:
    """
    Attempts up to _CRUMB_MAX_ATTEMPTS times to obtain a valid Yahoo cookie+crumb.
    Mirrors fetchCrumbWithBackoff from Extract5.
    Delay schedule: base * 1.5^(attempt-1) ± 20 % jitter, max 6 s.
    """
    for attempt in range(1, _CRUMB_MAX_ATTEMPTS + 1):
        try:
            logger.debug("[Yahoo][Session] crumb attempt %d/%d", attempt, _CRUMB_MAX_ATTEMPTS)

            # Step A: cookie from fc.yahoo.com
            await _wait_for_token()
            cookie_resp = await client.get(
                "https://fc.yahoo.com/",
                headers={"User-Agent": YAHOO_USER_AGENT},
                follow_redirects=True,
                timeout=15,
            )
            if cookie_resp.status_code in (429, 999) or cookie_resp.status_code >= 500:
                raise RuntimeError(f"Cookie fetch failed: status={cookie_resp.status_code}")

            cookie_header = cookie_resp.headers.get("set-cookie")
            if not cookie_header:
                raise RuntimeError("No session cookie received from fc.yahoo.com")

            # Step B: crumb (try query1 then query2)
            await _wait_for_token()
            crumb: str | None = None
            for host in ("query1", "query2"):
                try:
                    crumb_resp = await client.get(
                        f"https://{host}.finance.yahoo.com/v1/test/getcrumb",
                        headers={"cookie": cookie_header, "User-Agent": YAHOO_USER_AGENT},
                        timeout=15,
                    )
                    if crumb_resp.status_code in (429, 999) or crumb_resp.status_code >= 500:
                        continue
                    if not crumb_resp.is_success:
                        continue
                    body = crumb_resp.text.strip()
                    if body and len(body) >= 5:
                        crumb = body
                        break
                except Exception as e:
                    logger.debug("[Yahoo][Session] crumb host=%s error: %s", host, e)

            if not crumb:
                raise RuntimeError("Invalid or empty crumb received")

            _session.cookie = cookie_header
            _session.crumb = crumb
            _session.acquired_at_ms = time.time() * 1000
            logger.info("[Yahoo][Session] cookie+crumb acquired OK")
            return

        except Exception as exc:
            if attempt == _CRUMB_MAX_ATTEMPTS:
                raise RuntimeError(
                    f"Yahoo session bootstrap failed after {_CRUMB_MAX_ATTEMPTS} attempts: {exc}"
                ) from exc
            delay_ms = min(6000.0, _CRUMB_BASE_DELAY_MS * (1.5 ** (attempt - 1)))
            delay_s = _add_jitter(delay_ms) / 1000
            logger.warning("[Yahoo][Session] retry %d/%d, sleeping %.1fs: %s",
                           attempt, _CRUMB_MAX_ATTEMPTS, delay_s, exc)
            await asyncio.sleep(delay_s)


async def ensure_session(client: httpx.AsyncClient) -> None:
    """
    Ensures a fresh Yahoo session exists.  Single-flight via asyncio.Lock.
    Mirrors ensureSession from Extract5.
    """
    if _session.is_fresh():
        return
    async with _session._lock:
        # Re-check after acquiring the lock
        if _session.is_fresh():
            return
        await _fetch_crumb_with_backoff(client)


# ---------------------------------------------------------------------------
# signed_fetch (mirrors signedFetch from Extract5)
# ---------------------------------------------------------------------------
async def signed_fetch(
    path: str,
    params: dict[str, str],
    client: httpx.AsyncClient,
) -> dict[str, Any]:
    """
    Performs an authenticated GET to query1.finance.yahoo.com.
    Appends crumb to params. 3 attempts with exponential backoff.

    Returns parsed JSON dict on success.
    Raises RuntimeError with structured info on final failure.
    """
    await ensure_session(client)

    last_error: str = "unknown"

    for attempt in range(1, _FETCH_MAX_ATTEMPTS + 1):
        try:
            final_params = {**params, "crumb": _session.crumb}
            url = f"https://query1.finance.yahoo.com{path}"

            await _wait_for_token()
            response = await client.get(
                url,
                params=final_params,
                headers={"cookie": _session.cookie, "User-Agent": YAHOO_USER_AGENT},
                timeout=30,
            )

            logger.debug("[Yahoo][Fetch] %s status=%d len=%d",
                         path, response.status_code, len(response.content))

            # 401/403: invalidate session and retry
            if response.status_code in (401, 403):
                if attempt < _FETCH_MAX_ATTEMPTS:
                    logger.warning("[Yahoo][Fetch] session invalid (status=%d), refreshing",
                                   response.status_code)
                    _session.invalidate()
                    await ensure_session(client)
                    continue
                raise RuntimeError(
                    f"Authentication failed after session refresh: status={response.status_code}"
                )

            # 429/999/5xx: backoff and retry
            if response.status_code in (429, 999) or response.status_code >= 500:
                if attempt < _FETCH_MAX_ATTEMPTS:
                    delay_ms = _FETCH_BASE_DELAY_MS * (2 ** (attempt - 1))
                    delay_s = _add_jitter(delay_ms) / 1000
                    logger.warning("[Yahoo][Fetch] rate/server error status=%d, sleeping %.1fs",
                                   response.status_code, delay_s)
                    await asyncio.sleep(delay_s)
                    continue
                raise RuntimeError(
                    f"Rate limited / server error after retries: status={response.status_code}"
                )

            # Client errors (non-auth): don't retry
            if response.status_code >= 400:
                raise RuntimeError(
                    f"Client error from Yahoo API: status={response.status_code}"
                )

            body = response.text

            # HTML response: treat as transient, retry
            if body.lstrip().startswith("<"):
                last_error = "Received HTML instead of JSON"
                if attempt < _FETCH_MAX_ATTEMPTS:
                    logger.warning("[Yahoo][Fetch] got HTML, retrying")
                    continue
                raise RuntimeError(last_error)

            return response.json()

        except RuntimeError:
            raise
        except Exception as exc:
            last_error = str(exc)
            if attempt < _FETCH_MAX_ATTEMPTS:
                delay_s = _add_jitter(1000 * attempt) / 1000
                logger.warning("[Yahoo][Fetch] network error attempt=%d, sleeping %.1fs: %s",
                               attempt, delay_s, exc)
                await asyncio.sleep(delay_s)
                continue
            raise RuntimeError(f"Network error after retries: {last_error}") from exc

    raise RuntimeError(f"signed_fetch exhausted all attempts: {last_error}")


# ---------------------------------------------------------------------------
# build_chart_request (mirrors buildChartRequest, shared helper)
# ---------------------------------------------------------------------------
def build_chart_params(
    ticker: str,
    *,
    range_: str | None = None,
    period1: int | None = None,
    period2: int | None = None,
    interval: str = "1d",
    events: str = "div%2Csplits",
) -> tuple[str, dict[str, str]]:
    """
    Returns (url_path, params_dict) for the Yahoo chart endpoint.
    Either range_ OR period1+period2 must be provided.
    """
    if not ticker:
        raise ValueError("ticker is required")
    path = f"/v8/finance/chart/{ticker}"
    params: dict[str, str] = {
        "interval": interval,
        "events": events,
        "includeAdjustedClose": "true",
    }
    if period1 is not None and period2 is not None:
        params["period1"] = str(period1)
        params["period2"] = str(period2)
    elif range_ is not None:
        params["range"] = range_
    else:
        raise ValueError("Either range_ or period1+period2 must be specified")
    return path, params


# ---------------------------------------------------------------------------
# fetch_5y_prices (convenience)
# ---------------------------------------------------------------------------
async def fetch_5y_prices(ticker: str, client: httpx.AsyncClient) -> dict[str, Any]:
    """Fetch 5-year daily price history from Yahoo chart API."""
    path, params = build_chart_params(ticker, range_="5y")
    data = await signed_fetch(path, params, client)
    result = data.get("chart", {}).get("result", [None])[0]
    if not result:
        raise RuntimeError(f"No chart data returned for {ticker}")
    return result


async def fetch_recent_prices(ticker: str, client: httpx.AsyncClient) -> dict[str, Any]:
    """Fetch 1-month recent daily price history from Yahoo chart API."""
    path, params = build_chart_params(ticker, range_="1mo")
    data = await signed_fetch(path, params, client)
    result = data.get("chart", {}).get("result", [None])[0]
    if not result:
        raise RuntimeError(f"No chart data returned for {ticker}")
    return result


# ---------------------------------------------------------------------------
# fetch_quote_summary (Yahoo Fundamentals — mirrors runYahooFundamentalsEtl)
# ---------------------------------------------------------------------------
_QUOTE_SUMMARY_MODULES = ",".join([
    "incomeStatementHistoryQuarterly",
    "cashflowStatementHistoryQuarterly",
    "balanceSheetHistoryQuarterly",
    "incomeStatementHistory",
    "cashflowStatementHistory",
    "balanceSheetHistory",
    "financialData",
    "defaultKeyStatistics",
    "price",
    "summaryDetail",
])


async def fetch_quote_summary(ticker: str, client: httpx.AsyncClient) -> dict[str, Any]:
    """
    Fetch quoteSummary from Yahoo Finance (query2).
    Uses same crumb session.  Returns result[0] dict or {} on empty payload.
    """
    await ensure_session(client)

    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
    params = {
        "modules": _QUOTE_SUMMARY_MODULES,
        "crumb": _session.crumb,
        "formatted": "false",
        "lang": "en-US",
        "region": "US",
        "corsDomain": "finance.yahoo.com",
    }
    headers = {
        "Cookie": _session.cookie or "",
        "User-Agent": YAHOO_USER_AGENT,
    }

    response = await client.get(url, params=params, headers=headers, timeout=30)
    if not response.is_success:
        raise RuntimeError(f"quoteSummary returned {response.status_code}")
    data = response.json()
    result = data.get("quoteSummary", {}).get("result", [None])[0]
    if not result:
        logger.warning("[Yahoo][QuoteSummary] Empty/invalid payload for %s", ticker)
        return {}
    return result
