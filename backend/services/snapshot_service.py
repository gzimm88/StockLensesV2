"""
Score Snapshot Service — Phases 3 & 5

Computes deterministic ScoreSnapshots for a (ticker, lens) pair and persists them.

Invariant: same metrics + same lens + same SCORE_VERSION → identical snapshot_hash.

Scoring policy (Phase 4):
  - Null categories are EXCLUDED from the weighted average (not penalised as 0).
  - Category score = null when no sub-metrics are computable.

Recommendation policy (Phase 2.1):
  - BUY   if final_score >= buy_threshold
  - WATCH if watch_threshold <= final_score < buy_threshold
  - AVOID otherwise
  - MOS and confidence do NOT gate recommendation.

MOS policy (Phase 2.2):
  - mos_signal: "+" / "0" / "-" based on neutral_band (default 5 %).
  - Display-only — changing MOS alone never changes recommendation.

Explainability (Phase 5):
  - top_positive_contributors / top_negative_contributors (by category)
  - missing_critical_fields
  - resolution_warnings
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

SCORE_VERSION = "1.0.0"
MOS_NEUTRAL_BAND = 5.0   # configurable; ±5% = neutral signal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and math.isfinite(v)


def _clamp(x: float, lo: float = 0.0, hi: float = 10.0) -> float:
    return max(lo, min(hi, x))


def _safe_avg(vals: list) -> float | None:
    nums = [v for v in vals if _is_num(v)]
    return sum(nums) / len(nums) if nums else None


def _camel_to_snake(s: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


# ---------------------------------------------------------------------------
# Category scorers — mirror frontend scoring.jsx exactly
# Null inputs propagate as null (never coerced to 0).
# ---------------------------------------------------------------------------

def _score_valuation(m: dict) -> float | None:
    pe     = m.get("pe_fwd")
    ev     = m.get("ev_ebitda")
    fcf_y  = m.get("fcf_yield_pct")
    peg    = m.get("peg_5y")
    pe_ttm = m.get("pe_ttm")
    pe_low = m.get("pe_5y_low")
    pe_hi  = m.get("pe_5y_high")

    sub_pe   = None
    if _is_num(pe):
        sub_pe = 10 if pe <= 10 else 9 if pe <= 15 else 7 if pe <= 20 else 5 if pe <= 25 else 3 if pe <= 35 else 1

    sub_peg  = None
    if _is_num(peg):
        sub_peg = 9 if peg <= 1 else 7 if peg <= 1.5 else 5 if peg <= 2 else 3 if peg <= 3 else 1

    sub_ev   = None
    if _is_num(ev):
        sub_ev = 10 if ev <= 7 else 8 if ev <= 10 else 6 if ev <= 14 else 4 if ev <= 20 else 2

    sub_fcfy = None
    if _is_num(fcf_y):
        sub_fcfy = 10 if fcf_y >= 8 else 8 if fcf_y >= 5 else 6 if fcf_y >= 3 else 3 if fcf_y >= 1 else 1

    sub_hist = None
    if _is_num(pe_ttm) and _is_num(pe_low) and _is_num(pe_hi) and pe_hi > pe_low:
        sub_hist = 10 * max(0.0, min(1.0, (pe_hi - pe_ttm) / (pe_hi - pe_low)))

    weights = [0.35, 0.15, 0.20, 0.15, 0.15]
    subs    = [sub_pe, sub_peg, sub_ev, sub_fcfy, sub_hist]
    num, den = 0.0, 0.0
    for v, w in zip(subs, weights):
        if _is_num(v):
            num += v * w; den += w
    return num / den if den > 0 else None


def _score_quality(m: dict) -> float | None:
    roic       = m.get("roic_pct")
    fcf_margin = m.get("fcf_margin_pct")
    cfo_to_ni  = m.get("cfo_to_ni")
    fcf_ebit   = m.get("fcf_to_ebit")
    accruals   = m.get("accruals_ratio")
    ms         = m.get("margin_stdev_5y_pct")

    sub_roic  = _clamp(roic / 2)        if _is_num(roic)       else None
    sub_fcfm  = _clamp(fcf_margin / 1.5) if _is_num(fcf_margin) else None

    cc_vals   = [v for v in [cfo_to_ni, fcf_ebit] if _is_num(v)]
    sub_cc    = _clamp(10 * sum(cc_vals) / len(cc_vals)) if cc_vals else None

    sub_acc   = None
    if accruals is None:
        sub_acc = 10.0    # missing = assume clean (conservative default)
    elif _is_num(accruals):
        sub_acc = _clamp(10 * max(0.0, min(1.0, (0.10 - abs(accruals)) / 0.10)))

    sub_ms    = _clamp(10 - _clamp(ms * 0.5)) if _is_num(ms) else None

    return _safe_avg([sub_roic, sub_fcfm, sub_cc, sub_acc, sub_ms])


def _score_capital_allocation(m: dict) -> float | None:
    buyback = m.get("buyback_yield_pct")
    int_cov = m.get("interest_coverage_x")

    sub_bb = _clamp((buyback + 2) / 2)                         if _is_num(buyback) else None
    sub_ic = _clamp(math.log10(int_cov + 1) * 4)               if (_is_num(int_cov) and int_cov > -1) else None

    return _safe_avg([sub_bb, sub_ic])


def _score_growth(m: dict) -> float | None:
    eps5y = m.get("eps_cagr_5y_pct")
    rev5y = m.get("revenue_cagr_5y_pct")
    eps3y = m.get("eps_cagr_3y_pct")
    rev3y = m.get("revenue_cagr_3y_pct")

    sub_eps5  = _clamp(eps5y / 2)  if _is_num(eps5y) else None
    sub_rev5  = _clamp(rev5y / 2)  if _is_num(rev5y) else None

    sub_acc = None
    if _is_num(eps5y) and _is_num(eps3y) and _is_num(rev5y) and _is_num(rev3y):
        acc = (eps3y - eps5y) + (rev3y - rev5y)
        if math.isfinite(acc):
            sub_acc = _clamp(5 + 0.5 * acc)

    sub_stage = None
    if _is_num(rev5y):
        sub_stage = 10 if rev5y >= 25 else 8 if rev5y >= 15 else 6 if rev5y >= 5 else 3

    return _safe_avg([sub_eps5, sub_rev5, sub_acc, sub_stage])


def _score_moat(m: dict) -> float | None:
    base      = m.get("moat_score_0_10")
    recurring = m.get("recurring_revenue_pct")
    insider   = m.get("insider_own_pct")
    founder   = m.get("founder_led_bool")

    sub_base  = base if _is_num(base) else None
    sub_rec   = 10 * max(0.0, min(1.0, recurring / 100)) if _is_num(recurring) else None
    sub_owner = None
    if _is_num(insider):
        sub_owner = min(2.0, 10 * max(0.0, min(1.0, insider / 100))) + (1 if founder else 0)

    return _safe_avg([sub_base, sub_rec, sub_owner])


def _score_risk(m: dict) -> float | None:
    base_risk = m.get("riskdownside_score_0_10")
    nd_ebitda = m.get("netdebt_to_ebitda")
    nc_mcap   = m.get("netcash_to_mktcap_pct")
    beta      = m.get("beta_5y")
    maxdd     = m.get("maxdrawdown_5y_pct")
    cyc_tag   = m.get("sector_cyc_tag")

    sub_base = base_risk if _is_num(base_risk) else None
    sub_nd   = _clamp(10 * max(0.0, min(1.0, (3 - nd_ebitda) / 2)))   if _is_num(nd_ebitda) else None
    sub_nc   = _clamp(5 + max(0.0, nc_mcap) / 2)                       if _is_num(nc_mcap) else None
    sub_beta = _clamp(10 - abs(beta) * 5)                               if _is_num(beta) else None
    sub_dd   = _clamp(10 - abs(maxdd))                                  if _is_num(maxdd) else None

    cyc_map  = {"defensive": 8, "secular": 7, "growth": 6, "cyclical": 4, "deep-cyclical": 3}
    sub_cyc  = cyc_map.get((cyc_tag or "").lower(), 6)

    return _safe_avg([sub_base, sub_nd, sub_nc, sub_beta, sub_dd, sub_cyc])


def _score_macro(m: dict) -> float | None:
    v = m.get("macrofit_score_0_10")
    return v if _is_num(v) else None


def _score_narrative(m: dict) -> float | None:
    v = m.get("narrative_score_0_10")
    return v if _is_num(v) else None


def _score_dilution(m: dict) -> float | None:
    change = m.get("sharecount_change_5y_pct")
    sbc    = m.get("sbc_to_sales_pct")
    if not _is_num(change) or not _is_num(sbc):
        return None
    return _clamp(10 + 2 * (change - sbc))


_CATEGORY_SCORERS = {
    "valuation":         _score_valuation,
    "quality":           _score_quality,
    "capitalAllocation": _score_capital_allocation,
    "growth":            _score_growth,
    "moat":              _score_moat,
    "risk":              _score_risk,
    "macro":             _score_macro,
    "narrative":         _score_narrative,
    "dilution":          _score_dilution,
}


def compute_category_scores(metrics: dict) -> dict[str, float | None]:
    """Compute all 9 category scores. Null = insufficient data for that category."""
    return {cat: fn(metrics) for cat, fn in _CATEGORY_SCORERS.items()}


def compute_final_score(
    category_scores: dict[str, float | None],
    lens_weights: dict[str, float | None],
) -> float | None:
    """
    Weighted average of category scores.
    Phase 4 policy: null categories are EXCLUDED from the weighted average.
    A category with null score does not count toward totalWeight.
    """
    total_score = 0.0
    total_weight = 0.0
    for cat, score in category_scores.items():
        w = lens_weights.get(cat) or lens_weights.get(_camel_to_snake(cat))
        if _is_num(score) and _is_num(w) and w > 0:
            total_score  += score * w
            total_weight += w
    return total_score / total_weight if total_weight > 0 else None


# ---------------------------------------------------------------------------
# Recommendation (Phase 2.1 — score only, no MOS/confidence gating)
# ---------------------------------------------------------------------------

def compute_recommendation(
    final_score: float | None,
    buy_threshold: float = 6.5,
    watch_threshold: float = 4.5,
) -> str:
    if not _is_num(final_score):
        return "INSUFFICIENT_DATA"
    if final_score >= buy_threshold:
        return "BUY"
    if final_score >= watch_threshold:
        return "WATCH"
    return "AVOID"


# ---------------------------------------------------------------------------
# MOS signal (Phase 2.2 — display only)
# ---------------------------------------------------------------------------

def compute_mos_signal(
    mos_pct: float | None,
    neutral_band: float = MOS_NEUTRAL_BAND,
) -> str | None:
    """
    Returns:
      "+"  if mos_pct > +neutral_band  (stock is below fair value)
      "-"  if mos_pct < -neutral_band  (stock is above fair value)
      "0"  if within ±neutral_band
      None if mos_pct is not available
    """
    if not _is_num(mos_pct):
        return None
    if mos_pct > neutral_band:
        return "+"
    if mos_pct < -neutral_band:
        return "-"
    return "0"


# ---------------------------------------------------------------------------
# Explainability (Phase 5)
# ---------------------------------------------------------------------------

def _compute_contributions(
    category_scores: dict[str, float | None],
    lens_weights: dict[str, float | None],
    final_score: float | None,
) -> tuple[list[dict], list[dict]]:
    """
    Per-category contribution relative to the final score.
    contribution_i = (score_i - final_score) * weight_i / sum(weights_present)

    Returns (top_positive[:3], top_negative[:3]).
    """
    if not _is_num(final_score):
        return [], []

    total_w = sum(
        (lens_weights.get(c) or lens_weights.get(_camel_to_snake(c)) or 0)
        for c, s in category_scores.items()
        if _is_num(s)
    )
    if total_w == 0:
        return [], []

    contribs = []
    for cat, score in category_scores.items():
        w = lens_weights.get(cat) or lens_weights.get(_camel_to_snake(cat)) or 0
        if _is_num(score) and w > 0:
            contrib = (score - final_score) * w / total_w
            contribs.append({
                "category":     cat,
                "score":        round(score, 3),
                "weight":       w,
                "contribution": round(contrib, 4),
            })

    contribs.sort(key=lambda x: x["contribution"], reverse=True)
    positive = [c for c in contribs if c["contribution"] >= 0][:3]
    negative = sorted(
        [c for c in contribs if c["contribution"] < 0],
        key=lambda x: x["contribution"],
    )[:3]
    return positive, negative


# ---------------------------------------------------------------------------
# Main snapshot computation
# ---------------------------------------------------------------------------

def compute_snapshot(
    ticker_symbol: str,
    lens: dict[str, Any],
    metrics: dict[str, Any],
    mos_pct: float | None = None,
    resolution_warnings: list[str] | None = None,
    neutral_band: float = MOS_NEUTRAL_BAND,
) -> dict[str, Any]:
    """
    Compute a fully deterministic ScoreSnapshot dict.

    Parameters
    ----------
    ticker_symbol       : stock symbol
    lens                : LensPreset dict (name, id, weights, thresholds)
    metrics             : Metrics dict (flat field → value)
    mos_pct             : optional Margin of Safety % (display only)
    resolution_warnings : warnings from TTM/metric resolution checks
    neutral_band        : ± band for mos_signal (default 5 %)

    Returns
    -------
    Dict ready for persistence (not yet flushed to DB).
    """
    from backend.services.confidence_calculator import compute_confidence

    lens_name      = lens.get("name", "Conservative")
    lens_id        = lens.get("id", "")
    buy_threshold  = lens.get("buyThreshold") or 6.5
    watch_threshold= lens.get("watchThreshold") or 4.5

    lens_weights = {
        "valuation":         lens.get("valuation"),
        "quality":           lens.get("quality"),
        "capitalAllocation": lens.get("capitalAllocation"),
        "growth":            lens.get("growth"),
        "moat":              lens.get("moat"),
        "risk":              lens.get("risk"),
        "macro":             lens.get("macro"),
        "narrative":         lens.get("narrative"),
        "dilution":          lens.get("dilution"),
    }

    # 1. Category scores (nulls preserved — not coerced to 0)
    category_scores = compute_category_scores(metrics)

    # 2. Final score (null categories excluded from weighted avg)
    final_score = compute_final_score(category_scores, lens_weights)

    # 3. Recommendation — score only
    recommendation = compute_recommendation(final_score, buy_threshold, watch_threshold)

    # 4. Confidence — does NOT affect recommendation
    conf = compute_confidence(metrics, lens_name)

    # 5. MOS signal — display only
    mos_signal = compute_mos_signal(mos_pct, neutral_band)

    # 6. Explainability
    top_pos, top_neg = _compute_contributions(category_scores, lens_weights, final_score)

    # 7. Data version
    as_of_raw = metrics.get("as_of_date") or date.today().isoformat()
    as_of_date = as_of_raw.isoformat() if hasattr(as_of_raw, "isoformat") else str(as_of_raw)

    # 8. Deterministic hash — same inputs → same hash
    hash_payload = {
        "ticker":           ticker_symbol,
        "lens_id":          lens_id,
        "lens_name":        lens_name,
        "score_version":    SCORE_VERSION,
        "as_of_date":       as_of_date,
        "final_score":      round(final_score, 6) if _is_num(final_score) else None,
        "category_scores":  {k: round(v, 6) if _is_num(v) else None for k, v in category_scores.items()},
        "recommendation":   recommendation,
    }
    snapshot_hash = hashlib.sha256(
        json.dumps(hash_payload, sort_keys=True).encode()
    ).hexdigest()[:16]

    return {
        "id":                        str(uuid.uuid4()),
        "ticker_symbol":             ticker_symbol,
        "lens_id":                   lens_id,
        "lens_name":                 lens_name,
        "score_version":             SCORE_VERSION,
        "data_version":              as_of_date,
        "final_score":               round(final_score, 4) if _is_num(final_score) else None,
        "category_scores":           {k: round(v, 4) if _is_num(v) else None for k, v in category_scores.items()},
        "recommendation":            recommendation,
        "confidence_pct":            conf["confidence_pct"],
        "confidence_grade":          conf["confidence_grade"],
        "mos_pct":                   round(mos_pct, 2) if _is_num(mos_pct) else None,
        "mos_signal":                mos_signal,
        "top_positive_contributors": top_pos,
        "top_negative_contributors": top_neg,
        "missing_critical_fields":   conf["missing_fields"],
        "resolution_warnings":       resolution_warnings or [],
        "snapshot_hash":             snapshot_hash,
        "as_of_date":                as_of_date,
        "created_at":                datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def upsert_snapshot(db: Session, snapshot_data: dict[str, Any]) -> Any:
    """
    Upsert a ScoreSnapshot.
    Idempotency key = (ticker_symbol, lens_id, as_of_date).

    Complex fields (list/dict) are JSON-serialised before storage.
    """
    from backend.models import ScoreSnapshot  # local import to avoid circular

    _JSON_FIELDS = {
        "category_scores", "top_positive_contributors",
        "top_negative_contributors", "missing_critical_fields",
        "resolution_warnings",
    }

    def _prep(data: dict) -> dict:
        return {
            k: json.dumps(v) if k in _JSON_FIELDS and not isinstance(v, str) else v
            for k, v in data.items()
        }

    existing = (
        db.query(ScoreSnapshot)
        .filter(
            ScoreSnapshot.ticker_symbol == snapshot_data["ticker_symbol"],
            ScoreSnapshot.lens_id       == snapshot_data["lens_id"],
            ScoreSnapshot.as_of_date    == snapshot_data["as_of_date"],
        )
        .first()
    )

    prepped = _prep(snapshot_data)

    if existing:
        for k, v in prepped.items():
            if k not in ("id", "created_at"):
                setattr(existing, k, v)
        db.commit()
        db.refresh(existing)
        return existing
    else:
        record = ScoreSnapshot(**prepped)
        db.add(record)
        db.commit()
        db.refresh(record)
        return record
