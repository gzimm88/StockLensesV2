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


def _weighted_avg(subs: list, weights: list) -> float | None:
    """Weighted average that excludes null/non-finite sub-scores (weight redistributed)."""
    num, den = 0.0, 0.0
    for v, w in zip(subs, weights):
        if _is_num(v):
            num += v * w
            den += w
    return num / den if den > 0 else None


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
    """
    Calibrated quality scorer.  Changes vs original:
    1. Null guards: roic/fcf_margin already correct; margin_stability already correct.
    2. Accruals null → None (not 10.0). Assuming clean books when data is absent
       is optimistic. Missing data should be excluded, not rewarded.
    3. Weighted average: ROIC dominant (35%) — most fundamental quality signal.
       ROIC 35% · FCF margin 25% · Cash conversion 20% · Accruals 10% · Stability 10%
    """
    roic       = m.get("roic_pct")
    fcf_margin = m.get("fcf_margin_pct")
    cfo_to_ni  = m.get("cfo_to_ni")
    fcf_ebit   = m.get("fcf_to_ebit")
    accruals   = m.get("accruals_ratio")
    ms         = m.get("margin_stdev_5y_pct")

    sub_roic = _clamp(roic / 2)         if _is_num(roic)       else None
    sub_fcfm = _clamp(fcf_margin / 1.5) if _is_num(fcf_margin) else None

    cc_vals  = [v for v in [cfo_to_ni, fcf_ebit] if _is_num(v)]
    sub_cc   = _clamp(10 * sum(cc_vals) / len(cc_vals)) if cc_vals else None

    # Null = missing data → exclude (do NOT default to 10 / assume clean books)
    sub_acc  = None
    if _is_num(accruals):
        sub_acc = _clamp(10 * max(0.0, min(1.0, (0.10 - abs(accruals)) / 0.10)))

    sub_ms   = _clamp(10 - _clamp(ms * 0.5)) if _is_num(ms) else None

    return _weighted_avg(
        [sub_roic, sub_fcfm, sub_cc, sub_acc, sub_ms],
        [0.35, 0.25, 0.20, 0.10, 0.10],
    )


def _score_capital_allocation(m: dict) -> float | None:
    """
    Calibrated capital allocation scorer.  Changes vs original:
    1. Buyback: lookup table replaces (yield+2)/2.
       OLD: 0% yield → 1/10 (penalises growth reinvestors like DUOL, CRM)
       NEW: 0% yield → 5/10 (neutral — reinvestment is not punished)
    2. Weighted average (buyback 40%, coverage 40%, roiic 20%).
       roiic almost never available; when absent, collapses to 50/50.
    """
    buyback = m.get("buyback_yield_pct")
    int_cov = m.get("interest_coverage_x")

    sub_bb = None
    if _is_num(buyback):
        sub_bb = (10 if buyback >= 6 else 8 if buyback >= 4 else 7 if buyback >= 2 else
                  5 if buyback >= 0 else 3 if buyback >= -2 else 1)

    sub_ic = _clamp(math.log10(int_cov + 1) * 4) if (_is_num(int_cov) and int_cov > -1) else None

    # roiic rarely available; included when present
    ebit_t    = m.get("ebit_t")
    ebit_t3   = m.get("ebit_t3")
    invcap_t  = m.get("invcap_t")
    invcap_t3 = m.get("invcap_t3")
    sub_roiic = None
    if all(_is_num(x) for x in [ebit_t, ebit_t3, invcap_t, invcap_t3]) and invcap_t != invcap_t3:
        roiic_proxy = (ebit_t - ebit_t3) / (invcap_t - invcap_t3)
        sub_roiic = _clamp(roiic_proxy * 100 / 2)

    return _weighted_avg([sub_bb, sub_ic, sub_roiic], [0.40, 0.40, 0.20])


def _score_growth(m: dict) -> float | None:
    """
    Calibrated growth scorer.  Changes vs original:
    1. EPS / Rev CAGR: lookup tables (aligned with real-world quality tiers).
    2. Acceleration: centered at 7 (not 5), multiplier 0.3 (not 0.5).
       Mild deceleration from a high base is normal and should not crater the score.
    3. Durability (sub_rec): recurring_revenue_pct replaces the old sub_stage
       (which was a redundant second rev-CAGR bucket).  A company with 90%+
       recurring revenue grows more durably than one with the same nominal CAGR
       but episodic / transactional revenue.
    4. Weighted average (EPS 40%, Rev 30%, Acc 15%, Durability 15%).

    Example — ADBE vs MOH (both ~6.9 before fix, ~7.4 vs 6.4 after):
      ADBE: eps5y=18.45%, rev5y=9%, recurring=93% → durability=9.3 → Growth~7.4
      MOH:  eps5y=12.4%,  rev5y=11.2%, recurring=40% → durability=4.0 → Growth~6.4
    """
    eps5y = m.get("eps_cagr_5y_pct")
    rev5y = m.get("revenue_cagr_5y_pct")
    eps3y = m.get("eps_cagr_3y_pct")
    rev3y = m.get("revenue_cagr_3y_pct")

    # EPS CAGR 5Y — lookup table
    sub_eps5 = None
    if _is_num(eps5y):
        sub_eps5 = (10 if eps5y >= 25 else 9 if eps5y >= 20 else 8 if eps5y >= 15 else
                    7 if eps5y >= 12 else 6 if eps5y >= 10 else 5 if eps5y >= 7 else
                    3 if eps5y >= 0 else 1)

    # Revenue CAGR 5Y — lookup table
    sub_rev5 = None
    if _is_num(rev5y):
        sub_rev5 = (10 if rev5y >= 20 else 9 if rev5y >= 15 else 8 if rev5y >= 12 else
                    7 if rev5y >= 8 else 6 if rev5y >= 5 else 3 if rev5y >= 0 else 1)

    # Acceleration: 3Y vs 5Y trend — centered at 7, dampened
    sub_acc = None
    if _is_num(eps5y) and _is_num(eps3y) and _is_num(rev5y) and _is_num(rev3y):
        acc = (eps3y - eps5y) + (rev3y - rev5y)
        if math.isfinite(acc):
            sub_acc = _clamp(max(0.0, 7 + 0.3 * acc))

    # Durability: recurring revenue % — rewards sticky, subscription-like growth
    # over episodic or thin-margin growth at the same nominal CAGR.
    # (sub_stage was a second rev-CAGR bucket — removed as redundant.)
    sub_rec = None
    rec_pct = m.get("recurring_revenue_pct")
    if _is_num(rec_pct):
        sub_rec = 10 * max(0.0, min(1.0, rec_pct / 100))

    # Weighted: EPS5Y 40% · Rev5Y 30% · Acceleration 15% · Durability 15%
    return _weighted_avg([sub_eps5, sub_rev5, sub_acc, sub_rec], [0.40, 0.30, 0.15, 0.15])


def _score_moat(m: dict) -> float | None:
    """
    Calibrated moat scorer.  Changes vs original:
    1. owner_block recalibrated for large caps:
       OLD: min(2, 10 * insider/100) + 1_if_founder → max 3/10
       NEW: 5 * min(1, insider/5%) + 2_if_founder   → max 7/10
            5% insider = meaningful at large-cap scale; founder adds +2 pts
    2. Weighted average (base 55%, rec 30%, owner 15%) instead of equal avg.
       The holistic moat quality score (sub_base) is the anchor.

    Example — Google  (base=8, rec=45%, insider=6.65%, founder=Yes):
      OLD: avg(8, 4.5, 1.67) = 4.72
      NEW: w-avg(8, 4.5, 7.0) with [0.55,0.30,0.15] = 6.80

    Example — ASML (base=8, rec=30%, insider=0.008%, founder=Yes):
      OLD: avg(8, 3.0, 1.0) = 4.03
      NEW: w-avg(8, 3.0, 2.0) with [0.55,0.30,0.15] = 5.60
    """
    base      = m.get("moat_score_0_10")
    recurring = m.get("recurring_revenue_pct")
    insider   = m.get("insider_own_pct")
    founder   = m.get("founder_led_bool")

    sub_base = base if _is_num(base) else None
    sub_rec  = 10 * max(0.0, min(1.0, recurring / 100)) if _is_num(recurring) else None

    sub_owner = None
    if _is_num(insider):
        # Large-cap calibrated: 5%+ insider = full owner score (5.0); founder adds +2.0
        owner_score   = _clamp(5.0 * min(1.0, insider / 5.0))
        founder_bonus = 2.0 if founder else 0.0
        sub_owner     = min(10.0, owner_score + founder_bonus)

    # sub_base is the holistic moat quality assessment — give it dominant weight
    weights = [0.55, 0.30, 0.15]
    subs    = [sub_base, sub_rec, sub_owner]
    num, den = 0.0, 0.0
    for v, w in zip(subs, weights):
        if _is_num(v):
            num += v * w; den += w
    return num / den if den > 0 else None


def _score_risk(m: dict) -> float | None:
    """
    Risk scorer — further calibrated beyond the max_drawdown fix.

    Additional changes:
    1. net_cash_mcap: symmetric lookup — negative net cash (net debt) now penalised.
       OLD: max(0, nc)/2 + 5 → floor at 5 for ALL indebted companies
       NEW: ≥20%→10, ≥10%→8, ≥0%→6, ≥-10%→4, ≥-25%→2, <-25%→0
    2. Weighted average: sub_base dominant (35%) — mirrors moat_base philosophy.
       The holistic expert risk score is the primary anchor; cyclicality (a tag
       lookup) should not get equal weight.
       Weights: base 35% · net_debt 20% · beta 15% · drawdown 10% · net_cash 10% · cyc 10%
    """
    base_risk = m.get("riskdownside_score_0_10")
    nd_ebitda = m.get("netdebt_to_ebitda")
    nc_mcap   = m.get("netcash_to_mktcap_pct")
    beta      = m.get("beta_5y")
    maxdd     = m.get("maxdrawdown_5y_pct")
    cyc_tag   = m.get("sector_cyc_tag")

    sub_base = base_risk if _is_num(base_risk) else None
    sub_nd   = _clamp(10 * max(0.0, min(1.0, (3 - nd_ebitda) / 2))) if _is_num(nd_ebitda) else None
    sub_beta = _clamp(10 - abs(beta) * 5)                            if _is_num(beta) else None

    # Net cash / mkt cap — symmetric lookup (negative = net debt is now penalised)
    sub_nc = None
    if _is_num(nc_mcap):
        sub_nc = (10 if nc_mcap >= 20 else 8 if nc_mcap >= 10 else 6 if nc_mcap >= 0 else
                  4 if nc_mcap >= -10 else 2 if nc_mcap >= -25 else 0)

    # Max drawdown — lookup table (from previous fix)
    sub_dd = None
    if _is_num(maxdd):
        d = abs(maxdd)
        sub_dd = (10 if d <= 15 else 8 if d <= 25 else 6 if d <= 35 else
                  4 if d <= 50 else 2 if d <= 65 else 0)

    cyc_map = {"defensive": 8, "secular": 7, "growth": 6, "cyclical": 4, "deep-cyclical": 3}
    sub_cyc = cyc_map.get((cyc_tag or "").lower(), 6)

    return _weighted_avg(
        [sub_base, sub_nd, sub_nc, sub_beta, sub_dd, sub_cyc],
        [0.35, 0.20, 0.10, 0.15, 0.10, 0.10],
    )


def _score_macro(m: dict) -> float | None:
    v = m.get("macrofit_score_0_10")
    return v if _is_num(v) else None


def _score_narrative(m: dict) -> float | None:
    v = m.get("narrative_score_0_10")
    return v if _is_num(v) else None


def _score_dilution(m: dict) -> float | None:
    """
    Calibrated dilution scorer.  Change vs original:
    OLD: _clamp(10 + 2 * (sharecount_change - sbc_to_sales))
    BUG: subtracts % of shares from % of revenue — dimensionally inconsistent.
         Impact of 2% SBC/sales on share count depends entirely on market cap / revenue ratio.

    NEW: independent lookup tables for each signal.
      sub_change: positive = fewer shares 5Y (net buyback > dilution)
        ≥+5%→10 | ≥+2%→8 | ≥0%→6 | ≥-2%→4 | ≥-5%→2 | <-5%→0
      sub_sbc: SBC as % of sales (lower = less compensation dilution)
        ≤1%→10 | ≤2%→8 | ≤4%→6 | ≤6%→4 | ≤10%→2 | >10%→0
    Weighted: share-count change 60% (primary), SBC ratio 40% (supporting)
    """
    change = m.get("sharecount_change_5y_pct")
    sbc    = m.get("sbc_to_sales_pct")

    sub_change = None
    if _is_num(change):
        sub_change = (10 if change >= 5 else 8 if change >= 2 else 6 if change >= 0 else
                      4 if change >= -2 else 2 if change >= -5 else 0)

    sub_sbc = None
    if _is_num(sbc):
        sub_sbc = (10 if sbc <= 1 else 8 if sbc <= 2 else 6 if sbc <= 4 else
                   4 if sbc <= 6 else 2 if sbc <= 10 else 0)

    return _weighted_avg([sub_change, sub_sbc], [0.60, 0.40])


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
