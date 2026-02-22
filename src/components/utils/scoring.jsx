import { nz, toPoints } from "./num";

const clamp01 = (x) => Math.max(0, Math.min(1, x));
const cap10 = (x) => Math.max(0, Math.min(10, x));

function safeAvg(vals) {
    const a = vals.filter((v) => typeof v === 'number' && !Number.isNaN(v));
    return a.length ? a.reduce((s, v) => s + v, 0) / a.length : null;
}

function weightedAvg(subs, weights) {
    let num = 0, den = 0;
    subs.forEach((v, i) => { if (v != null && typeof v === 'number' && !Number.isNaN(v)) { num += v * weights[i]; den += weights[i]; } });
    return den > 0 ? num / den : null;
}

// A) Valuation (weighted average) — unchanged
function scoreValuation(m) {
    const pe = nz(m.pe_fwd);
    const ev = nz(m.ev_ebitda);
    const fcf_y = toPoints(m.fcf_yield_pct);
    const peg = nz(m.peg_5y);
    const pe_ttm = nz(m.pe_ttm);
    const pe_low = nz(m.pe_5y_low);
    const pe_high = nz(m.pe_5y_high);

    const sub_pe = pe == null ? null : (pe <= 10 ? 10 : pe <= 15 ? 9 : pe <= 20 ? 7 : pe <= 25 ? 5 : pe <= 35 ? 3 : 1);
    const sub_ev = ev == null ? null : (ev <= 7 ? 10 : ev <= 10 ? 8 : ev <= 14 ? 6 : ev <= 20 ? 4 : 2);
    const sub_fcfy = fcf_y == null ? null : (fcf_y >= 8 ? 10 : fcf_y >= 5 ? 8 : fcf_y >= 3 ? 6 : fcf_y >= 1 ? 3 : 1);
    const sub_peg = peg == null ? null : (peg <= 1 ? 9 : peg <= 1.5 ? 7 : peg <= 2 ? 5 : peg <= 3 ? 3 : 1);
    const sub_hist = (pe_ttm == null || pe_low == null || pe_high == null || pe_high <= pe_low) ? null : 10 * clamp01((pe_high - pe_ttm) / (pe_high - pe_low));

    // pe=0.35  peg=0.15  ev=0.20  fcfy=0.15  hist=0.15  (mirrors backend)
    const weights = [0.35, 0.15, 0.20, 0.15, 0.15];
    const vals = [sub_pe, sub_peg, sub_ev, sub_fcfy, sub_hist];
    let num = 0, den = 0;
    vals.forEach((v, i) => { if (v != null) { num += v * weights[i]; den += weights[i]; } });
    return den ? num / den : null;
}

// B) Quality — CALIBRATED
//
// Changes vs original:
//  1. Null guards on all sub-scores:
//     - roic/fcf_margin: toPoints(null)/divisor = 0 in JS (null coerces) → was always 0, not excluded
//     - margin_stability: null stdev → was treated as 10 (perfectly stable)
//  2. Accruals null → null (not 10). Assuming clean books when data is absent
//     is optimistic, not conservative. Missing data should be excluded.
//  3. Weighted average: ROIC dominant (most fundamental quality signal)
//     ROIC 35% · FCF margin 25% · Cash conversion 20% · Accruals 10% · Margin stability 10%
function scoreQuality(m) {
    const roicRaw = toPoints(m.roic_pct);
    const roic = roicRaw == null ? null : cap10(roicRaw / 2);

    const fcfmRaw = toPoints(m.fcf_margin_pct);
    const fcf_margin = fcfmRaw == null ? null : cap10(fcfmRaw / 1.5);

    const ccAvg = safeAvg([nz(m.cfo_to_ni), nz(m.fcf_to_ebit)]);
    const cash_conversion = ccAvg == null ? null : cap10(10 * ccAvg);

    // Null = missing data → exclude (not assume perfect books)
    const accRaw = m.accruals_ratio;
    const accruals = accRaw == null ? null
        : 10 * clamp01((0.10 - Math.abs(nz(accRaw))) / 0.10);

    const msRaw = toPoints(m.margin_stdev_5y_pct);
    const margin_stability = msRaw == null ? null : 10 - cap10(msRaw * 0.5);

    // ROIC dominant: most predictive of durable business quality
    return weightedAvg(
        [roic, fcf_margin, cash_conversion, accruals, margin_stability],
        [0.35, 0.25, 0.20, 0.10, 0.10]
    );
}

// C) Capital Allocation — CALIBRATED
//
// Changes vs original:
//  1. Buyback: lookup table replaces (yield+2)/2 linear formula.
//     OLD: 0% yield → (0+2)/2 = 1/10 (heavily penalises growth reinvestors like DUOL/CRM)
//     NEW: 0% yield → 5/10 (neutral — reinvestment isn't punished if returns are earned)
//          Negative yield (net issuance) is penalised.
//  2. interest_cover null guard:
//     OLD: nz(null)=null, null+1=1, log10(1)*4=0 → null treated as 0 coverage (score 0)
//     NEW: explicit null → null → excluded from weighted average
//  3. Weighted average (buyback 40%, coverage 40%, roiic 20%).
//     roiic is almost never available; when absent, weights collapse to 50/50.
function scoreCapitalAllocation(m) {
    // Buyback yield — lookup table; 0% = neutral (reinvestment not penalised)
    const byRaw = toPoints(m.buyback_yield_pct);
    const buyback = byRaw == null ? null
        : byRaw >= 6 ? 10 : byRaw >= 4 ? 8 : byRaw >= 2 ? 7 : byRaw >= 0 ? 5
        : byRaw >= -2 ? 3 : 1;

    // Interest coverage — null guard (null no longer coerces to 0 via log10)
    const icRaw = nz(m.interest_coverage_x);
    const interest_cover = icRaw == null ? null : cap10(Math.log10(icRaw + 1) * 4);

    const roiic = (() => {
        const ebit_t = nz(m.ebit_t);
        const ebit_t3 = nz(m.ebit_t3);
        const invcap_t = nz(m.invcap_t);
        const invcap_t3 = nz(m.invcap_t3);
        if (ebit_t == null || ebit_t3 == null || invcap_t == null || invcap_t3 == null || invcap_t === invcap_t3) return null;
        const roiic_proxy = (ebit_t - ebit_t3) / (invcap_t - invcap_t3);
        return cap10(roiic_proxy * 100 / 2);
    })();

    return weightedAvg([buyback, interest_cover, roiic], [0.40, 0.40, 0.20]);
}

// D) Growth — CALIBRATED
//
// Changes vs original:
//  1. EPS / Rev CAGR: lookup tables instead of linear /2 scaling
//     → more realistic breakpoints aligned with real-world quality tiers
//  2. Acceleration sub-score: centered at 7 (not 5), multiplier 0.3 (not 0.5)
//     → moderate deceleration from a high base (e.g. 16→11%) is normal and should not
//        halve the score; formula now starts at 7 and damps the delta
//  3. Stage: 5 breakpoints instead of 3 — distinguishes 5-25% revenue growth
//  4. Weighted average (EPS 40%, Rev 30%, Acc 15%, Stage 15%) instead of equal avg
//     → acceleration is an informative signal but should not have 1/4 of total weight
//
// Example — Google (EPS5Y=16, Rev5Y=12, EPS3Y=11, Rev3Y=12.5):
//   OLD: avg(8, 6, 2.75, 6) = 5.69   ← deceleration sub crushed it
//   NEW: w-avg(8, 8, 5.65, 7) = 7.50  ← appropriate for 16% EPS CAGR at scale
function scoreGrowth(m) {
    const eps5yRaw = toPoints(m.eps_cagr_5y_pct);
    const rev5yRaw = toPoints(m.revenue_cagr_5y_pct ?? m.rev_cagr_5y_pct);
    const eps3yRaw = toPoints(m.eps_cagr_3y_pct);
    const rev3yRaw = toPoints(m.revenue_cagr_3y_pct ?? m.rev_cagr_3y_pct);

    // EPS CAGR 5Y — lookup table (max at ≥25%)
    const sub_eps5 = eps5yRaw == null ? null :
        eps5yRaw >= 25 ? 10 : eps5yRaw >= 20 ? 9 : eps5yRaw >= 15 ? 8 :
        eps5yRaw >= 12 ? 7 : eps5yRaw >= 10 ? 6 : eps5yRaw >= 7  ? 5 :
        eps5yRaw >= 0  ? 3 : 1;

    // Revenue CAGR 5Y — lookup table
    const sub_rev5 = rev5yRaw == null ? null :
        rev5yRaw >= 20 ? 10 : rev5yRaw >= 15 ? 9 : rev5yRaw >= 12 ? 8 :
        rev5yRaw >= 8  ? 7 : rev5yRaw >= 5  ? 6 : rev5yRaw >= 0  ? 3 : 1;

    // Acceleration: 3Y vs 5Y trend
    // Centered at 7 so mild deceleration from a high base costs only a small penalty.
    // Multiplier 0.3 dampens the delta vs original 0.5.
    const acceleration = (() => {
        if (eps3yRaw == null || eps5yRaw == null || rev3yRaw == null || rev5yRaw == null) return null;
        const acc = (eps3yRaw - eps5yRaw) + (rev3yRaw - rev5yRaw);
        if (!Number.isFinite(acc)) return null;
        return cap10(Math.max(0, 7 + 0.3 * acc));
    })();

    // Durability: recurring revenue % — rewards sticky, subscription-like growth
    // over episodic or thin-margin growth at the same nominal CAGR.
    // (sub_stage was a second rev-CAGR bucket — removed as redundant.)
    const recPctRaw = toPoints(m.recurring_revenue_pct);
    const sub_rec = recPctRaw == null ? null : 10 * clamp01(recPctRaw / 100);

    // Weighted: EPS5Y 40% · Rev5Y 30% · Acceleration 15% · Durability 15%
    return weightedAvg([sub_eps5, sub_rev5, acceleration, sub_rec], [0.40, 0.30, 0.15, 0.15]);
}

// E) Moat — CALIBRATED
//
// Changes vs original:
//  1. sub_base uses explicit null-guard (prevents nz() coercing null→0 into the avg)
//  2. owner_block: recalibrated for large caps
//     OLD: min(2, 10 * insider/100) + 1_if_founder → max 3/10; Google (6.65%) → 1.67
//     NEW: 5 * min(1, insider/5%) + 2_if_founder → max 7/10; Google (6.65%) → 7.0
//          5% insider ownership = meaningful for large caps; founder adds +2 pts
//  3. Weighted average: base 55% · recurring 30% · ownership 15%
//     → holistic moat quality (sub_base) is the anchor; recurring and ownership refine it
//
// Example — Google (base=8, rec=45%, insider=6.65%, founder=Yes):
//   OLD: avg(8, 4.5, 1.67) = 4.72
//   NEW: w-avg(8, 4.5, 7.0) with [0.55,0.30,0.15] = 6.80
//
// Example — ASML (base=8, rec=30%, insider=0.008%, founder=Yes):
//   OLD: avg(8, 3.0, 1.0) = 4.03
//   NEW: w-avg(8, 3.0, 2.0) with [0.55,0.30,0.15] = 5.60
function scoreMoat(m) {
    // Explicit null guard — do not coerce missing moat data to 0
    const sub_base = m.moat_score_0_10 != null ? m.moat_score_0_10 : null;

    // Recurring revenue: % of revenue that is contractual/subscription
    const recPct = toPoints(m.recurring_revenue_pct);
    const sub_rec = recPct != null ? 10 * clamp01(recPct / 100) : null;

    // Ownership — large-cap calibrated
    //   5%+ insider = full owner score (5.0); founder status adds 2.0 pts; cap 10
    const ins = toPoints(m.insider_own_pct);
    const sub_owner = (() => {
        if (ins == null) return null;
        const ownerScore = cap10(5.0 * Math.min(1.0, ins / 5.0));
        const founderBonus = m.founder_led_bool ? 2.0 : 0;
        return Math.min(10, ownerScore + founderBonus);
    })();

    // sub_base is the holistic moat quality assessment — give it dominant weight
    return weightedAvg([sub_base, sub_rec, sub_owner], [0.55, 0.30, 0.15]);
}

// F) Risk/Downside — CALIBRATED
//
// Changes vs previous fix (max_drawdown) and additional improvements:
//  1. Null guards on netdebt_to_ebitda and beta_5y:
//     OLD: nz(null)=null → (3-null)/2 = 1.5 → score 10 (treated as zero-debt best case)
//           nz(null)=null → Math.abs(null)=0 → 10-0=10 (treated as zero-beta)
//     NEW: null → null → excluded from weighted average
//  2. net_cash_mcap: symmetric lookup (negative net cash now penalised below 6)
//     OLD: max(0, nc)/2 + 5 → floor at 5 for all indebted companies
//     NEW: lookup table →  ≥20%→10, ≥10%→8, ≥0%→6, ≥-10%→4, ≥-25%→2, <-25%→0
//  3. Weighted average: sub_base dominant (35%) — same philosophy as moat_base in moat scorer.
//     sub_base is a holistic expert judgment; cyclicality (a tag lookup) should not
//     equal it at 1/6 weight.
//     Weights: base_risk 35% · net_debt 20% · beta 15% · max_drawdown 10% · net_cash 10% · cyclicality 10%
function scoreRisk(m) {
    // Holistic analyst risk score — dominant anchor (mirrors moat_base philosophy)
    const base_risk = nz(m.riskdownside_score_0_10);

    // Net debt/EBITDA — null guard: null no longer coerces to best-case score
    const ndRaw = nz(m.netdebt_to_ebitda);
    const net_debt_ebitda = ndRaw == null ? null : 10 * clamp01((3 - ndRaw) / 2);

    // Net cash / mkt cap — symmetric: negative net cash (debt) penalised below 6
    const ncRaw = toPoints(m.netcash_to_mktcap_pct);
    const net_cash_mcap = ncRaw == null ? null
        : ncRaw >= 20 ? 10 : ncRaw >= 10 ? 8 : ncRaw >= 0 ? 6
        : ncRaw >= -10 ? 4 : ncRaw >= -25 ? 2 : 0;

    // Beta — null guard: null no longer coerces to Math.abs(null)=0 → score 10
    const betaRaw = nz(m.beta_5y);
    const beta = betaRaw == null ? null : cap10(10 - Math.abs(betaRaw) * 5);

    // Max drawdown — lookup table (from previous fix); accept both field name variants
    const ddRaw = toPoints(m.max_drawdown_5y_pct ?? m.maxdrawdown_5y_pct);
    const max_drawdown = ddRaw == null ? null : (() => {
        const d = Math.abs(ddRaw);
        return d <= 15 ? 10 : d <= 25 ? 8 : d <= 35 ? 6 : d <= 50 ? 4 : d <= 65 ? 2 : 0;
    })();

    const cyclicality = (() => {
        const tag = m.sector_cyc_tag?.toLowerCase();
        switch (tag) {
            case 'defensive': return 8;
            case 'secular': return 7;
            case 'growth': return 6;
            case 'cyclical': return 4;
            case 'deep-cyclical': return 3;
            default: return 6;
        }
    })();

    // Weighted: base_risk dominates; cyclicality is informative but not primary
    return weightedAvg(
        [base_risk, net_debt_ebitda, net_cash_mcap, beta, max_drawdown, cyclicality],
        [0.35, 0.20, 0.10, 0.15, 0.10, 0.10]
    );
}

// G/H) Macro & Narrative
const scoreMacro = (m) => nz(m.macrofit_score_0_10);
const scoreNarrative = (m) => nz(m.narrative_score_0_10);

// I) Dilution — CALIBRATED
//
// Change vs original:
//   OLD: cap10(10 + 2 * (sharecount_change - sbc_to_sales))
//   BUG: subtracts % of shares from % of revenue — dimensionally inconsistent.
//        A $200B-revenue company and a $1B startup both get the same 2% SBC penalty
//        despite utterly different share dilution impact.
//
//   NEW: independent lookup tables for each signal:
//     sub_change: positive value = fewer shares (net buyback), negative = dilution
//       ≥+5% → 10 | ≥+2% → 8 | ≥0% → 6 | ≥-2% → 4 | ≥-5% → 2 | <-5% → 0
//     sub_sbc: SBC as % of sales — lower = less compensation dilution
//       ≤1% → 10 | ≤2% → 8 | ≤4% → 6 | ≤6% → 4 | ≤10% → 2 | >10% → 0
//   Weighted: share count change 60% (primary), SBC ratio 40% (supporting signal)
function scoreDilution(m) {
    const change = toPoints(m.sharecount_change_5y_pct);
    const sbc = toPoints(m.sbc_to_sales_pct);

    // Positive = net reduction in shares (buyback > dilution), negative = net dilution
    const sub_change = change == null ? null
        : change >= 5 ? 10 : change >= 2 ? 8 : change >= 0 ? 6
        : change >= -2 ? 4 : change >= -5 ? 2 : 0;

    // SBC as % of sales — lower cost = less shareholder dilution from compensation
    const sub_sbc = sbc == null ? null
        : sbc <= 1 ? 10 : sbc <= 2 ? 8 : sbc <= 4 ? 6 : sbc <= 6 ? 4 : sbc <= 10 ? 2 : 0;

    return weightedAvg([sub_change, sub_sbc], [0.60, 0.40]);
}


// Null scores are returned as null — NOT coerced to 0.
// computeFinalScore excludes null categories from the weighted average
// (they don't penalise the stock, weight is redistributed to present categories).
// This mirrors the backend Phase-4 policy exactly.
export function computeCategoryScores(m) {
  return {
    valuation:        scoreValuation(m),
    quality:          scoreQuality(m),
    capitalAllocation: scoreCapitalAllocation(m),
    growth:           scoreGrowth(m),
    moat:             scoreMoat(m),
    risk:             scoreRisk(m),
    macro:            scoreMacro(m),
    narrative:        scoreNarrative(m),
    dilution:         scoreDilution(m),
  };
}

export function computeFinalScore(categoryScores, lens) {
    if (!categoryScores || !lens) return 0;

    const weights = {
        valuation: lens.valuation,
        quality: lens.quality,
        capitalAllocation: lens.capitalAllocation,
        growth: lens.growth,
        moat: lens.moat,
        risk: lens.risk,
        macro: lens.macro,
        narrative: lens.narrative,
        dilution: lens.dilution
    };

    let totalScore = 0;
    let totalWeight = 0;

    for (const category in categoryScores) {
        if (typeof categoryScores[category] === 'number' && typeof weights[category] === 'number') {
            totalScore += categoryScores[category] * weights[category];
            totalWeight += weights[category];
        }
    }

    if (totalWeight === 0) return 0;

    return totalScore / totalWeight;
}
