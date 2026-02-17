import { nz, toPoints } from "./num";

const clamp01 = (x) => Math.max(0, Math.min(1, x));
const cap10 = (x) => Math.max(0, Math.min(10, x));

function safeAvg(vals) {
    const a = vals.filter((v) => typeof v === 'number' && !Number.isNaN(v));
    return a.length ? a.reduce((s, v) => s + v, 0) / a.length : null;
}

// A) Valuation (weighted average)
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

    const weights = [0.35, 0.20, 0.15, 0.15, 0.15];
    const vals = [sub_pe, sub_peg, sub_ev, sub_fcfy, sub_hist];
    let num = 0, den = 0;
    vals.forEach((v, i) => { if (v != null) { num += v * weights[i]; den += weights[i]; } });
    return den ? num / den : null;
}

// ... (rest of the scoring functions remain the same, just ensure they skip nulls) ...
// B) Quality (simple average)
function scoreQuality(m) {
    const roic = cap10(toPoints(m.roic_pct) / 2);
    const fcf_margin = cap10(toPoints(m.fcf_margin_pct) / 1.5);
    const cash_conversion = cap10(10 * safeAvg([nz(m.cfo_to_ni), nz(m.fcf_to_ebit)]));
    const accruals = m.accruals_ratio == null ? 10 : 10 * clamp01((0.10 - Math.abs(nz(m.accruals_ratio))) / 0.10);
    const margin_stability = 10 - cap10(toPoints(m.margin_stdev_5y_pct) * 0.5);
    return safeAvg([roic, fcf_margin, cash_conversion, accruals, margin_stability]);
}

// C) Capital Allocation (simple average)
function scoreCapitalAllocation(m) {
    const buyback = cap10((toPoints(m.buyback_yield_pct) + 2) / 2);
    const interest_cover = cap10(Math.log10(nz(m.interest_coverage_x) + 1) * 4);
    const roiic = (() => {
        const ebit_t = nz(m.ebit_t);
        const ebit_t3 = nz(m.ebit_t3);
        const invcap_t = nz(m.invcap_t);
        const invcap_t3 = nz(m.invcap_t3);
        if (ebit_t == null || ebit_t3 == null || invcap_t == null || invcap_t3 == null || invcap_t === invcap_t3) return null;
        const roiic_proxy = (ebit_t - ebit_t3) / (invcap_t - invcap_t3);
        return cap10(roiic_proxy * 100 / 2);
    })();
    return safeAvg([buyback, interest_cover, roiic]);
}

// D) Growth (simple average)
function scoreGrowth(m) {
    const eps5y = cap10(toPoints(m.eps_cagr_5y_pct) / 2);
    const rev5y = cap10(toPoints(m.rev_cagr_5y_pct) / 2);
    const acceleration = (() => {
        const acc = (toPoints(m.eps_cagr_3y_pct) - toPoints(m.eps_cagr_5y_pct)) + (toPoints(m.rev_cagr_3y_pct) - toPoints(m.rev_cagr_5y_pct));
        if (isNaN(acc)) return null;
        return cap10(5 + 0.5 * acc);
    })();
    const stage_tag = (() => {
        const r = toPoints(m.rev_cagr_5y_pct);
        if (r == null) return null;
        if (r >= 25) return 10;
        if (r >= 15) return 8;
        if (r >= 5) return 6;
        return 3;
    })();
    return safeAvg([eps5y, rev5y, acceleration, stage_tag]);
}

// E) Moat (simple average)
function scoreMoat(m) {
    const base_moat = nz(m.moat_score_0_10);
    const recurring = 10 * clamp01(toPoints(m.recurring_revenue_pct) / 100);
    const owner_block = Math.min(2, 10 * clamp01(toPoints(m.insider_own_pct) / 100)) + (m.founder_led_bool ? 1 : 0);
    return safeAvg([base_moat, recurring, owner_block]);
}

// F) Risk/Downside (simple average)
function scoreRisk(m) {
    const base_risk = nz(m.riskdownside_score_0_10);
    const net_debt_ebitda = 10 * clamp01((3 - nz(m.netdebt_to_ebitda)) / 2);
    const net_cash_mcap = cap10(5 + Math.max(0, toPoints(m.netcash_to_mktcap_pct)) / 2);
    const beta = 10 - cap10(Math.abs(nz(m.beta_5y)) * 5);
    const max_drawdown = 10 - cap10(Math.abs(toPoints(m.max_drawdown_5y_pct)));
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
    return safeAvg([base_risk, net_debt_ebitda, net_cash_mcap, beta, max_drawdown, cyclicality]);
}

// G/H) Macro & Narrative
const scoreMacro = (m) => nz(m.macrofit_score_0_10);
const scoreNarrative = (m) => nz(m.narrative_score_0_10);

// I) Dilution
function scoreDilution(m) {
    const change = toPoints(m.sharecount_change_5y_pct);
    const sbc = toPoints(m.sbc_to_sales_pct);
    if (change == null || sbc == null) return null;
    return cap10(10 + 2 * (change - sbc));
}


export function computeCategoryScores(m) {
  return {
    valuation: scoreValuation(m) ?? 0,
    quality: scoreQuality(m) ?? 0,
    capitalAllocation: scoreCapitalAllocation(m) ?? 0,
    growth: scoreGrowth(m) ?? 0,
    moat: scoreMoat(m) ?? 0,
    risk: scoreRisk(m) ?? 0,
    macro: scoreMacro(m) ?? 0,
    narrative: scoreNarrative(m) ?? 0,
    dilution: scoreDilution(m) ?? 0,
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