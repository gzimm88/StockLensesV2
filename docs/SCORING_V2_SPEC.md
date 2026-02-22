# Scoring v2 Specification (Draft)

## 1) Purpose

Define a deterministic, auditable scoring system for stock ranking that is:

- Stable across refreshes
- Explicit about units and null behavior
- Resistant to outliers and bad upstream fields
- Traceable from raw inputs -> category subscores -> final score -> recommendation

This spec is implementation-facing and should be treated as the single source of truth.

---

## 2) Scope

This spec covers:

- Input data contract (metrics fields, units, allowed ranges)
- Normalization and validation rules
- Category score formulas
- Final score aggregation
- Recommendation thresholds and gates
- Audit outputs, explainability, and test cases

This spec does not define:

- Portfolio sizing or trade execution
- Position management rules
- Intraday timing logic

---

## 3) Canonical Input Contract

All scoring inputs must come from a single `metrics` row per ticker (`metrics.ticker_symbol`), with explicit units.

### 3.1 Unit conventions

- Percent-in-points fields: `18.5` means `18.5%`
- Ratio fields: `1.25` means `1.25x`
- Multiples: `22.3` means `22.3x`
- Currency-like values are absolute numeric values (same base currency per ticker)

### 3.2 Canonical field names (required by scorer)

- Valuation: `pe_fwd`, `ev_ebitda`, `fcf_yield_pct`, `peg_5y`, `pe_ttm`, `pe_5y_low`, `pe_5y_high`
- Quality: `roic_pct`, `fcf_margin_pct`, `cfo_to_ni`, `fcf_to_ebit`, `accruals_ratio`, `margin_stdev_5y_pct`
- Capital allocation: `buyback_yield_pct`, `interest_coverage_x`
- Growth: `eps_cagr_5y_pct`, `eps_cagr_3y_pct`, `revenue_cagr_5y_pct`, `revenue_cagr_3y_pct`
- Moat: `moat_score_0_10`, `recurring_revenue_pct`, `insider_own_pct`, `founder_led_bool`
- Risk: `riskdownside_score_0_10`, `netdebt_to_ebitda`, `netcash_to_mktcap_pct`, `beta_5y`, `maxdrawdown_5y_pct`, `sector_cyc_tag`
- Dilution: `sharecount_change_5y_pct`, `sbc_to_sales_pct`

### 3.3 Backward compatibility aliases

If old keys are present, map before scoring:

- `rev_cagr_5y_pct` -> `revenue_cagr_5y_pct`
- `rev_cagr_3y_pct` -> `revenue_cagr_3y_pct`

Do not mix old and new keys directly inside formulas.

---

## 4) Data Quality and Pre-Score Validation

### 4.1 Null policy

- Missing value is `null`, never coerced to `0`.
- Subscores with missing required inputs are omitted from category average.
- Category score is null if all subscores are null.
- Final category value for ranking may fallback to `0` only at final aggregation layer (explicitly flagged as imputed).

### 4.2 Numeric sanity bounds

Hard validity checks (invalid -> null):

- Multiples: `< 0` invalid
- Percent points: outside `[-500, 500]` invalid unless explicitly whitelisted
- Ratios: non-finite invalid

### 4.3 Outlier controls (winsorization)

Apply caps before scoring:

- `pe_fwd`: cap to `[1, 120]`
- `ev_ebitda`: cap to `[1, 80]`
- `peg_5y`: cap to `[0, 20]`
- `fcf_yield_pct`: cap to `[-20, 30]`
- `eps/revenue CAGR`: cap to `[-50, 80]`
- `maxdrawdown_5y_pct`: cap to `[0, 95]`

---

## 5) Category Formulas (0 to 10)

Use:

- `cap10(x) = min(10, max(0, x))`
- `safeAvg(valid_values)` over non-null values only

### 5.1 Valuation

Subscores:

- `sub_pe`: step function on `pe_fwd`
- `sub_ev`: step function on `ev_ebitda`
- `sub_fcfy`: step function on `fcf_yield_pct`
- `sub_peg`: step function on `peg_5y`
- `sub_hist = 10 * clamp01((pe_5y_high - pe_ttm) / (pe_5y_high - pe_5y_low))`

Weights:

- `PE 0.35`, `PEG 0.20`, `EV/EBITDA 0.15`, `FCF Yield 0.15`, `Hist PE Position 0.15`

Category score:

- Weighted average across available valuation subscores; renormalize by available weight sum.

### 5.2 Quality

- `roic = cap10(roic_pct / 2)`
- `fcf_margin = cap10(fcf_margin_pct / 1.5)`
- `cash_conversion = cap10(10 * avg(cfo_to_ni, fcf_to_ebit))`
- `accruals = 10 * clamp01((0.10 - abs(accruals_ratio)) / 0.10)`; if missing -> omit
- `margin_stability = 10 - cap10(margin_stdev_5y_pct * 0.5)`

Category score:

- `safeAvg([roic, fcf_margin, cash_conversion, accruals, margin_stability])`

### 5.3 Capital Allocation

- `buyback = cap10((buyback_yield_pct + 2) / 2)`
- `interest_cover = cap10(log10(interest_coverage_x + 1) * 4)`
- Optional ROIIC proxy (if implemented): `cap10(roiic_pct / 2)`; omit if unavailable

Category score:

- `safeAvg(available_subscores)`

### 5.4 Growth

- `eps5y = cap10(eps_cagr_5y_pct / 2)` if present
- `rev5y = cap10(revenue_cagr_5y_pct / 2)` if present
- `acceleration` only if all 4 are present:
  - `acc = (eps_3y - eps_5y) + (rev_3y - rev_5y)`
  - `acceleration = cap10(5 + 0.5 * acc)`
- `stage_tag` from `revenue_cagr_5y_pct`:
  - `>=25 -> 10`
  - `>=15 -> 8`
  - `>=5 -> 6`
  - else `3`

Category score:

- `safeAvg([eps5y, rev5y, acceleration, stage_tag])`

### 5.5 Moat

- `base = moat_score_0_10`
- `recurring = 10 * clamp01(recurring_revenue_pct / 100)`
- `owner_block = min(2, 10 * clamp01(insider_own_pct / 100)) + (founder_led_bool ? 1 : 0)`

Category score:

- `safeAvg([base, recurring, owner_block])`

### 5.6 Risk

- `base = riskdownside_score_0_10`
- `net_debt = 10 * clamp01((3 - netdebt_to_ebitda) / 2)`
- `net_cash = cap10(5 + max(0, netcash_to_mktcap_pct) / 2)`
- `beta = 10 - cap10(abs(beta_5y) * 5)`
- `drawdown = 10 - cap10(abs(maxdrawdown_5y_pct))`
- `cyclicality_tag` mapping:
  - defensive `8`, secular `7`, growth `6`, cyclical `4`, deep-cyclical `3`, default `6`

Category score:

- `safeAvg([base, net_debt, net_cash, beta, drawdown, cyclicality_tag])`

### 5.7 Macro and Narrative

- `macro = macrofit_score_0_10`
- `narrative = narrative_score_0_10`

### 5.8 Dilution

- Requires both `sharecount_change_5y_pct` and `sbc_to_sales_pct`
- `dilution = cap10(10 + 2 * (sharecount_change_5y_pct - sbc_to_sales_pct))`

---

## 6) Final Score

Given category scores and lens weights:

- `final = sum(category_score_i * weight_i) / sum(weight_i for available categories)`
- If all categories missing: `0`

Lens weights are sourced from `lens_presets` entity fields:

- `valuation`, `quality`, `capitalAllocation`, `growth`, `moat`, `risk`, `macro`, `narrative`, `dilution`

---

## 7) Recommendation Layer

Threshold gates (current policy):

- `BUY` if:
  - `final_score >= buy_threshold`
  - `MOS >= mos_threshold` (if MOS gate enabled)
  - `confidence >= conf_threshold` (if confidence gate enabled)
- `WATCH` if:
  - `final_score >= watch_threshold` and BUY gate fails
- else `AVOID`

Current defaults:

- `Buy >= 6.5`
- `Watch >= 4.5`
- `MOS >= 60%`
- `Confidence >= 20%`

---

## 8) Confidence Definition

Confidence is data completeness over a fixed required field set:

- `confidence_pct = present_numeric_required_fields / total_required_fields * 100`

Required fields must be explicitly versioned with scorer version.

---

## 9) Explainability and Trace Output

For each ticker score run, persist or emit:

- `scoring_version`
- Raw normalized inputs used
- Subscore components per category
- Omitted components and omission reason
- Category scores
- Final weighted contribution by category
- Recommendation gate results (`score_gate`, `mos_gate`, `conf_gate`)

Minimum debug payload:

- `growth_trace`: `eps5y`, `rev5y`, `acceleration`, `stage_tag`, `final_growth`
- `valuation_trace`: each subscore + effective weight denominator

---

## 10) Acceptance Tests

### 10.1 Missing-data correctness

- If `eps_cagr_5y_pct=null` and `revenue_cagr_5y_pct=42.5`, growth must not include `eps5y=0`.
- Growth must average only available components.

### 10.2 Key alias correctness

- `revenue_cagr_*` and legacy `rev_cagr_*` produce same score.

### 10.3 Determinism

- Same input row must always produce identical score outputs.

### 10.4 Guardrails

- Non-finite values never enter subscores.
- Out-of-bound values are clipped per winsorization rules.

---

## 11) Versioning and Change Control

- Add `scoring_version` string in frontend and backend traces, e.g. `v2.0.0`.
- Any formula or threshold change increments minor version.
- Any field contract change increments major version.

---

## 12) Recommended Immediate Refactors

1. Single threshold source of truth:
- Move recommendation thresholds to `lens_presets` DB, remove static fallback drift.

2. Single MOS source of truth:
- Persist MOS to DB (`metrics` or dedicated table), avoid in-memory-only cache.

3. Field contract lock:
- Central typed schema for scorer input with alias mapping and unit normalization.

4. Scoring trace panel:
- Expose per-category subscore traces in UI for every ticker row.

