/**
 * Recommendation — score-only, no MOS or confidence gating.
 *
 * Policy:
 *   BUY   if score >= buyMin
 *   WATCH if score >= watchMin
 *   AVOID otherwise
 *   null / non-finite score → INSUFFICIENT_DATA
 *
 * MOS and confidence are display-only; they never affect the rec.
 */
export function recommend(score, config = {}) {
  const buyMin   = typeof config.buy   === "number" ? config.buy   : 6.5;
  const watchMin = typeof config.watch === "number" ? config.watch : 4.5;

  if (score == null || !isFinite(score)) return { rec: "INSUFFICIENT_DATA" };
  if (score >= buyMin)   return { rec: "BUY" };
  if (score >= watchMin) return { rec: "WATCH" };
  return { rec: "AVOID" };
}

/**
 * MOS display signal — mirrors backend compute_mos_signal.
 * mos is the fractional value cached by getMos (e.g. 0.15 = 15 %).
 * neutralBand is in the same fractional units (default 0.05 = ±5 %).
 *
 * Returns "+" / "0" / "-" / null (null when mos is unavailable).
 */
export function mosSignal(mos, neutralBand = 0.05) {
  if (mos == null || !isFinite(mos)) return null;
  if (mos >  neutralBand) return "+";
  if (mos < -neutralBand) return "-";
  return "0";
}
