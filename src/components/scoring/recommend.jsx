
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

export function recommend(score, mos, config = {}) {
  const buyMin = typeof config.buy === "number" ? config.buy : 6.5;
  const watchMin = typeof config.watch === "number" ? config.watch : 4.5;
  const mosRequired = typeof config.mos === "number" ? config.mos : 0;
  const confidence = typeof config.confidence === "number" ? config.confidence : null;
  const confRequired = typeof config.conf === "number" ? config.conf : 0;

  const hasMOSReq = !!mosRequired && mosRequired > 0;
  const hasConfReq = confRequired > 0;
  const hasMOSVal = mos != null && isFinite(mos);
  const hasConfVal = confidence != null && isFinite(confidence);
  const confPass = !hasConfReq || (hasConfVal && confidence >= confRequired);
  const confStatus = hasConfReq ? (hasConfVal ? (confidence >= confRequired ? "✓" : "✕") : "-") : null;

  if (!hasMOSReq && !hasConfReq) {
    if (score >= buyMin) return { rec: 'BUY', mosStatus: null, confStatus: null };
    if (score >= watchMin) return { rec: 'WATCH', mosStatus: null, confStatus: null };
    return { rec: 'AVOID', mosStatus: null, confStatus: null };
  }

  // MOS and/or Confidence required lens
  if (hasMOSVal) {
    const mosPass = mos >= mosRequired;
    if (score >= buyMin && mosPass && confPass) return { rec: 'BUY', mosStatus: hasMOSReq ? '✓' : null, confStatus };
    if (score >= watchMin) return { rec: 'WATCH', mosStatus: hasMOSReq ? (mosPass ? '✓' : '✕') : null, confStatus };
    return { rec: 'AVOID', mosStatus: hasMOSReq ? '✕' : null, confStatus };
  } else {
    // MOS missing: fall back to score-only but tag as unknown
    if (score >= buyMin) return { rec: 'WATCH', mosStatus: hasMOSReq ? '-' : null, confStatus }; // conservative fallback
    if (score >= watchMin) return { rec: 'WATCH', mosStatus: hasMOSReq ? '-' : null, confStatus };
    return { rec: 'AVOID', mosStatus: hasMOSReq ? '-' : null, confStatus };
  }

}
