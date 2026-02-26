
/**
 * Recommendation with backward-compatible signatures:
 *   recommend(score, config)
 *   recommend(score, mos, config)
 */
export function recommend(score, arg2 = {}, arg3 = {}) {
  const hasMosArg = typeof arg2 === "number" || arg2 == null;
  const mos = hasMosArg ? arg2 : null;
  const config = hasMosArg ? (arg3 || {}) : (arg2 || {});

  const buyMin = typeof config.buy === "number" ? config.buy : 6.5;
  const watchMin = typeof config.watch === "number" ? config.watch : 4.5;
  const mosRequired = typeof config.mos === "number" ? config.mos : 0;
  const confidence = typeof config.confidence === "number" ? config.confidence : null;
  const confRequired = typeof config.conf === "number" ? config.conf : 0;

  if (score == null || !isFinite(score)) {
    return { rec: "INSUFFICIENT_DATA", mosStatus: null, confStatus: null };
  }

  const hasMOSReq = mosRequired > 0;
  const hasConfReq = confRequired > 0;
  const hasMOSVal = mos != null && isFinite(mos);
  const hasConfVal = confidence != null && isFinite(confidence);
  const confPass = !hasConfReq || (hasConfVal && confidence >= confRequired);
  const confStatus = hasConfReq
    ? (hasConfVal ? (confidence >= confRequired ? "✓" : "✕") : "-")
    : null;

  // Recommendation is score-only. MOS and confidence are audit/display signals.
  const rec = score >= buyMin ? "BUY" : score >= watchMin ? "WATCH" : "AVOID";

  let mosStatus = null;
  if (hasMOSReq) {
    if (!hasMOSVal) mosStatus = "-";
    else mosStatus = mos >= mosRequired ? "✓" : "✕";
  }

  return { rec, mosStatus, confStatus };
}

/**
 * MOS display signal — mirrors backend compute_mos_signal.
 */
export function mosSignal(mos, neutralBand = 0.05) {
  if (mos == null || !isFinite(mos)) return null;
  if (mos > neutralBand) return "+";
  if (mos < -neutralBand) return "-";
  return "0";
}
