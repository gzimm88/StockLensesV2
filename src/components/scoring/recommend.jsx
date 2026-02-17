export function recommend(score, mos, mosRequired) {
  const hasMOSReq = !!mosRequired && mosRequired > 0;
  const hasMOSVal = mos != null && isFinite(mos);

  // Use a default lens if thresholds are not provided, for safety
  const buyMin = 8.0;
  const watchMin = 6.5;

  if (!hasMOSReq) {
    if (score >= buyMin) return { rec: 'BUY', mosStatus: null };
    if (score >= watchMin) return { rec: 'WATCH', mosStatus: null };
    return { rec: 'AVOID', mosStatus: null };
  }

  // MOS required lens
  if (hasMOSVal) {
    if (score >= buyMin && mos >= mosRequired) return { rec: 'BUY', mosStatus: '✓' };
    if (score >= watchMin) return { rec: 'WATCH', mosStatus: mos < mosRequired ? '✕' : '✓' };
    return { rec: 'AVOID', mosStatus: '✕' };
  } else {
    // MOS missing: fall back to score-only but tag as "MOS unknown"
    if (score >= buyMin) return { rec: 'WATCH', mosStatus: '-' }; // conservative fallback
    if (score >= watchMin) return { rec: 'WATCH', mosStatus: '-' };
    return { rec: 'AVOID', mosStatus: '-' };
  }
}