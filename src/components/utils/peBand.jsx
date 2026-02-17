export function cleanBand(peLow, peHigh, peTTM) {
  const low = isFinite(peLow) ? Number(peLow) : null;
  const high = isFinite(peHigh) ? Number(peHigh) : null;
  const ttm = isFinite(peTTM) ? Number(peTTM) : null;

  if (low != null && (low <= 0 || low > 200)) return {low:null, high:null, ttm};
  if (high != null && (high <= 0 || high > 400)) return {low:null, high:null, ttm};
  if (low != null && high != null && low > high) return {low:null, high:null, ttm};

  return {low, high, ttm};
}

export function bandMid(low, high) {
  return (low!=null && high!=null) ? (low + high)/2 : null;
}