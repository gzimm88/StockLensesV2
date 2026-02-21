// Projection and Entry Calculation utilities

export const annualized = (start, end, years) => Math.pow(end / start, 1 / years) - 1;

export const peCurrentTrend = (mid, trend, N, lo, hi) => {
  const clamp = (x, low, high) => Math.max(low, Math.min(high, x));
  return clamp(mid * Math.pow(1 + trend, N), lo, hi);
};

export function epsN(EPS0, g, N) {
  return EPS0 * Math.pow(1 + g, N);
}

export function priceN(EPSN, PE) {
  return EPSN * PE;
}

export function cagrFrom(priceN, priceToday, N) {
  return Math.pow(priceN / priceToday, 1 / N) - 1;
}

export function entryRequired(priceN, target, N) {
  return priceN / Math.pow(1 + target, N);
}

// Generate price path for charting
export function generatePricePath(priceToday, cagr, years) {
  const path = [];
  for (let i = 0; i <= years; i++) {
    path.push({
      year: i,
      price: priceToday * Math.pow(1 + cagr, i)
    });
  }
  return path;
}

// Generate target path for comparison
export function generateTargetPath(priceToday, targetCagr, years) {
  const path = [];
  for (let i = 0; i <= years; i++) {
    path.push({
      year: i,
      price: priceToday * Math.pow(1 + targetCagr, i)
    });
  }
  return path;
}

// NEW: Derive P/E band from metrics
export function deriveBandFromMetrics(m) {
  const lo = (typeof m?.pe_5y_low === 'number' && !isNaN(m.pe_5y_low)) ? m.pe_5y_low : null;
  const hi = (typeof m?.pe_5y_high === 'number' && !isNaN(m.pe_5y_high)) ? m.pe_5y_high : null;
  const mid = (lo != null && hi != null) ? (lo + hi) / 2 : null;
  return { bear: lo, mid, bull: hi };
}

// NEW: Clamp to available band edges
export function clampToBand(x, band) {
  let y = x;
  if (band.bear != null) y = Math.max(band.bear, y);
  if (band.bull != null) y = Math.min(band.bull, y);
  return y;
}

// Build comprehensive yearly paths for all scenarios
export function buildPaths(params) {
  const { EPS0, gPct, N, priceToday, PE_bear, PE_mid, PE_bull, PE_trendPct, PE_custom, targetCAGR, PE_now, band } = params;

  if (PE_now == null || !isFinite(PE_now) || N <= 0) {
    const emptyPaths = { bear: [], bull: [], constant: [], current: [], custom: [] };
    return { years: [], epsPath: [], pePaths: emptyPaths, pricePaths: () => [], terminal: () => ({}) };
  }

  const years = Array.from({ length: N + 1 }, (_, t) => t);
  const g = 1 + (gPct ?? 0) / 100;
  const peTrend = 1 + (PE_trendPct ?? 0) / 100;

  const epsPath = years.map(t => EPS0 * Math.pow(g, t));

  const customTerminalPE = (PE_custom != null && isFinite(PE_custom)) ? PE_custom : PE_mid;

  const pePaths = {
    bear:     years.map(t => PE_now + (PE_bear - PE_now) * (t / N)),
    bull:     years.map(t => PE_now + (PE_bull - PE_now) * (t / N)),
    constant: years.map(() => PE_now),
    current:  years.map(t => {
      const trendPE = PE_now * Math.pow(peTrend, t);
      return band ? clampToBand(trendPE, band) : trendPE;
    }),
    // Custom scenario now glides from Year-0 PE to user-provided terminal PE by Year-N.
    custom:   years.map(t => PE_now + (customTerminalPE - PE_now) * (t / N)),
  };

  const pricePaths = (key) => epsPath.map((eps, i) => (i === 0 ? priceToday : eps * pePaths[key][i]));

  const terminal = (key) => {
    const priceN = pricePaths(key)[N];
    const impliedCAGR = priceToday > 0 ? Math.pow(priceN / priceToday, 1 / N) - 1 : 0;
    const reqEntry = targetCAGR != null
      ? priceN / Math.pow(1 + targetCAGR / 100, N)
      : null;
    return { priceN, impliedCAGR, reqEntry };
  };

  return { years, epsPath, pePaths, pricePaths, terminal };
}
