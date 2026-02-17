export const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
export const clamp01 = (x) => clamp(x, 0, 1);
export const nz = (x) => (x === null || x === undefined || Number.isNaN(+x)) ? null : +x;

// treat strings like "21%", "0.21" or 21 -> return point-units (21)
export const toPoints = (v) => {
  if (v === null || v === undefined || v === '') return null;
  const s = ('' + v).trim();
  if (s.endsWith('%')) return +s.slice(0, -1);
  const n = +s;
  if (Number.isNaN(n)) return null;
  // decimals between -1..1 are % decimals; convert to points
  if (n > -1 && n < 1 && n !== 0) return n * 100;
  return n;
};