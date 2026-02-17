export function normalizeSymbol(s) {
  return (s || '').toString().trim().toUpperCase();
}

export function normalizeExchange(e) {
  return (e || '').toString().trim().toUpperCase();
}