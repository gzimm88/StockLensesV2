// Simple MOS cache - in a real app this would be a proper store
let mosCache = {};

export function cacheLatestMosForTicker(ticker, mos) {
  mosCache[ticker] = mos;
}

export function getLatestMosForTicker(ticker) {
  return mosCache[ticker] ?? null;
}

export function clearMosCache() {
  mosCache = {};
}