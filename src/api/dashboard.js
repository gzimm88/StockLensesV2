/**
 * dashboard.js
 *
 * API client for the Control Center dashboard endpoints.
 * Follows the same apiFetch pattern from entities.js.
 */

const BASE = "/api";

async function apiFetch(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", ...options.headers },
    ...options,
  });
  if (res.status === 204) return null;
  const data = await res.json();
  if (!res.ok) {
    const msg = data?.detail ?? `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

/** Market session status for US, Europe, Asia-Pacific (no auth required). */
export function getMarketSessions() {
  return apiFetch("/dashboard/market-sessions");
}

/** Watchlist with entry price gaps (auth required). */
export function getWatchlistSummary() {
  return apiFetch("/dashboard/watchlist-summary");
}

/** Composite overview: portfolio summary, top movers, recent activity (auth required). */
export function getDashboardOverview() {
  return apiFetch("/dashboard/overview");
}

/** Market indices: SPY, QQQ, DIA, EUR/USD (no auth required). */
export function getMarketIndices() {
  return apiFetch("/dashboard/market-indices");
}

/** Auto-onboard index ETFs if not already present (fire-and-forget). */
export function ensureIndices() {
  return apiFetch("/dashboard/ensure-indices", { method: "POST" });
}
