/**
 * entities.js
 *
 * Thin fetch wrappers that mirror the Base44 entity API surface
 * (list / filter / create / update / delete) but call our FastAPI
 * backend at http://localhost:8000 instead of the cloud platform.
 *
 * Every method returns a plain JS object / array (already parsed JSON).
 * Errors bubble up as thrown Error instances so callers can catch them.
 */

const BASE = "http://localhost:8000";

async function apiFetch(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...options.headers },
    ...options,
  });
  if (res.status === 204) return null; // DELETE → no body
  const data = await res.json();
  if (!res.ok) {
    const msg = data?.detail ?? `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

// ---------------------------------------------------------------------------
// Ticker
// ---------------------------------------------------------------------------

export const Ticker = {
  /** Return all tickers (up to 500). */
  list: () => apiFetch("/tickers"),

  /**
   * Filter tickers by field values.
   * Supported keys: symbol
   * e.g. Ticker.filter({ symbol: "AAPL" })
   */
  filter: (params = {}) => {
    const qs = new URLSearchParams(
      Object.fromEntries(Object.entries(params).filter(([, v]) => v != null))
    ).toString();
    return apiFetch(`/tickers/filter${qs ? `?${qs}` : ""}`);
  },

  /** Create a new ticker row. */
  create: (data) =>
    apiFetch("/tickers", { method: "POST", body: JSON.stringify(data) }),

  /** Partial-update a ticker by id. */
  update: (id, data) =>
    apiFetch(`/tickers/${encodeURIComponent(id)}`, {
      method: "PATCH",
      body: JSON.stringify(data),
    }),
};

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

export const Metrics = {
  /** Return all metrics rows (up to 500). */
  list: () => apiFetch("/metrics"),

  /**
   * Filter metrics by field values.
   * Supported keys: ticker_symbol
   * e.g. Metrics.filter({ ticker_symbol: "AAPL" })
   */
  filter: (params = {}) => {
    const qs = new URLSearchParams(
      Object.fromEntries(Object.entries(params).filter(([, v]) => v != null))
    ).toString();
    return apiFetch(`/metrics/filter${qs ? `?${qs}` : ""}`);
  },

  /** Create a new metrics row. */
  create: (data) =>
    apiFetch("/metrics", { method: "POST", body: JSON.stringify(data) }),

  /** Partial-update a metrics row by id. */
  update: (id, data) =>
    apiFetch(`/metrics/${encodeURIComponent(id)}`, {
      method: "PATCH",
      body: JSON.stringify(data),
    }),
};

// ---------------------------------------------------------------------------
// LensPreset
// ---------------------------------------------------------------------------

export const LensPreset = {
  /** Return all lens presets. */
  list: () => apiFetch("/lens-presets"),

  /** Create a new lens preset (id must be supplied in data). */
  create: (data) =>
    apiFetch("/lens-presets", { method: "POST", body: JSON.stringify(data) }),

  /** Partial-update a lens preset by id. */
  update: (id, data) =>
    apiFetch(`/lens-presets/${encodeURIComponent(id)}`, {
      method: "PATCH",
      body: JSON.stringify(data),
    }),

  /** Delete a lens preset by id. */
  delete: (id) =>
    apiFetch(`/lens-presets/${encodeURIComponent(id)}`, { method: "DELETE" }),
};

// ---------------------------------------------------------------------------
// User  (stub – not backed by the local API)
// ---------------------------------------------------------------------------

export const User = {
  me: () => apiFetch("/auth/me"),
  login: (data) => apiFetch("/auth/login", { method: "POST", body: JSON.stringify(data) }),
  logout: () => apiFetch("/auth/logout", { method: "POST" }),
  list: () => apiFetch("/admin/users"),
  create: (data) => apiFetch("/admin/users", { method: "POST", body: JSON.stringify(data) }),
  update: (id, data) =>
    apiFetch(`/admin/users/${encodeURIComponent(id)}`, {
      method: "PATCH",
      body: JSON.stringify(data),
    }),
  resetPassword: (id, data) =>
    apiFetch(`/admin/users/${encodeURIComponent(id)}/reset-password`, {
      method: "POST",
      body: JSON.stringify(data),
    }),
};

// ---------------------------------------------------------------------------
// ProjectionAssumption
// ---------------------------------------------------------------------------

export const ProjectionAssumption = {
  list: () => apiFetch("/projection-assumptions"),
  upsert: (data) =>
    apiFetch("/projection-assumptions", {
      method: "POST",
      body: JSON.stringify(data),
    }),
};

// ---------------------------------------------------------------------------
// ProjectionSnapshot — frozen snapshots with BUY/SELL price triggers
// ---------------------------------------------------------------------------

function _qs(params = {}) {
  const entries = Object.entries(params).filter(([, v]) => v != null && v !== "");
  if (!entries.length) return "";
  return "?" + new URLSearchParams(entries.map(([k, v]) => [k, String(v)])).toString();
}

export const ProjectionSnapshot = {
  list: (params = {}) => apiFetch(`/projections/snapshots${_qs(params)}`),
  get: (id) => apiFetch(`/projections/snapshots/${encodeURIComponent(id)}`),
  create: (data) =>
    apiFetch("/projections/snapshots", {
      method: "POST",
      body: JSON.stringify(data),
    }),
  update: (id, data) =>
    apiFetch(`/projections/snapshots/${encodeURIComponent(id)}`, {
      method: "PATCH",
      body: JSON.stringify(data),
    }),
  delete: (id) =>
    apiFetch(`/projections/snapshots/${encodeURIComponent(id)}`, {
      method: "DELETE",
    }),
};

// ---------------------------------------------------------------------------
// AlertNotification — triggered alert events for the Notification Center
// ---------------------------------------------------------------------------

export const AlertNotification = {
  list: (params = {}) => apiFetch(`/projections/alerts${_qs(params)}`),
  dismiss: (id) =>
    apiFetch(`/projections/alerts/${encodeURIComponent(id)}/dismiss`, {
      method: "POST",
    }),
  markRead: (id) =>
    apiFetch(`/projections/alerts/${encodeURIComponent(id)}/read`, {
      method: "POST",
    }),
  markAllRead: () =>
    apiFetch("/projections/alerts/read-all", { method: "POST" }),
  delete: (id) =>
    apiFetch(`/projections/alerts/${encodeURIComponent(id)}`, {
      method: "DELETE",
    }),
};

// ---------------------------------------------------------------------------
// UserEmail — for notification destination
// ---------------------------------------------------------------------------

export const UserEmail = {
  get: () => apiFetch("/me/email"),
  update: (email) =>
    apiFetch("/me/email", {
      method: "PATCH",
      body: JSON.stringify({ email }),
    }),
};

// ---------------------------------------------------------------------------
// Watchlist — unified CRUD + lifecycle actions
// ---------------------------------------------------------------------------

export const Watchlist = {
  /** Returns the enriched watchlist summary with prices, entry, status, source. */
  summary: () => apiFetch("/dashboard/watchlist-summary"),
  remove: (ticker) =>
    apiFetch(`/watchlist/${encodeURIComponent(ticker)}`, { method: "DELETE" }),
  update: (ticker, data) =>
    apiFetch(`/watchlist/${encodeURIComponent(ticker)}`, {
      method: "PATCH",
      body: JSON.stringify(data),
    }),
  action: (ticker, data) =>
    apiFetch(`/watchlist/${encodeURIComponent(ticker)}/action`, {
      method: "POST",
      body: JSON.stringify(data),
    }),
};

// ---------------------------------------------------------------------------
// ScreenerPreference
// ---------------------------------------------------------------------------

export const ScreenerPreference = {
  list: () => apiFetch("/screener/preferences"),
  upsert: (data) =>
    apiFetch("/screener/preferences", {
      method: "POST",
      body: JSON.stringify(data),
    }),
};

// ---------------------------------------------------------------------------
// MarketData
// ---------------------------------------------------------------------------

export const MarketData = {
  refreshLatest: (data = {}) =>
    apiFetch("/market-data/refresh", {
      method: "POST",
      body: JSON.stringify(data),
    }),
};
