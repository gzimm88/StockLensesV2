async function apiFetch(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`API error ${response.status} for ${path}`);
  }
  return response.json();
}

function buildQuery(params = {}) {
  const q = Object.entries(params)
    .filter(([, v]) => v !== undefined && v !== null)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&');
  return q ? `?${q}` : '';
}

export const Ticker = {
  list: ({ limit } = {}) => apiFetch(`/api/tickers${buildQuery({ limit })}`),
};

export const Metrics = {
  list: ({ limit } = {}) => apiFetch(`/api/metrics${buildQuery({ limit })}`),
};

export const FinancialsHistory = {
  list: ({ limit } = {}) => apiFetch(`/api/financials-history${buildQuery({ limit })}`),
};

export const PricesHistory = {
  list: ({ limit } = {}) => apiFetch(`/api/prices-history${buildQuery({ limit })}`),
};

export const LensPreset = {
  list: ({ limit } = {}) => apiFetch(`/api/lens-presets${buildQuery({ limit })}`),
};

export const User = {};
