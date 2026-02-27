const BASE = "/api";

async function apiFetch(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });

  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }

  if (!res.ok) {
    const msg = data?.detail ?? data?.message ?? `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

export function listPortfolios() {
  return apiFetch("/portfolios");
}

export function createPortfolio({ name, base_currency = "USD" }) {
  return apiFetch("/portfolios", {
    method: "POST",
    body: JSON.stringify({ name, base_currency }),
  });
}

export function deletePortfolio(portfolioId) {
  return apiFetch(`/portfolios/${encodeURIComponent(portfolioId)}`, { method: "DELETE" });
}

export function importPortfolioCsv(portfolioId, { replaceExisting = false } = {}) {
  const qs = replaceExisting ? "?replace_existing=true" : "";
  return apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/import-csv${qs}`, { method: "POST" });
}

export function processPortfolio(portfolioId, { strict = false } = {}) {
  const qs = strict ? "?strict=true" : "";
  return apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/process${qs}`, { method: "POST" });
}

export function rebuildPortfolioEquityHistory(
  portfolioId,
  { mode = "incremental", force = false, strict = false } = {}
) {
  const qs = new URLSearchParams({
    mode,
    force: force ? "true" : "false",
    strict: strict ? "true" : "false",
  });
  return apiFetch(`/portfolios/${encodeURIComponent(portfolioId)}/rebuild-equity-history?${qs.toString()}`, {
    method: "POST",
  });
}

export async function getLastPortfolioRun(portfolioId) {
  try {
    return await apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/last`, { method: "GET" });
  } catch (err) {
    const msg = String(err?.message || "");
    if (msg.includes("404") || msg.includes("No saved portfolio run found")) return null;
    throw err;
  }
}

export async function getLatestPortfolioRunMetadata(portfolioId) {
  try {
    return await apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/runs/latest`, { method: "GET" });
  } catch (err) {
    if (String(err?.message || "").includes("404")) return null;
    throw err;
  }
}

export async function getValuationAttribution(portfolioId) {
  try {
    return await apiFetch(`/portfolios/${encodeURIComponent(portfolioId)}/valuation-attribution`, { method: "GET" });
  } catch (err) {
    if (String(err?.message || "").includes("404")) return null;
    throw err;
  }
}

export async function getValuationDiff(portfolioId) {
  try {
    return await apiFetch(`/portfolios/${encodeURIComponent(portfolioId)}/valuation-diff`, { method: "GET" });
  } catch (err) {
    if (String(err?.message || "").includes("404")) return null;
    throw err;
  }
}

export async function getPortfolioDashboardSummary(portfolioId) {
  try {
    return await apiFetch(`/portfolios/${encodeURIComponent(portfolioId)}/dashboard-summary`, { method: "GET" });
  } catch (err) {
    const msg = String(err?.message || "");
    if (msg.includes("404") || msg.includes("No valuation snapshot found")) return null;
    throw err;
  }
}

export async function getPortfolioHoldings(portfolioId) {
  try {
    return await apiFetch(`/portfolios/${encodeURIComponent(portfolioId)}/holdings`, { method: "GET" });
  } catch (err) {
    const msg = String(err?.message || "");
    if (msg.includes("404") || msg.includes("No valuation snapshot found")) return null;
    throw err;
  }
}

export async function getPortfolioEquityHistory(portfolioId, range = "6M") {
  const q = encodeURIComponent(range || "6M");
  const defaultPerf = "absolute";
  try {
    return await apiFetch(
      `/portfolios/${encodeURIComponent(portfolioId)}/equity-history?range=${q}&performance_mode=${defaultPerf}&show_fx_impact=false`,
      { method: "GET" }
    );
  } catch (err) {
    const msg = String(err?.message || "");
    if (msg.includes("404") || msg.includes("No valuation snapshot found")) return null;
    throw err;
  }
}

export async function getPortfolioEquityHistorySeries(
  portfolioId,
  { range = "6M", performanceMode = "absolute", showFxImpact = false } = {}
) {
  const q = encodeURIComponent(range || "6M");
  const perf = encodeURIComponent(performanceMode || "absolute");
  const showFx = showFxImpact ? "true" : "false";
  return apiFetch(
    `/portfolios/${encodeURIComponent(portfolioId)}/equity-history?range=${q}&performance_mode=${perf}&show_fx_impact=${showFx}`,
    { method: "GET" }
  );
}

export async function getPortfolioSettings(portfolioId) {
  return apiFetch(`/portfolios/${encodeURIComponent(portfolioId)}/settings`, { method: "GET" });
}

export async function updatePortfolioSettings(portfolioId, payload) {
  return apiFetch(`/portfolios/${encodeURIComponent(portfolioId)}/settings`, {
    method: "PUT",
    body: JSON.stringify(payload),
  });
}

export function listPortfolioTransactions(portfolioId) {
  const normalize = (res) => {
    const raw = res?.data?.transactions || [];
    const mapped = raw.map((tx) => ({
      id: tx.id,
      ticker: tx.ticker,
      ticker_raw: tx.ticker_raw || tx.ticker,
      type: tx.type,
      trade_date: tx.trade_date || tx.date,
      shares: tx.shares ?? tx.quantity,
      price: tx.price,
      currency: tx.currency,
      note: tx.note ?? null,
      version: tx.version,
      created_at: tx.created_at,
      updated_at: tx.updated_at,
      deleted_at: tx.deleted_at,
    }));
    return { ...res, data: { ...(res?.data || {}), transactions: mapped } };
  };

  return apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/transactions`, { method: "GET" })
    .then(normalize)
    .catch(async (err) => {
      const msg = String(err?.message || "");
      if (!msg.includes("404") && !msg.includes("Not Found")) throw err;
      const res = await apiFetch(`/portfolios/${encodeURIComponent(portfolioId)}/transactions`, { method: "GET" });
      return normalize(res);
    });
}

export function createPortfolioTransaction(portfolioId, payload) {
  const legacyPayload = {
    ticker: payload.ticker,
    type: payload.type,
    trade_date: payload.trade_date,
    shares: payload.shares,
    price: payload.price,
    currency: payload.currency,
    note: payload.note ?? null,
  };
  const newPayload = {
    portfolio_id: portfolioId,
    ticker: payload.ticker,
    type: payload.type,
    quantity: payload.shares,
    price: payload.price,
    date: payload.trade_date,
    currency: payload.currency,
  };
  return apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/transactions`, {
    method: "POST",
    body: JSON.stringify(legacyPayload),
  }).catch(async (err) => {
    const msg = String(err?.message || "");
    if (!msg.includes("404") && !msg.includes("Not Found")) throw err;
    return apiFetch(`/transactions`, {
      method: "POST",
      body: JSON.stringify(newPayload),
    });
  });
}

export function updatePortfolioTransaction(portfolioId, transactionId, payload) {
  const legacyPayload = {
    ticker: payload.ticker,
    type: payload.type,
    trade_date: payload.trade_date,
    shares: payload.shares,
    price: payload.price,
    currency: payload.currency,
    note: payload.note ?? null,
  };
  const newPayload = {
    ticker: payload.ticker,
    type: payload.type,
    quantity: payload.shares,
    price: payload.price,
    date: payload.trade_date,
    currency: payload.currency,
  };
  return apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/transactions/${encodeURIComponent(transactionId)}`, {
    method: "PUT",
    body: JSON.stringify(legacyPayload),
  }).catch(async (err) => {
    const msg = String(err?.message || "");
    if (!msg.includes("404") && !msg.includes("Not Found")) throw err;
    return apiFetch(`/transactions/${encodeURIComponent(transactionId)}`, {
      method: "PUT",
      body: JSON.stringify(newPayload),
    });
  });
}

export function deletePortfolioTransaction(portfolioId, transactionId) {
  return apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/transactions/${encodeURIComponent(transactionId)}`, {
    method: "DELETE",
  }).catch(async (err) => {
    const msg = String(err?.message || "");
    if (!msg.includes("404") && !msg.includes("Not Found")) throw err;
    return apiFetch(`/transactions/${encodeURIComponent(transactionId)}`, {
      method: "DELETE",
    });
  });
}
