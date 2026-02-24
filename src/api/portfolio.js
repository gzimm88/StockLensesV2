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

export function listPortfolioTransactions(portfolioId) {
  return apiFetch(`/portfolios/${encodeURIComponent(portfolioId)}/transactions`, { method: "GET" }).then((res) => {
    const raw = res?.data?.transactions || [];
    const mapped = raw.map((tx) => ({
      id: tx.id,
      ticker: tx.ticker,
      ticker_raw: tx.ticker,
      type: tx.type,
      trade_date: tx.date,
      shares: tx.quantity,
      price: tx.price,
      currency: tx.currency,
      note: null,
      version: tx.version,
      created_at: tx.created_at,
      updated_at: tx.updated_at,
      deleted_at: tx.deleted_at,
    }));
    return { ...res, data: { ...(res?.data || {}), transactions: mapped } };
  });
}

export function createPortfolioTransaction(portfolioId, payload) {
  return apiFetch(`/transactions`, {
    method: "POST",
    body: JSON.stringify({
      portfolio_id: portfolioId,
      ticker: payload.ticker,
      type: payload.type,
      quantity: payload.shares,
      price: payload.price,
      date: payload.trade_date,
      currency: payload.currency,
    }),
  });
}

export function updatePortfolioTransaction(portfolioId, transactionId, payload) {
  return apiFetch(`/transactions/${encodeURIComponent(transactionId)}`, {
    method: "PUT",
    body: JSON.stringify({
      ticker: payload.ticker,
      type: payload.type,
      quantity: payload.shares,
      price: payload.price,
      date: payload.trade_date,
      currency: payload.currency,
    }),
  });
}

export function deletePortfolioTransaction(portfolioId, transactionId) {
  return apiFetch(`/transactions/${encodeURIComponent(transactionId)}`, {
    method: "DELETE",
  });
}
