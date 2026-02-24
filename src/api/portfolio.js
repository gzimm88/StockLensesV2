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
    if (String(err?.message || "").includes("404")) return null;
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
  return apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/transactions`, { method: "GET" });
}

export function createPortfolioTransaction(portfolioId, payload) {
  return apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/transactions`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function updatePortfolioTransaction(portfolioId, transactionId, payload) {
  return apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/transactions/${encodeURIComponent(transactionId)}`, {
    method: "PUT",
    body: JSON.stringify(payload),
  });
}

export function deletePortfolioTransaction(portfolioId, transactionId) {
  return apiFetch(`/portfolio/${encodeURIComponent(portfolioId)}/transactions/${encodeURIComponent(transactionId)}`, {
    method: "DELETE",
  });
}
