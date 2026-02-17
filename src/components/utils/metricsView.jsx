import { normalizeSymbol } from "./normalizeSymbol";

export function getLatestMetricsBySymbol(metricsData) {
  const metricsMap = new Map();
  
  for (const metric of metricsData) {
    if (!metric || !metric.ticker_symbol) continue;
    
    const key = normalizeSymbol(metric.ticker_symbol);
    const existing = metricsMap.get(key);
    
    // Choose the row with the max asOf (fallback: updated_date)
    const currentDate = metric.asOf || metric.updated_date || metric.created_date || '1900-01-01';
    const existingDate = existing ? (existing.asOf || existing.updated_date || existing.created_date || '1900-01-01') : '1900-01-01';
    
    if (!existing || new Date(currentDate) > new Date(existingDate)) {
      metricsMap.set(key, metric);
    }
  }
  
  return Array.from(metricsMap.values());
}

export function deduplicateTickers(tickerData) {
  const tickerMap = new Map();
  
  for (const ticker of tickerData) {
    if (!ticker || !ticker.symbol) continue;
    
    const normalizedSymbol = normalizeSymbol(ticker.symbol);
    const exchange = ticker.exchange || '';
    const key = `${normalizedSymbol}|${exchange}`;
    
    const existing = tickerMap.get(key);
    
    if (!existing) {
      // Normalize the symbol in the ticker object
      tickerMap.set(key, {
        ...ticker,
        symbol: normalizedSymbol,
        exchange: exchange
      });
    } else {
      // Keep the one with the newer created_date or id
      const currentDate = ticker.created_date || ticker.id || '1900-01-01';
      const existingDate = existing.created_date || existing.id || '1900-01-01';
      
      if (currentDate > existingDate) {
        tickerMap.set(key, {
          ...ticker,
          symbol: normalizedSymbol,
          exchange: exchange
        });
      }
    }
  }
  
  return Array.from(tickerMap.values());
}