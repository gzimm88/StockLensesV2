import React from "react";
import { Link } from "react-router-dom";
import { AreaChart, Area, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from "recharts";
import { Settings } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { createPageUrl } from "@/utils";
import { Ticker } from "@/api/entities";
import {
  createPortfolio,
  createPortfolioTransaction,
  deletePortfolioTransaction,
  getClosedPositions,
  getPortfolioDashboardSummary,
  getPortfolioEquityHistorySeries,
  getPortfolioHoldings,
  getPerformanceBreakdown,
  getTimeReturns,
  getPortfolioSettings,
  getLastPortfolioRun,
  getValuationAttribution,
  getValuationDiff,
  getLatestPortfolioRunMetadata,
  importPortfolioCsv,
  listPortfolioTransactions,
  listPortfolios,
  processPortfolio,
  rebuildPortfolioEquityHistory,
  updatePortfolioSettings,
  updatePortfolioTransaction,
} from "@/api/portfolio";
import AttributionPanel from "@/components/portfolio/AttributionPanel";

function statusColor(status) {
  switch (status) {
    case "OK":
      return "text-green-700 bg-green-50 border-green-200";
    case "BoundedLeadingGap":
      return "text-amber-700 bg-amber-50 border-amber-200";
    case "MissingSegments":
      return "text-orange-700 bg-orange-50 border-orange-200";
    case "NoFeed":
      return "text-red-700 bg-red-50 border-red-200";
    default:
      return "text-slate-700 bg-slate-50 border-slate-200";
  }
}

function shortHash(hash) {
  if (!hash) return "-";
  return hash.length > 16 ? `${hash.slice(0, 16)}...` : hash;
}

function fmtNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function fmtCurrency(value, currency = "USD") {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toLocaleString("en-US", {
    style: "currency",
    currency,
    currencySign: "accounting",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function fmtPercent(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return `${fmtNumber(value, digits)}%`;
}

function fmtDateTime(value) {
  if (!value) return "-";
  if (typeof value === "string" && value.toLowerCase().includes("no price data")) return "No price data";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString("en-US");
}

function fmtAxisNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "";
  return Number(value).toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
}

function parseIsoDate(value) {
  if (!value || typeof value !== "string") return null;
  const d = new Date(`${value}T00:00:00Z`);
  return Number.isNaN(d.getTime()) ? null : d;
}

function formatYahooXAxisTick(value) {
  const d = parseIsoDate(value);
  if (!d) return "";
  const day = d.getUTCDate();
  if (day <= 7) {
    return d.toLocaleString("en-US", { month: "short", timeZone: "UTC" });
  }
  return `${day}`;
}

function normalizeTickerKey(value) {
  return String(value || "")
    .split(":")
    .pop()
    .trim()
    .toUpperCase()
    .replace(/-/g, ".");
}

function computeOpenLotsForTicker(rows) {
  const lots = [];
  const txs = [...rows].sort((a, b) => String(a.trade_date).localeCompare(String(b.trade_date)));
  for (const tx of txs) {
    const type = String(tx.type || "").toUpperCase();
    const shares = Number(tx.shares || 0);
    const price = Number(tx.price || 0);
    if (!Number.isFinite(shares) || shares <= 0) continue;
    if (type === "BUY") {
      lots.push({
        id: tx.id,
        trade_date: tx.trade_date,
        shares_open: shares,
        price,
      });
      continue;
    }
    if (type === "SELL") {
      let remaining = shares;
      while (remaining > 0 && lots.length > 0) {
        const lot = lots[0];
        const take = Math.min(lot.shares_open, remaining);
        lot.shares_open -= take;
        remaining -= take;
        if (lot.shares_open <= 1e-10) lots.shift();
      }
    }
  }
  return lots.filter((lot) => lot.shares_open > 1e-10);
}

const EMPTY_TX = {
  ticker: "",
  type: "Buy",
  trade_date: "",
  shares: "",
  price: "",
  currency: "USD",
  note: "",
};

export default function Portfolio() {
  const [portfolios, setPortfolios] = React.useState([]);
  const [selectedPortfolioId, setSelectedPortfolioId] = React.useState("");
  const [selectedFile, setSelectedFile] = React.useState(null);
  const [isLoading, setIsLoading] = React.useState(true);
  const [isProcessing, setIsProcessing] = React.useState(false);
  const [isImporting, setIsImporting] = React.useState(false);
  const [strictMode, setStrictMode] = React.useState(false);
  const [error, setError] = React.useState("");
  const [equityHistoryNotice, setEquityHistoryNotice] = React.useState("");
  const [result, setResult] = React.useState(null);
  const [metadata, setMetadata] = React.useState(null);
  const [activeTab, setActiveTab] = React.useState("summary");
  const [showCreate, setShowCreate] = React.useState(false);
  const [createName, setCreateName] = React.useState("");
  const [createCurrency, setCreateCurrency] = React.useState("USD");
  const [transactions, setTransactions] = React.useState([]);
  const [closedPositions, setClosedPositions] = React.useState([]);
  const [performanceBreakdown, setPerformanceBreakdown] = React.useState(null);
  const [timeReturns, setTimeReturns] = React.useState(null);
  const [valuationAttribution, setValuationAttribution] = React.useState(null);
  const [valuationDiff, setValuationDiff] = React.useState(null);
  const [dashboardSummary, setDashboardSummary] = React.useState(null);
  const [holdingsRows, setHoldingsRows] = React.useState([]);
  const [equityRange, setEquityRange] = React.useState("6M");
  const [performanceMode, setPerformanceMode] = React.useState("value");
  const [hoveredEquity, setHoveredEquity] = React.useState(null);
  const [equitySeries, setEquitySeries] = React.useState([]);
  const [portfolioSettings, setPortfolioSettings] = React.useState(null);
  const [withholdingInput, setWithholdingInput] = React.useState("");
  const [isSavingSettings, setIsSavingSettings] = React.useState(false);
  const [settingsOpen, setSettingsOpen] = React.useState(false);
  const [txDirty, setTxDirty] = React.useState(false);
  const [showTxModal, setShowTxModal] = React.useState(false);
  const [isSavingTx, setIsSavingTx] = React.useState(false);
  const [deletingTxId, setDeletingTxId] = React.useState(null);
  const [txForm, setTxForm] = React.useState(EMPTY_TX);
  const [editTxId, setEditTxId] = React.useState(null);
  const [knownTickers, setKnownTickers] = React.useState([]);
  const [holdingsSortField, setHoldingsSortField] = React.useState("market_value");
  const [holdingsSortDirection, setHoldingsSortDirection] = React.useState("desc");
  const [closedSortField, setClosedSortField] = React.useState("close_date");
  const [closedSortDirection, setClosedSortDirection] = React.useState("desc");
  const [expandedHoldingTicker, setExpandedHoldingTicker] = React.useState("");
  const [holdingDetailTabByTicker, setHoldingDetailTabByTicker] = React.useState({});

  const selectedPortfolio = portfolios.find((p) => p.id === selectedPortfolioId) || null;
  const displayCurrency = portfolioSettings?.base_currency || selectedPortfolio?.base_currency || "USD";
  const trackCashEnabled = (portfolioSettings?.cash_management_mode || "track_cash") === "track_cash";
  const equityPerformanceMode = performanceMode === "growth" ? "net_of_contributions" : "absolute";
  const latestEquityPoint = equitySeries.length > 0 ? equitySeries[equitySeries.length - 1] : null;
  const rightSideMetric = performanceMode === "growth"
    ? fmtPercent((hoveredEquity?.twr_return_pct ?? latestEquityPoint?.twr_return_pct))
    : fmtCurrency((hoveredEquity?.plotted_value ?? latestEquityPoint?.plotted_value), displayCurrency);

  const sortedHoldingsRows = React.useMemo(() => {
    const rows = holdingsRows.map((row) => {
      const basis = Number(row?.total_cost_basis || 0);
      const realized = Number(row?.realized_gain_value || 0);
      const realizedPct = basis !== 0 ? (realized / basis) * 100 : null;
      return { ...row, realized_gain_percent: realizedPct };
    });
    const dir = holdingsSortDirection === "asc" ? 1 : -1;
    rows.sort((a, b) => {
      const av = a?.[holdingsSortField];
      const bv = b?.[holdingsSortField];
      const an = typeof av === "number" ? av : Number(av);
      const bn = typeof bv === "number" ? bv : Number(bv);
      if (!Number.isNaN(an) && !Number.isNaN(bn)) return (an - bn) * dir;
      return String(av ?? "").localeCompare(String(bv ?? "")) * dir;
    });
    return rows;
  }, [holdingsRows, holdingsSortDirection, holdingsSortField]);

  const sortedClosedRows = React.useMemo(() => {
    const rows = [...closedPositions];
    const dir = closedSortDirection === "asc" ? 1 : -1;
    rows.sort((a, b) => {
      const av = a?.[closedSortField];
      const bv = b?.[closedSortField];
      const ad = Date.parse(`${av || ""}`);
      const bd = Date.parse(`${bv || ""}`);
      if (!Number.isNaN(ad) && !Number.isNaN(bd)) return (ad - bd) * dir;
      const an = typeof av === "number" ? av : Number(av);
      const bn = typeof bv === "number" ? bv : Number(bv);
      if (!Number.isNaN(an) && !Number.isNaN(bn)) return (an - bn) * dir;
      return String(av ?? "").localeCompare(String(bv ?? "")) * dir;
    });
    return rows;
  }, [closedPositions, closedSortField, closedSortDirection]);

  const toggleHoldingsSort = (field) => {
    if (holdingsSortField === field) {
      setHoldingsSortDirection((p) => (p === "asc" ? "desc" : "asc"));
      return;
    }
    setHoldingsSortField(field);
    setHoldingsSortDirection("desc");
  };

  const sortIndicator = (field) => {
    if (holdingsSortField !== field) return "";
    return holdingsSortDirection === "desc" ? "▾" : "▴";
  };

  const toggleHoldingDetails = (ticker) => {
    setExpandedHoldingTicker((prev) => (prev === ticker ? "" : ticker));
    setHoldingDetailTabByTicker((prev) => ({
      ...prev,
      [ticker]: prev[ticker] || "transactions",
    }));
  };

  const setHoldingDetailTab = (ticker, tab) => {
    setHoldingDetailTabByTicker((prev) => ({ ...prev, [ticker]: tab }));
  };

  const openAddTxForTicker = (ticker, lastPrice) => {
    setEditTxId(null);
    setTxForm({
      ...EMPTY_TX,
      ticker,
      type: "Buy",
      trade_date: new Date().toISOString().slice(0, 10),
      shares: "",
      price: lastPrice != null && !Number.isNaN(Number(lastPrice)) ? String(Number(lastPrice)) : "",
      currency: selectedPortfolio?.base_currency || "USD",
      note: "",
    });
    setShowTxModal(true);
  };

  const toggleClosedSort = (field) => {
    if (closedSortField === field) {
      setClosedSortDirection((p) => (p === "asc" ? "desc" : "asc"));
      return;
    }
    setClosedSortField(field);
    setClosedSortDirection("desc");
  };

  const closedSortIndicator = (field) => {
    if (closedSortField !== field) return "";
    return closedSortDirection === "desc" ? "▾" : "▴";
  };

  const loadPortfolios = React.useCallback(async () => {
    const res = await listPortfolios();
    const rows = res?.data?.portfolios || [];
    setPortfolios(rows);
    if (!selectedPortfolioId && rows.length > 0) {
      setSelectedPortfolioId(rows[0].id);
    } else if (selectedPortfolioId && !rows.some((p) => p.id === selectedPortfolioId)) {
      setSelectedPortfolioId(rows[0]?.id || "");
    }
  }, [selectedPortfolioId]);

  const loadTransactions = React.useCallback(
    async (portfolioId, finishedAt = null) => {
      if (!portfolioId) return;
      const res = await listPortfolioTransactions(portfolioId);
      const txs = res?.data?.transactions || [];
      setTransactions(txs);
      if (!finishedAt) {
        setTxDirty(txs.length > 0);
        return;
      }
      const lastRunMs = Date.parse(finishedAt.replace("Z", ""));
      const changed = txs.some((tx) => {
        if (!tx.updated_at) return true;
        return Date.parse(tx.updated_at.replace("Z", "")) > lastRunMs;
      });
      setTxDirty(changed);
    },
    []
  );

  const loadPortfolioState = React.useCallback(
    async (portfolioId) => {
      if (!portfolioId) {
        setResult(null);
        setMetadata(null);
        setTransactions([]);
        setValuationAttribution(null);
        setValuationDiff(null);
        setDashboardSummary(null);
        setHoldingsRows([]);
        setClosedPositions([]);
        setPerformanceBreakdown(null);
        setTimeReturns(null);
        setEquitySeries([]);
        setPortfolioSettings(null);
        setEquityHistoryNotice("");
        return;
      }
      const settled = await Promise.allSettled([
        getLastPortfolioRun(portfolioId),
        getLatestPortfolioRunMetadata(portfolioId),
        getValuationAttribution(portfolioId),
        getValuationDiff(portfolioId),
        getPortfolioDashboardSummary(portfolioId),
        getPortfolioHoldings(portfolioId),
        getPortfolioEquityHistorySeries(portfolioId, {
          range: equityRange,
          performanceMode: equityPerformanceMode,
          showFxImpact: false,
        }),
        getPortfolioSettings(portfolioId),
        listPortfolioTransactions(portfolioId),
        getClosedPositions(portfolioId),
        getPerformanceBreakdown(portfolioId),
        getTimeReturns(portfolioId),
      ]);

      const getSettledData = (idx) => (settled[idx]?.status === "fulfilled" ? settled[idx].value?.data || null : null);
      const getSettledError = (idx) =>
        settled[idx]?.status === "rejected" ? (settled[idx].reason?.message || "Request failed.") : "";
      const isMissingHistoryErr = (msg) =>
        typeof msg === "string" && msg.toLowerCase().includes("no equity history rows found");

      const lastData = getSettledData(0);
      const metaData = getSettledData(1);
      const attributionData = getSettledData(2);
      const diffData = getSettledData(3);
      const summaryData = getSettledData(4);
      const holdingsData = getSettledData(5);
      const historyData = getSettledData(6);
      const settingsData = getSettledData(7);
      const transactionsData = getSettledData(8);
      const closedData = getSettledData(9);
      const performanceData = getSettledData(10);
      const timeReturnsData = getSettledData(11);

      const summaryErr = getSettledError(4);
      const historyErr = getSettledError(6);
      const missingHistory = isMissingHistoryErr(summaryErr) || isMissingHistoryErr(historyErr);

      setResult(lastData);
      setMetadata(metaData);
      setValuationAttribution(attributionData);
      setValuationDiff(diffData);
      setDashboardSummary(summaryData);
      setHoldingsRows(holdingsData?.holdings || []);
      const txs = transactionsData?.transactions || [];
      setTransactions(txs);
      setClosedPositions(closedData?.closed_positions || []);
      setPerformanceBreakdown(performanceData || null);
      setTimeReturns(timeReturnsData || null);
      setEquitySeries(historyData?.series || []);
      setPortfolioSettings(settingsData);
      setEquityHistoryNotice(missingHistory ? "Equity history not built yet." : "");

      const lastRunFinishedAt = metaData?.finished_at || null;
      if (!lastRunFinishedAt) {
        setTxDirty(txs.length > 0);
      } else {
        const lastRunMs = Date.parse(lastRunFinishedAt.replace("Z", ""));
        const changed = txs.some((tx) => !tx.updated_at || Date.parse(tx.updated_at.replace("Z", "")) > lastRunMs);
        setTxDirty(changed);
      }

      const nonHistoryErrors = [
        getSettledError(0),
        getSettledError(1),
        getSettledError(2),
        getSettledError(3),
        getSettledError(5),
        getSettledError(7),
        getSettledError(8),
        getSettledError(9),
        getSettledError(10),
        getSettledError(11),
      ].filter(Boolean);
      const relevantSummaryErrors = [summaryErr, historyErr].filter((msg) => msg && !isMissingHistoryErr(msg));
      const combinedErrors = [...nonHistoryErrors, ...relevantSummaryErrors];
      if (combinedErrors.length > 0) {
        setError(combinedErrors[0]);
      }
    },
    [equityRange, equityPerformanceMode]
  );

  React.useEffect(() => {
    let active = true;
    (async () => {
      setIsLoading(true);
      try {
        await loadPortfolios();
        const tickers = await Ticker.list();
        if (active) {
          setKnownTickers((tickers || []).map((t) => t.symbol).filter(Boolean));
        }
      } catch (err) {
        if (active) setError(err instanceof Error ? err.message : "Failed to load portfolios.");
      } finally {
        if (active) setIsLoading(false);
      }
    })();
    return () => {
      active = false;
    };
  }, [loadPortfolios]);

  React.useEffect(() => {
      setResult(null);
      setMetadata(null);
      setTransactions([]);
      setValuationAttribution(null);
      setValuationDiff(null);
      setDashboardSummary(null);
      setHoldingsRows([]);
      setClosedPositions([]);
      setPerformanceBreakdown(null);
      setTimeReturns(null);
      setEquitySeries([]);
      setPortfolioSettings(null);
      setEquityHistoryNotice("");
      setTxDirty(false);
      setError("");
    if (!selectedPortfolioId) return;
    (async () => {
      try {
        await loadPortfolioState(selectedPortfolioId);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load portfolio state.");
      }
    })();
  }, [selectedPortfolioId, loadPortfolioState]);

  React.useEffect(() => {
    if (!selectedPortfolioId) return;
    (async () => {
      try {
        const history = await getPortfolioEquityHistorySeries(selectedPortfolioId, {
          range: equityRange,
          performanceMode: equityPerformanceMode,
          showFxImpact: false,
        });
        setEquitySeries(history?.data?.series || []);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load equity history.");
      }
    })();
  }, [selectedPortfolioId, equityRange, equityPerformanceMode]);

  React.useEffect(() => {
    if (!portfolioSettings) {
      setWithholdingInput("");
      return;
    }
    const value = portfolioSettings?.dividend_withholding_percent;
    setWithholdingInput(value === null || value === undefined ? "" : String(value));
  }, [portfolioSettings]);

  const handleFileChange = (event) => {
    const file = event.target.files?.[0] ?? null;
    setSelectedFile(file);
  };

  const handleCreatePortfolio = async () => {
    setError("");
    try {
      const created = await createPortfolio({ name: createName, base_currency: createCurrency });
      const newId = created?.data?.id;
      await loadPortfolios();
      if (newId) setSelectedPortfolioId(newId);
      setShowCreate(false);
      setCreateName("");
      setCreateCurrency("USD");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create portfolio.");
    }
  };

  const handleImportCsv = async () => {
    if (!selectedPortfolioId) return;
    setIsImporting(true);
    setError("");
    try {
      await importPortfolioCsv(selectedPortfolioId, { replaceExisting: true });
      await loadTransactions(selectedPortfolioId, metadata?.finished_at || null);
      setTxDirty(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : "CSV import failed.");
    } finally {
      setIsImporting(false);
    }
  };

  const handleProcess = async () => {
    if (!selectedPortfolioId) return;
    setIsProcessing(true);
    setError("");
    try {
      const response = await processPortfolio(selectedPortfolioId, { strict: strictMode });
      setResult(response?.data || null);
      const latestMeta = await getLatestPortfolioRunMetadata(selectedPortfolioId);
      setMetadata(latestMeta?.data || null);
      await loadPortfolioState(selectedPortfolioId);
      setTxDirty(false);
      await loadPortfolios();
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Portfolio processing failed.";
      if (msg.includes("Historical inputs changed before last equity history date")) {
        try {
          await rebuildPortfolioEquityHistory(selectedPortfolioId, {
            mode: "full",
            force: true,
            strict: strictMode,
          });
          await loadPortfolioState(selectedPortfolioId);
          setTxDirty(false);
          await loadPortfolios();
          setError("");
        } catch (fallbackErr) {
          setError(fallbackErr instanceof Error ? fallbackErr.message : msg);
        }
      } else {
        setError(msg);
      }
    } finally {
      setIsProcessing(false);
    }
  };

  const openAddTx = () => {
    setEditTxId(null);
    setTxForm({
      ...EMPTY_TX,
      trade_date: new Date().toISOString().slice(0, 10),
      currency: selectedPortfolio?.base_currency || "USD",
    });
    setShowTxModal(true);
  };

  const openEditTx = (tx) => {
    setEditTxId(tx.id);
    setTxForm({
      ticker: tx.ticker_raw || tx.ticker,
      type: tx.type,
      trade_date: tx.trade_date,
      shares: tx.shares ?? "",
      price: tx.price ?? "",
      currency: tx.currency || "USD",
      note: tx.note || "",
    });
    setShowTxModal(true);
  };

  const saveTx = async () => {
    if (!selectedPortfolioId) return;
    setError("");
    setIsSavingTx(true);
    const payload = {
      trade_date: txForm.trade_date,
      shares: Number(txForm.shares),
      price: Number(txForm.price),
      currency: txForm.currency,
    };
    try {
      if (editTxId) {
        await updatePortfolioTransaction(selectedPortfolioId, editTxId, payload);
      } else {
        await createPortfolioTransaction(selectedPortfolioId, {
          ...payload,
          ticker: txForm.ticker,
          type: txForm.type,
          note: txForm.note || null,
        });
      }
      setShowTxModal(false);
      setTxForm(EMPTY_TX);
      setEditTxId(null);
      await loadPortfolioState(selectedPortfolioId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save transaction.");
    } finally {
      setIsSavingTx(false);
    }
  };

  const deleteTx = async (txId) => {
    if (!selectedPortfolioId) return;
    if (!window.confirm("Delete this transaction? This will trigger deterministic rebuild.")) return;
    setError("");
    setDeletingTxId(txId);
    try {
      await deletePortfolioTransaction(selectedPortfolioId, txId);
      await loadPortfolioState(selectedPortfolioId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete transaction.");
    } finally {
      setDeletingTxId(null);
    }
  };

  const saveSettings = async (next) => {
    if (!selectedPortfolioId) return;
    setIsSavingSettings(true);
    setError("");
    try {
      const res = await updatePortfolioSettings(selectedPortfolioId, next);
      setPortfolioSettings(res?.data || null);
      await rebuildPortfolioEquityHistory(selectedPortfolioId, {
        mode: "full",
        force: true,
        strict: strictMode,
      });
      await loadPortfolioState(selectedPortfolioId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update portfolio settings.");
    } finally {
      setIsSavingSettings(false);
    }
  };

  const coverageRows = metadata?.coverage_summary ?? result?.coverage_status?.coverage_summary ?? [];
  const hasNonOkCoverage = coverageRows.some((r) => r?.status && r.status !== "OK");
  const correctionCount = metadata?.correction_event_count ?? result?.correction_event_count ?? 0;
  const fallbackCount = metadata?.fallback_count ?? result?.fallback_count ?? 0;
  const warningCount = metadata?.warnings_count ?? result?.warnings_count ?? 0;
  const corrections = metadata?.corrections ?? [];

  return (
    <div className="space-y-6">
      <div className="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 p-6 space-y-4">
        <div className="flex items-center justify-between gap-3">
          <h1 className="text-2xl font-semibold text-slate-900 dark:text-slate-100">Portfolio Processing</h1>
          <div className="flex items-center gap-2">
            <select
              value={selectedPortfolioId}
              onChange={(e) => setSelectedPortfolioId(e.target.value)}
              className="border rounded-md px-2 py-1 text-sm bg-white dark:bg-slate-900"
            >
              {portfolios.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.name}
                </option>
              ))}
            </select>
            <Button variant="outline" onClick={() => setShowCreate((v) => !v)}>Create New Portfolio</Button>
            <Link to={createPageUrl("Portfolios")}>
              <Button variant="outline">All Portfolios</Button>
            </Link>
            <Button variant="outline" size="icon" onClick={() => setSettingsOpen(true)} title="Portfolio settings">
              <Settings className="h-4 w-4" />
            </Button>
          </div>
        </div>

        {showCreate && (
          <div className="rounded-md border border-slate-200 p-3 space-y-2">
            <div className="grid md:grid-cols-2 gap-2">
              <input placeholder="Portfolio name" value={createName} onChange={(e) => setCreateName(e.target.value)} className="border rounded-md px-2 py-1 text-sm" />
              <input placeholder="Base currency" value={createCurrency} onChange={(e) => setCreateCurrency(e.target.value.toUpperCase())} className="border rounded-md px-2 py-1 text-sm" />
            </div>
            <Button onClick={handleCreatePortfolio}>Create</Button>
          </div>
        )}

        <div className="space-y-2">
          <label htmlFor="portfolio-csv" className="text-sm font-medium text-slate-700 dark:text-slate-300">Portfolio CSV Upload</label>
          <input id="portfolio-csv" type="file" accept=".csv" onChange={handleFileChange} className="block w-full text-sm file:mr-4 file:rounded-md file:border-0 file:bg-slate-900 file:px-3 file:py-2 file:text-white dark:file:bg-slate-200 dark:file:text-slate-900" />
          {selectedFile && <p className="text-xs text-slate-500 dark:text-slate-400">Selected: {selectedFile.name}</p>}
        </div>

        <div className="rounded-md border border-slate-200 dark:border-slate-800 p-3">
          <label className="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300">
            <input type="checkbox" checked={strictMode} onChange={(e) => setStrictMode(e.target.checked)} />
            Strict Coverage Mode (passes <code>strict=true</code>)
          </label>
        </div>

        <div className="flex items-center gap-3">
          <Button onClick={handleImportCsv} disabled={isImporting || !selectedPortfolioId}>
            {isImporting ? "Importing..." : "Import CSV"}
          </Button>
          <Button onClick={handleProcess} disabled={isProcessing || !selectedPortfolioId}>
            {isProcessing ? "Processing..." : "Reprocess Now"}
          </Button>
          {error && <Button variant="outline" onClick={handleProcess} disabled={isProcessing || !selectedPortfolioId}>Retry</Button>}
        </div>

        {txDirty && (
          <div className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
            Portfolio has unprocessed changes. Re-run processing to update metrics.
            <Button size="sm" className="ml-3" onClick={handleProcess} disabled={isProcessing || !selectedPortfolioId}>
              Reprocess Now
            </Button>
          </div>
        )}

        {isLoading && <p className="text-xs text-slate-500 dark:text-slate-400">Loading portfolios...</p>}
        {equityHistoryNotice && (
          <div className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
            {equityHistoryNotice}
          </div>
        )}
        {error && <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-700">{error}</div>}
      </div>

      <Dialog open={settingsOpen} onOpenChange={setSettingsOpen}>
        <DialogContent className="sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>Portfolio Settings</DialogTitle>
            <DialogDescription>
              Configure cash/performance behavior for this portfolio.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 text-sm">
            <div>
              <p className="text-slate-500">Base Currency</p>
              <p className="font-medium">{displayCurrency}</p>
            </div>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={(portfolioSettings?.cash_management_mode || "track_cash") === "track_cash"}
                disabled={isSavingSettings}
                onChange={(e) =>
                  saveSettings({
                    cash_management_mode: e.target.checked ? "track_cash" : "ignore_cash",
                  })
                }
              />
              Track cash balance
            </label>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={Boolean(portfolioSettings?.include_dividends_in_performance ?? true)}
                disabled={isSavingSettings}
                onChange={(e) =>
                  saveSettings({
                    include_dividends_in_performance: e.target.checked,
                  })
                }
              />
              Include dividends in performance
            </label>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={Boolean(portfolioSettings?.reinvest_dividends_overlay ?? false)}
                disabled={isSavingSettings}
                onChange={(e) =>
                  saveSettings({
                    reinvest_dividends_overlay: e.target.checked,
                  })
                }
              />
              Reinvest dividends overlay
            </label>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={Boolean(portfolioSettings?.apply_dividend_withholding ?? false)}
                disabled={isSavingSettings}
                onChange={(e) =>
                  saveSettings({
                    apply_dividend_withholding: e.target.checked,
                    dividend_withholding_percent: e.target.checked
                      ? Number(withholdingInput || 0)
                      : null,
                  })
                }
              />
              Apply dividend withholding tax
            </label>
            <div className="max-w-56">
              <label className="text-xs text-slate-500">Withholding (%)</label>
              <input
                type="number"
                min="0"
                max="100"
                step="0.01"
                value={withholdingInput}
                disabled={isSavingSettings || !Boolean(portfolioSettings?.apply_dividend_withholding)}
                onChange={(e) => setWithholdingInput(e.target.value)}
                onBlur={() => {
                  if (!Boolean(portfolioSettings?.apply_dividend_withholding)) return;
                  const parsed = Number(withholdingInput);
                  if (Number.isNaN(parsed)) return;
                  saveSettings({ dividend_withholding_percent: parsed });
                }}
                className="mt-1 w-full border rounded px-2 py-1"
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setSettingsOpen(false)}>Close</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {!result && !metadata && selectedPortfolio && transactions.length === 0 && !equityHistoryNotice && (
        <div className="rounded-xl border border-slate-200 bg-white p-6">
          <h2 className="text-lg font-semibold">No transactions yet</h2>
          <p className="text-sm text-slate-600 mt-1">Import CSV or add manually.</p>
          <Button className="mt-3" onClick={openAddTx}>Add Transaction</Button>
        </div>
      )}

      {(result || metadata || transactions.length > 0) && (
        <div className="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 p-6 space-y-4">
          <div className="flex items-center gap-2">
            {["summary", "attribution", "coverage", "corrections", "transactions"].map((tab) => (
              <button
                key={tab}
                type="button"
                className={`px-3 py-1.5 text-sm rounded border ${
                  activeTab === tab ? "bg-slate-900 text-white border-slate-900" : "border-slate-300"
                }`}
                onClick={() => setActiveTab(tab)}
              >
                {tab[0].toUpperCase() + tab.slice(1)}
              </button>
            ))}
          </div>

          {activeTab === "summary" && (
            <div className="space-y-4">
              <div className="flex items-center justify-between gap-2">
                <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">Portfolio Summary</h2>
                <span className="text-xs px-2 py-1 rounded border border-slate-300 text-slate-700" title={`Results are fully reproducible from transaction ledger + price data snapshot. Input hash: ${metadata?.input_hash || result?.input_hash || "-"}`}>
                  Deterministic Run
                </span>
              </div>
              {correctionCount > 0 && (
                <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-700">
                  If corrections were applied, results may differ from raw transaction intent.
                </div>
              )}
              <div className="grid md:grid-cols-3 gap-3 text-sm">
                <div><p className="text-slate-500">Total Equity</p><p className="font-medium">{fmtCurrency(dashboardSummary?.total_equity, displayCurrency)}</p></div>
                <div>
                  <p className="text-slate-500">Day Change</p>
                  <p className="font-medium">{fmtCurrency(dashboardSummary?.day_change_value, displayCurrency)} ({fmtPercent(dashboardSummary?.day_change_percent)})</p>
                </div>
                <div>
                  <p className="text-slate-500">Unrealized Gain/Loss</p>
                  <p className="font-medium">{fmtCurrency(dashboardSummary?.unrealized_gain_value, displayCurrency)} ({fmtPercent(dashboardSummary?.unrealized_gain_percent)})</p>
                </div>
                <div><p className="text-slate-500">Market Impact</p><p className="font-medium">{fmtCurrency(dashboardSummary?.market_move_component, displayCurrency)}</p></div>
                <div><p className="text-slate-500">FX Impact</p><p className="font-medium">{fmtCurrency(dashboardSummary?.currency_move_component, displayCurrency)}</p></div>
                <div>
                  <p className="text-slate-500">Realized Gain/Loss</p>
                  <p className="font-medium">{fmtCurrency(dashboardSummary?.realized_gain_value, displayCurrency)}</p>
                  <p className="text-xs text-slate-500 mt-1">Last Prices Updated: {fmtDateTime(dashboardSummary?.last_prices_updated_at)}</p>
                </div>
                <div><p className="text-slate-500">Cash</p><p className="font-medium">{trackCashEnabled ? fmtCurrency(dashboardSummary?.cash_balance, displayCurrency) : "--"}</p></div>
                <div><p className="text-slate-500">Generated At</p><p className="font-medium">{result?.generated_at ?? "-"}</p></div>
              </div>

              <details className="rounded-md border border-slate-200 dark:border-slate-800 p-3">
                <summary className="cursor-pointer text-sm font-semibold">Performance Breakdown</summary>
                <div className="mt-3 space-y-2 text-sm">
                  <div className="flex justify-between"><span>Realized</span><span className="font-medium">{fmtCurrency(performanceBreakdown?.realized_gain, displayCurrency)}</span></div>
                  <div className="flex justify-between"><span>Unrealized</span><span className="font-medium">{fmtCurrency(performanceBreakdown?.unrealized_gain, displayCurrency)}</span></div>
                  <div className="flex justify-between"><span>FX Impact</span><span className="font-medium">{fmtCurrency(performanceBreakdown?.fx_gain, displayCurrency)}</span></div>
                  <div className="flex justify-between"><span>Dividends</span><span className="font-medium">{fmtCurrency(performanceBreakdown?.dividend_gain, displayCurrency)}</span></div>
                  <div className="flex justify-between border-t pt-2">
                    <span className="font-semibold">Total Gain</span>
                    <span className="font-semibold">{fmtCurrency(performanceBreakdown?.total_gain, displayCurrency)} ({fmtPercent(performanceBreakdown?.total_gain_pct)})</span>
                  </div>
                </div>
              </details>

              <div className="rounded-md border border-slate-200 dark:border-slate-800 p-3">
                <h3 className="text-sm font-semibold">Time Returns</h3>
                <div className="mt-3 space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span>Since inception</span>
                    <span className="font-medium">{fmtPercent(timeReturns?.since_inception_return_pct)}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>YTD</span>
                    <span className="font-medium">{fmtPercent(timeReturns?.ytd_return_pct)}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>1Y</span>
                    <span className="font-medium">{fmtPercent(timeReturns?.one_year_return_pct)}</span>
                  </div>
                </div>
              </div>

              <div className="rounded-md border border-slate-200 dark:border-slate-800 p-3">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-sm font-semibold">Equity Curve</h3>
                  <div className="flex items-center gap-2">
                    <select
                      value={performanceMode}
                      onChange={(e) => setPerformanceMode(e.target.value)}
                      className="border rounded-md px-2 py-1 text-sm bg-white dark:bg-slate-900"
                    >
                      <option value="value">Holdings Value</option>
                      <option value="growth">Holdings Growth</option>
                    </select>
                    <select
                      value={equityRange}
                      onChange={(e) => setEquityRange(e.target.value)}
                      className="border rounded-md px-2 py-1 text-sm bg-white dark:bg-slate-900"
                    >
                      {["1D", "5D", "1W", "1M", "3M", "6M", "1Y", "ALL"].map((r) => <option key={r} value={r}>{r}</option>)}
                    </select>
                  </div>
                </div>
                <div className="h-56 rounded-md border border-slate-200 bg-gradient-to-b from-slate-50 to-white p-2 relative">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart
                      data={equitySeries}
                      margin={{ top: 8, right: 62, left: 8, bottom: 28 }}
                      onMouseMove={(state) => {
                        const p = state?.activePayload?.[0]?.payload;
                        const c = state?.activeCoordinate;
                        if (!p || !c) {
                          setHoveredEquity(null);
                          return;
                        }
                        setHoveredEquity({
                          date: p.date,
                          plotted_value: p.plotted_value,
                          twr_return_pct: p.twr_return_pct,
                          x: c.x,
                          y: c.y,
                        });
                      }}
                      onMouseLeave={() => setHoveredEquity(null)}
                    >
                      <defs>
                        <linearGradient id="equityFill" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#38bdf8" stopOpacity={0.35} />
                          <stop offset="95%" stopColor="#38bdf8" stopOpacity={0.05} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="#cbd5e1" />
                      <XAxis
                        dataKey="date"
                        tickFormatter={formatYahooXAxisTick}
                        minTickGap={24}
                        tickMargin={8}
                        tick={{ fontSize: 10, fill: "#94a3b8", fontWeight: 400 }}
                      />
                      <YAxis
                        orientation="right"
                        tickFormatter={(v) => (performanceMode === "growth" ? fmtPercent(v) : fmtAxisNumber(v))}
                        tickMargin={6}
                        tick={{ fontSize: 9, fill: "#94a3b8", fontWeight: 400 }}
                        width={58}
                      />
                      <Tooltip
                        cursor={{ stroke: "#64748b", strokeDasharray: "4 4" }}
                        content={() => null}
                      />
                      <Area type="monotone" dataKey="plotted_value" stroke="none" fill="url(#equityFill)" />
                      {hoveredEquity && (
                        <ReferenceLine y={hoveredEquity.plotted_value} stroke="#94a3b8" strokeDasharray="4 4" />
                      )}
                      {latestEquityPoint && (
                        <ReferenceLine
                          y={latestEquityPoint.plotted_value}
                          stroke="#10b981"
                          strokeDasharray="6 6"
                        />
                      )}
                      <Line type="monotone" dataKey="plotted_value" stroke="#0ea5e9" dot={false} strokeWidth={2.5} />
                    </AreaChart>
                  </ResponsiveContainer>
                  <div className="absolute right-2 top-10 px-2 py-0.5 rounded text-white bg-emerald-700 text-[10px] font-medium leading-4">
                    {rightSideMetric}
                  </div>
                  {hoveredEquity?.date && (
                    <div
                      className="absolute px-2 py-0.5 rounded text-white bg-slate-800 text-[10px] leading-4"
                      style={{ left: `calc(${hoveredEquity.x}px - 45px)`, bottom: 2 }}
                    >
                      {new Date(`${hoveredEquity.date}T00:00:00Z`).toLocaleDateString("en-US")}
                    </div>
                  )}
                </div>
              </div>

              <div className="overflow-x-auto">
                <div className="flex items-center justify-between mb-2">
                  <h3 className="text-sm font-semibold">Holdings</h3>
                  <span className="text-xs text-slate-500">
                    Sorted by {holdingsSortField.replaceAll("_", " ")} {holdingsSortDirection === "desc" ? "↓" : "↑"}
                  </span>
                </div>
                <table className="w-full text-sm border-collapse">
                  <thead>
                    <tr className="border-b bg-slate-900 text-slate-100">
                      <th className="text-left py-2 px-2 whitespace-nowrap"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("ticker")}>Symbol {sortIndicator("ticker")}</button></th>
                      <th className="text-left py-2 px-2">Status</th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("quantity")}>Shares {sortIndicator("quantity")}</button></th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("market_price")}>Last Price {sortIndicator("market_price")}</button></th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("avg_cost_basis_native")}>AC/Share {sortIndicator("avg_cost_basis_native")}</button></th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("total_cost_basis")}>Total Cost {sortIndicator("total_cost_basis")}</button></th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("market_value")}>Market Value {sortIndicator("market_value")}</button></th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("total_dividends")}>Tot Div Income ({displayCurrency}) {sortIndicator("total_dividends")}</button></th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("day_change_percent")}>Day Gain UNRL (%) {sortIndicator("day_change_percent")}</button></th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("day_change_value")}>Day Gain UNRL {sortIndicator("day_change_value")}</button></th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("unrealized_gain_percent")}>Tot Gain UNRL (%) {sortIndicator("unrealized_gain_percent")}</button></th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("unrealized_gain_value")}>Tot Gain UNRL {sortIndicator("unrealized_gain_value")}</button></th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("realized_gain_percent")}>Realized Gain (%) {sortIndicator("realized_gain_percent")}</button></th>
                      <th className="text-left py-2 px-2"><button className="font-medium hover:text-white" onClick={() => toggleHoldingsSort("realized_gain_value")}>Realized Gain ({displayCurrency}) {sortIndicator("realized_gain_value")}</button></th>
                    </tr>
                  </thead>
                  <tbody>
                    {holdingsRows.length === 0 ? (
                      <tr><td colSpan={14} className="py-3 text-slate-500">No holdings snapshot available.</td></tr>
                    ) : (
                      sortedHoldingsRows.map((row) => {
                        const ticker = row.ticker;
                        const tickerKey = normalizeTickerKey(ticker);
                        const detailTab = holdingDetailTabByTicker[ticker] || "transactions";
                        const tickerTx = transactions
                          .filter((tx) => normalizeTickerKey(tx.ticker_raw || tx.ticker || tx.ticker_normalized) === tickerKey && tx.type !== "Dividend")
                          .sort((a, b) => String(b.trade_date).localeCompare(String(a.trade_date)));
                        const tickerDivs = transactions
                          .filter((tx) => normalizeTickerKey(tx.ticker_raw || tx.ticker || tx.ticker_normalized) === tickerKey && tx.type === "Dividend")
                          .sort((a, b) => String(b.trade_date).localeCompare(String(a.trade_date)));
                        const tickerLots = computeOpenLotsForTicker(tickerTx);
                        const isExpanded = expandedHoldingTicker === ticker;

                        return (
                          <React.Fragment key={ticker}>
                            <tr className="border-b hover:bg-slate-50">
                              <td className="py-2 px-2 font-mono text-blue-700 whitespace-nowrap">
                                <div className="inline-flex items-center gap-2">
                                  <button
                                    type="button"
                                    className="text-slate-500 hover:text-slate-900 text-[11px] leading-none"
                                    onClick={() => toggleHoldingDetails(ticker)}
                                    title={isExpanded ? "Hide details" : "Show details"}
                                  >
                                    {isExpanded ? "▾" : "▸"}
                                  </button>
                                  <span>{ticker}</span>
                                </div>
                              </td>
                              <td className="py-2 px-2">Open</td>
                              <td className="py-2 px-2">{fmtNumber(row.quantity, 6)}</td>
                              <td className="py-2 px-2">{fmtCurrency(row.market_price, displayCurrency)}</td>
                              <td className="py-2 px-2">{fmtCurrency(row.avg_cost_basis_native, row.native_currency || displayCurrency)}</td>
                              <td className="py-2 px-2">{fmtCurrency(row.total_cost_basis, displayCurrency)}</td>
                              <td className="py-2 px-2">{fmtCurrency(row.market_value, displayCurrency)}</td>
                              <td className="py-2 px-2">{fmtCurrency(row.total_dividends, displayCurrency)}</td>
                              <td className={`py-2 px-2 ${(Number(row.day_change_percent || 0) >= 0) ? "text-emerald-700" : "text-red-600"}`}>
                                {fmtPercent(row.day_change_percent)}
                              </td>
                              <td className={`py-2 px-2 ${(Number(row.day_change_value || 0) >= 0) ? "text-emerald-700" : "text-red-600"}`}>
                                {fmtCurrency(row.day_change_value, displayCurrency)}
                              </td>
                              <td className={`py-2 px-2 ${(Number(row.unrealized_gain_percent || 0) >= 0) ? "text-emerald-700" : "text-red-600"}`}>
                                {fmtPercent(row.unrealized_gain_percent)}
                              </td>
                              <td className={`py-2 px-2 ${(Number(row.unrealized_gain_value || 0) >= 0) ? "text-emerald-700" : "text-red-600"}`}>
                                {fmtCurrency(row.unrealized_gain_value, displayCurrency)}
                              </td>
                              <td className={`py-2 px-2 ${(Number(row.realized_gain_percent || 0) >= 0) ? "text-emerald-700" : "text-red-600"}`}>
                                {row.realized_gain_percent == null ? "--" : fmtPercent(row.realized_gain_percent)}
                              </td>
                              <td className={`py-2 px-2 ${(Number(row.realized_gain_value || 0) >= 0) ? "text-emerald-700" : "text-red-600"}`}>
                                {fmtCurrency(row.realized_gain_value, displayCurrency)}
                              </td>
                            </tr>
                            {isExpanded && (
                              <tr className="border-b bg-slate-50">
                                <td colSpan={14} className="p-3">
                                  <div className="space-y-3">
                                    <div className="flex items-center gap-2">
                                      <button
                                        type="button"
                                        className={`px-3 py-1 text-xs rounded border ${detailTab === "share_lots" ? "bg-blue-50 border-blue-300 text-blue-700" : "bg-white border-slate-300 text-slate-700"}`}
                                        onClick={() => setHoldingDetailTab(ticker, "share_lots")}
                                      >
                                        Share Lots
                                      </button>
                                      <button
                                        type="button"
                                        className={`px-3 py-1 text-xs rounded border ${detailTab === "transactions" ? "bg-blue-50 border-blue-300 text-blue-700" : "bg-white border-slate-300 text-slate-700"}`}
                                        onClick={() => setHoldingDetailTab(ticker, "transactions")}
                                      >
                                        Transactions
                                      </button>
                                      <button
                                        type="button"
                                        className={`px-3 py-1 text-xs rounded border ${detailTab === "dividends" ? "bg-blue-50 border-blue-300 text-blue-700" : "bg-white border-slate-300 text-slate-700"}`}
                                        onClick={() => setHoldingDetailTab(ticker, "dividends")}
                                      >
                                        Dividends
                                      </button>
                                      <Button
                                        size="sm"
                                        variant="outline"
                                        onClick={() => openAddTxForTicker(ticker, row.market_price)}
                                      >
                                        Add Transaction
                                      </Button>
                                    </div>

                                    {detailTab === "share_lots" && (
                                      <div className="overflow-x-auto">
                                        <table className="w-full text-xs border-collapse">
                                          <thead>
                                            <tr className="border-b">
                                              <th className="text-left py-1">Date</th>
                                              <th className="text-left py-1">Shares Open</th>
                                              <th className="text-left py-1">Cost/Share</th>
                                              <th className="text-left py-1">Total Cost</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {tickerLots.length === 0 ? (
                                              <tr><td colSpan={4} className="py-2 text-slate-500">No open lots for {ticker}.</td></tr>
                                            ) : tickerLots.map((lot) => (
                                              <tr key={lot.id} className="border-b">
                                                <td className="py-1">{lot.trade_date}</td>
                                                <td className="py-1">{fmtNumber(lot.shares_open, 6)}</td>
                                                <td className="py-1">{fmtCurrency(lot.price, displayCurrency)}</td>
                                                <td className="py-1">{fmtCurrency(Number(lot.shares_open) * Number(lot.price), displayCurrency)}</td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>
                                    )}

                                    {detailTab === "transactions" && (
                                      <div className="overflow-x-auto">
                                        <table className="w-full text-xs border-collapse">
                                          <thead>
                                            <tr className="border-b">
                                              <th className="text-left py-1">Date</th>
                                              <th className="text-left py-1">Type</th>
                                              <th className="text-left py-1">Shares</th>
                                              <th className="text-left py-1">Cost/Share</th>
                                              <th className="text-left py-1">Total</th>
                                              <th className="text-left py-1">Actions</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {tickerTx.length === 0 ? (
                                              <tr><td colSpan={6} className="py-2 text-slate-500">No transactions for {ticker}.</td></tr>
                                            ) : tickerTx.map((tx) => (
                                              <tr key={tx.id} className="border-b">
                                                <td className="py-1">{tx.trade_date}</td>
                                                <td className="py-1">{tx.type}</td>
                                                <td className="py-1">{fmtNumber(tx.shares, 6)}</td>
                                                <td className="py-1">{fmtCurrency(tx.price, displayCurrency)}</td>
                                                <td className="py-1">{fmtCurrency((Number(tx.shares || 0) || 0) * (Number(tx.price || 0) || 0), displayCurrency)}</td>
                                                <td className="py-1">
                                                  <div className="flex gap-1">
                                                    <Button size="sm" variant="outline" onClick={() => openEditTx(tx)}>Edit</Button>
                                                    <Button size="sm" variant="outline" onClick={() => deleteTx(tx.id)} disabled={deletingTxId === tx.id}>
                                                      {deletingTxId === tx.id ? "Deleting..." : "Delete"}
                                                    </Button>
                                                  </div>
                                                </td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>
                                    )}

                                    {detailTab === "dividends" && (
                                      <div className="overflow-x-auto">
                                        <table className="w-full text-xs border-collapse">
                                          <thead>
                                            <tr className="border-b">
                                              <th className="text-left py-1">Date</th>
                                              <th className="text-left py-1">Dividend/Share</th>
                                              <th className="text-left py-1">Quantity</th>
                                              <th className="text-left py-1">Total Amount</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {tickerDivs.length === 0 ? (
                                              <tr><td colSpan={4} className="py-2 text-slate-500">No dividends for {ticker}.</td></tr>
                                            ) : tickerDivs.map((tx) => {
                                              const meta = tx.metadata || {};
                                              const perShare = meta?.dividend_per_share_native;
                                              const qty = meta?.entitled_shares ?? tx.shares;
                                              const total = meta?.net_amount_base ?? ((Number(tx.shares || 0) || 0) * (Number(tx.price || 0) || 0));
                                              return (
                                                <tr key={tx.id} className="border-b">
                                                  <td className="py-1">{tx.trade_date}</td>
                                                  <td className="py-1">{perShare == null ? "-" : fmtNumber(perShare, 4)}</td>
                                                  <td className="py-1">{qty == null ? "-" : fmtNumber(qty, 4)}</td>
                                                  <td className="py-1">{fmtCurrency(total, displayCurrency)}</td>
                                                </tr>
                                              );
                                            })}
                                          </tbody>
                                        </table>
                                      </div>
                                    )}
                                  </div>
                                </td>
                              </tr>
                            )}
                          </React.Fragment>
                        );
                      })
                    )}
                  </tbody>
                </table>
              </div>
              <details className="rounded-md border border-slate-200 dark:border-slate-800 p-3">
                <summary className="cursor-pointer text-sm font-semibold">Closed Positions</summary>
                <div className="mt-3 overflow-x-auto">
                  <table className="w-full text-sm border-collapse">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left py-2">Ticker</th>
                        <th className="text-left py-2">Open date</th>
                        <th className="text-left py-2">Close date</th>
                        <th className="text-left py-2">Holding period</th>
                        <th className="text-left py-2">Cost basis</th>
                        <th className="text-left py-2">Proceeds</th>
                        <th className="text-left py-2">Dividends</th>
                        <th className="text-left py-2">Realized gain $</th>
                        <th className="text-left py-2">Realized gain %</th>
                        <th className="text-left py-2">FX component</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedClosedRows.length === 0 ? (
                        <tr><td colSpan={10} className="py-3 text-slate-500">No closed positions.</td></tr>
                      ) : sortedClosedRows.map((row) => (
                        <tr key={`${row.ticker}-${row.close_date}`} className="border-b hover:bg-slate-50">
                          <td className="py-2 font-mono">{row.ticker}</td>
                          <td className="py-2">{row.open_date || "--"}</td>
                          <td className="py-2">{row.close_date}</td>
                          <td className="py-2">{row.holding_period_days}</td>
                          <td className="py-2">{fmtCurrency(row.total_cost_basis, displayCurrency)}</td>
                          <td className="py-2">{fmtCurrency(row.total_proceeds, displayCurrency)}</td>
                          <td className="py-2">{fmtCurrency(row.total_dividends, displayCurrency)}</td>
                          <td className={`py-2 ${Number(row.realized_gain || 0) >= 0 ? "text-emerald-700" : "text-red-600"}`}>
                            {fmtCurrency(row.realized_gain, displayCurrency)}
                          </td>
                          <td className={`py-2 ${Number(row.realized_gain_pct || 0) >= 0 ? "text-emerald-700" : "text-red-600"}`}>
                            {fmtPercent(row.realized_gain_pct)}
                          </td>
                          <td className={`py-2 ${Number(row.fx_component || 0) >= 0 ? "text-emerald-700" : "text-red-600"}`}>
                            {fmtCurrency(row.fx_component, displayCurrency)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </details>
              <details>
                <summary className="cursor-pointer text-base font-semibold">Processing Metadata</summary>
                <div className="mt-3 grid md:grid-cols-2 gap-3 text-sm">
                  <div><p className="text-slate-500">Processing Run ID</p><p className="font-mono break-all">{metadata?.run_id ?? result?.run_id ?? "-"}</p></div>
                  <div><p className="text-slate-500">Input Hash</p><p className="font-mono" title={metadata?.input_hash || result?.input_hash || "-"}>{shortHash(metadata?.input_hash || result?.input_hash)}</p></div>
                  <div><p className="text-slate-500">Engine Version</p><p className="font-medium">{metadata?.engine_version ?? result?.engine_version ?? "-"}</p></div>
                  <div><p className="text-slate-500">Warnings Count</p><p className="font-medium">{warningCount}</p></div>
                  <div><p className="text-slate-500">Corrections Count</p><p className="font-medium">{correctionCount}</p></div>
                  <div><p className="text-slate-500">Prior Close Fallback Count</p><p className="font-medium">{fallbackCount}</p></div>
                  <div><p className="text-slate-500">Started At</p><p className="font-medium">{metadata?.started_at ?? "-"}</p></div>
                  <div><p className="text-slate-500">Finished At</p><p className="font-medium">{metadata?.finished_at ?? "-"}</p></div>
                </div>
              </details>
            </div>
          )}

          {activeTab === "attribution" && (
            <AttributionPanel attribution={valuationAttribution} diff={valuationDiff} performanceBreakdown={performanceBreakdown} />
          )}

          {activeTab === "coverage" && (
            <div className="overflow-x-auto">
              <table className="w-full text-sm border-collapse">
                <thead><tr className="border-b"><th className="text-left py-2">Ticker</th><th className="text-left py-2">Status</th><th className="text-left py-2">Fallback Days</th><th className="text-left py-2">First Missing Date</th><th className="text-left py-2">Last Missing Date</th></tr></thead>
                <tbody>
                  {coverageRows.length === 0 ? <tr><td colSpan={5} className="py-3 text-slate-500">No structured coverage data available.</td></tr> : coverageRows.map((row) => (
                    <tr key={`${row.ticker}-${row.status}`} className="border-b">
                      <td className="py-2 font-mono">{row.ticker}</td>
                      <td className="py-2"><span className={`text-xs px-2 py-1 rounded border ${statusColor(row.status)}`}>{row.status}</span></td>
                      <td className="py-2">{row.fallback_days ?? 0}</td>
                      <td className="py-2">{row.first_missing_date || "-"}</td>
                      <td className="py-2">{row.last_missing_date || "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {activeTab === "corrections" && (
            <div className="overflow-x-auto">
              <table className="w-full text-sm border-collapse">
                <thead><tr className="border-b"><th className="text-left py-2">Ticker</th><th className="text-left py-2">Event Type</th><th className="text-left py-2">Date</th><th className="text-left py-2">Original Shares</th><th className="text-left py-2">Corrected Shares</th><th className="text-left py-2">Delta %</th><th className="text-left py-2">Triggered By</th><th className="text-left py-2">Run ID</th></tr></thead>
                <tbody>
                  {corrections.length === 0 ? <tr><td colSpan={8} className="py-3 text-slate-500">No correction events.</td></tr> : corrections.map((row, idx) => (
                    <tr key={`${row.run_id}-${row.ticker}-${idx}`} className="border-b">
                      <td className="py-2 font-mono">{row.ticker}</td>
                      <td className="py-2">{row.event_type}</td>
                      <td className="py-2">{row.date || "-"}</td>
                      <td className="py-2">{row.original_shares}</td>
                      <td className="py-2">{row.corrected_shares}</td>
                      <td className="py-2">{row.delta_pct == null ? "-" : `${Number(row.delta_pct).toFixed(4)}%`}</td>
                      <td className="py-2">{row.triggered_by || "policy"}</td>
                      <td className="py-2 font-mono">{row.run_id}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {activeTab === "transactions" && (
            <div className="space-y-3">
              <div className="flex justify-between">
                <h2 className="text-lg font-semibold">Transactions</h2>
                <Button onClick={openAddTx}>Add Transaction</Button>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm border-collapse">
                  <thead><tr className="border-b"><th className="text-left py-2">Date</th><th className="text-left py-2">Ticker</th><th className="text-left py-2">Type</th><th className="text-left py-2">Shares</th><th className="text-left py-2">Price</th><th className="text-left py-2">Total</th><th className="text-left py-2">Actions</th></tr></thead>
                  <tbody>
                    {transactions.length === 0 ? <tr><td colSpan={7} className="py-3 text-slate-500">No transactions.</td></tr> : transactions.map((tx) => (
                      <tr key={tx.id} className="border-b">
                        <td className="py-2">{tx.trade_date}</td>
                        <td className="py-2 font-mono">{tx.ticker}</td>
                        <td className="py-2">{tx.type}</td>
                        <td className="py-2">{tx.shares}</td>
                        <td className="py-2">{fmtCurrency(tx.price, displayCurrency)}</td>
                        <td className="py-2">{fmtCurrency((Number(tx.shares || 0) || 0) * (Number(tx.price || 0) || 0), displayCurrency)}</td>
                        <td className="py-2">
                          <div className="flex gap-2">
                            <Button size="sm" variant="outline" onClick={() => openEditTx(tx)}>Edit</Button>
                            <Button size="sm" variant="outline" onClick={() => deleteTx(tx.id)} disabled={deletingTxId === tx.id}>
                              {deletingTxId === tx.id ? "Deleting..." : "Delete"}
                            </Button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

        </div>
      )}

      {showTxModal && (
        <div className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4">
          <div className="bg-white dark:bg-slate-950 rounded-lg border w-full max-w-2xl p-4 space-y-3">
            <h3 className="text-lg font-semibold">{editTxId ? "Edit Transaction" : "Add Transaction"}</h3>
            <div className="grid md:grid-cols-2 gap-2">
              <div>
                <label className="text-sm">Ticker</label>
                <input
                  list="ticker-options"
                  value={txForm.ticker}
                  onChange={(e) => setTxForm((p) => ({ ...p, ticker: e.target.value }))}
                  className="w-full border rounded px-2 py-1"
                  disabled={Boolean(editTxId)}
                />
                <datalist id="ticker-options">
                  {[...new Set([...knownTickers, ...transactions.map((t) => t.ticker)])].map((sym) => (
                    <option key={sym} value={sym} />
                  ))}
                </datalist>
                {editTxId && <p className="text-xs text-slate-500 mt-1">Ticker cannot be edited.</p>}
              </div>
              <div>
                <label className="text-sm">Type</label>
                <select value={txForm.type} onChange={(e) => setTxForm((p) => ({ ...p, type: e.target.value }))} className="w-full border rounded px-2 py-1" disabled={Boolean(editTxId)}>
                  <option>Buy</option>
                  <option>Sell</option>
                  <option>Dividend</option>
                </select>
                {editTxId && <p className="text-xs text-slate-500 mt-1">Transaction type cannot be edited.</p>}
              </div>
              <div>
                <label className="text-sm">Date</label>
                <input type="date" value={txForm.trade_date} onChange={(e) => setTxForm((p) => ({ ...p, trade_date: e.target.value }))} className="w-full border rounded px-2 py-1" />
              </div>
              <div>
                <label className="text-sm">Currency</label>
                <input value={txForm.currency} onChange={(e) => setTxForm((p) => ({ ...p, currency: e.target.value.toUpperCase() }))} className="w-full border rounded px-2 py-1" />
              </div>
              <div>
                <label className="text-sm">Shares</label>
                <input type="number" step="any" value={txForm.shares} onChange={(e) => setTxForm((p) => ({ ...p, shares: e.target.value }))} className="w-full border rounded px-2 py-1" />
              </div>
              <div>
                <label className="text-sm">Price</label>
                <input type="number" step="any" value={txForm.price} onChange={(e) => setTxForm((p) => ({ ...p, price: e.target.value }))} className="w-full border rounded px-2 py-1" />
              </div>
              <div className="md:col-span-2">
                <label className="text-sm">Note</label>
                <input value={txForm.note} onChange={(e) => setTxForm((p) => ({ ...p, note: e.target.value }))} className="w-full border rounded px-2 py-1" />
              </div>
            </div>
            <div className="flex justify-end gap-2">
              <Button variant="outline" onClick={() => setShowTxModal(false)} disabled={isSavingTx}>Cancel</Button>
              <Button onClick={saveTx} disabled={isSavingTx}>{isSavingTx ? "Saving..." : "Save"}</Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
