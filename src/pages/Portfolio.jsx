import React from "react";
import { Link } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { createPageUrl } from "@/utils";
import {
  createPortfolio,
  getLastPortfolioRun,
  getLatestPortfolioRunMetadata,
  importPortfolioCsv,
  listPortfolios,
  processPortfolio,
} from "@/api/portfolio";

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

export default function Portfolio() {
  const [portfolios, setPortfolios] = React.useState([]);
  const [selectedPortfolioId, setSelectedPortfolioId] = React.useState("");
  const [selectedFile, setSelectedFile] = React.useState(null);
  const [isLoading, setIsLoading] = React.useState(true);
  const [isProcessing, setIsProcessing] = React.useState(false);
  const [isImporting, setIsImporting] = React.useState(false);
  const [strictMode, setStrictMode] = React.useState(false);
  const [error, setError] = React.useState("");
  const [result, setResult] = React.useState(null);
  const [metadata, setMetadata] = React.useState(null);
  const [activeTab, setActiveTab] = React.useState("coverage");
  const [showCreate, setShowCreate] = React.useState(false);
  const [createName, setCreateName] = React.useState("");
  const [createCurrency, setCreateCurrency] = React.useState("USD");

  const selectedPortfolio = portfolios.find((p) => p.id === selectedPortfolioId) || null;

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

  const loadPortfolioState = React.useCallback(
    async (portfolioId) => {
      if (!portfolioId) {
        setResult(null);
        setMetadata(null);
        return;
      }
      const [last, latestMeta] = await Promise.all([
        getLastPortfolioRun(portfolioId),
        getLatestPortfolioRunMetadata(portfolioId),
      ]);
      setResult(last?.data || null);
      setMetadata(latestMeta?.data || null);
    },
    []
  );

  React.useEffect(() => {
    let active = true;
    (async () => {
      setIsLoading(true);
      try {
        await loadPortfolios();
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
    // Isolation on switch.
    setResult(null);
    setMetadata(null);
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
      await loadPortfolioState(selectedPortfolioId);
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
      await loadPortfolios();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Portfolio processing failed.");
    } finally {
      setIsProcessing(false);
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
            <Link to="/portfolios">
              <Button variant="outline">All Portfolios</Button>
            </Link>
          </div>
        </div>

        {showCreate && (
          <div className="rounded-md border border-slate-200 p-3 space-y-2">
            <div className="grid md:grid-cols-2 gap-2">
              <input
                placeholder="Portfolio name"
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                className="border rounded-md px-2 py-1 text-sm"
              />
              <input
                placeholder="Base currency"
                value={createCurrency}
                onChange={(e) => setCreateCurrency(e.target.value.toUpperCase())}
                className="border rounded-md px-2 py-1 text-sm"
              />
            </div>
            <Button onClick={handleCreatePortfolio}>Create</Button>
          </div>
        )}

        <div className="space-y-2">
          <label htmlFor="portfolio-csv" className="text-sm font-medium text-slate-700 dark:text-slate-300">
            Portfolio CSV Upload
          </label>
          <input
            id="portfolio-csv"
            type="file"
            accept=".csv"
            onChange={handleFileChange}
            className="block w-full text-sm file:mr-4 file:rounded-md file:border-0 file:bg-slate-900 file:px-3 file:py-2 file:text-white dark:file:bg-slate-200 dark:file:text-slate-900"
          />
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
            {isProcessing ? "Processing..." : "Process Portfolio"}
          </Button>
          {error && (
            <Button variant="outline" onClick={handleProcess} disabled={isProcessing || !selectedPortfolioId}>
              Retry
            </Button>
          )}
        </div>

        {isLoading && <p className="text-xs text-slate-500 dark:text-slate-400">Loading portfolios...</p>}
        {error && <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-700">{error}</div>}
      </div>

      {!result && !metadata && selectedPortfolio && (
        <div className="rounded-xl border border-slate-200 bg-white p-6">
          <h2 className="text-lg font-semibold">No transactions yet</h2>
          <p className="text-sm text-slate-600 mt-1">Import CSV or add manually.</p>
        </div>
      )}

      {(result || metadata) && (
        <>
          <div className="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 p-6 space-y-3">
            <div className="flex items-center justify-between gap-2">
              <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">Portfolio Summary</h2>
              <span
                className="text-xs px-2 py-1 rounded border border-slate-300 text-slate-700"
                title={`Results are fully reproducible from transaction ledger + price data snapshot. Input hash: ${metadata?.input_hash || result?.input_hash || "-"}`}
              >
                Deterministic Run
              </span>
            </div>
            {correctionCount > 0 && (
              <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-700">
                If corrections were applied, results may differ from raw transaction intent.
              </div>
            )}
            <div className="grid md:grid-cols-3 gap-3 text-sm">
              <div>
                <p className="text-slate-500 dark:text-slate-400">NAV (Total Equity)</p>
                <p className="font-medium">{result?.nav ?? "-"}</p>
              </div>
              <div>
                <p className="text-slate-500 dark:text-slate-400">IRR</p>
                <p className="font-medium">{result?.irr ?? "-"}</p>
              </div>
              <div>
                <p className="text-slate-500 dark:text-slate-400">Generated At</p>
                <p className="font-medium">{result?.generated_at ?? "-"}</p>
              </div>
            </div>
          </div>

          <div className="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 p-6">
            <details>
              <summary className="cursor-pointer text-lg font-semibold text-slate-900 dark:text-slate-100">
                Processing Metadata
              </summary>
              <div className="mt-4 space-y-4">
                <div className="flex flex-wrap gap-2">
                  {correctionCount > 0 && (
                    <span className="text-xs px-2 py-1 rounded border border-red-300 text-red-700 bg-red-50">
                      Corrections: {correctionCount}
                    </span>
                  )}
                  {hasNonOkCoverage && (
                    <span className="text-xs px-2 py-1 rounded border border-amber-300 text-amber-700 bg-amber-50">
                      Coverage Alerts
                    </span>
                  )}
                </div>
                <div className="grid md:grid-cols-2 gap-3 text-sm">
                  <div>
                    <p className="text-slate-500">Processing Run ID</p>
                    <p className="font-mono break-all">{metadata?.run_id ?? result?.run_id ?? "-"}</p>
                  </div>
                  <div>
                    <p className="text-slate-500">Input Hash</p>
                    <p className="font-mono" title={metadata?.input_hash || result?.input_hash || "-"}>
                      {shortHash(metadata?.input_hash || result?.input_hash)}
                    </p>
                  </div>
                  <div>
                    <p className="text-slate-500">Engine Version</p>
                    <p className="font-medium">{metadata?.engine_version ?? result?.engine_version ?? "-"}</p>
                  </div>
                  <div>
                    <p className="text-slate-500">Warnings Count</p>
                    <p className="font-medium">{warningCount}</p>
                  </div>
                  <div>
                    <p className="text-slate-500">Corrections Count</p>
                    <p className="font-medium">{correctionCount}</p>
                  </div>
                  <div>
                    <p className="text-slate-500">Prior Close Fallback Count</p>
                    <p className="font-medium">{fallbackCount}</p>
                  </div>
                  <div>
                    <p className="text-slate-500">Started At</p>
                    <p className="font-medium">{metadata?.started_at ?? "-"}</p>
                  </div>
                  <div>
                    <p className="text-slate-500">Finished At</p>
                    <p className="font-medium">{metadata?.finished_at ?? "-"}</p>
                  </div>
                </div>
              </div>
            </details>
          </div>

          <div className="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 p-6 space-y-4">
            <div className="flex items-center gap-2">
              <button
                type="button"
                className={`px-3 py-1.5 text-sm rounded border ${
                  activeTab === "coverage" ? "bg-slate-900 text-white border-slate-900" : "border-slate-300"
                }`}
                onClick={() => setActiveTab("coverage")}
              >
                Coverage
              </button>
              {correctionCount > 0 && (
                <button
                  type="button"
                  className={`px-3 py-1.5 text-sm rounded border ${
                    activeTab === "corrections" ? "bg-slate-900 text-white border-slate-900" : "border-slate-300"
                  }`}
                  onClick={() => setActiveTab("corrections")}
                >
                  Corrections
                </button>
              )}
            </div>

            {activeTab === "coverage" && (
              <div className="overflow-x-auto">
                <table className="w-full text-sm border-collapse">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left py-2">Ticker</th>
                      <th className="text-left py-2">Status</th>
                      <th className="text-left py-2">Fallback Days</th>
                      <th className="text-left py-2">First Missing Date</th>
                      <th className="text-left py-2">Last Missing Date</th>
                    </tr>
                  </thead>
                  <tbody>
                    {coverageRows.length === 0 ? (
                      <tr>
                        <td colSpan={5} className="py-3 text-slate-500">No structured coverage data available.</td>
                      </tr>
                    ) : (
                      coverageRows.map((row) => (
                        <tr key={`${row.ticker}-${row.status}`} className="border-b">
                          <td className="py-2 font-mono">{row.ticker}</td>
                          <td className="py-2">
                            <span className={`text-xs px-2 py-1 rounded border ${statusColor(row.status)}`}>
                              {row.status}
                            </span>
                          </td>
                          <td className="py-2">{row.fallback_days ?? 0}</td>
                          <td className="py-2">{row.first_missing_date || "-"}</td>
                          <td className="py-2">{row.last_missing_date || "-"}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}

            {activeTab === "corrections" && correctionCount > 0 && (
              <div className="overflow-x-auto">
                <table className="w-full text-sm border-collapse">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left py-2">Ticker</th>
                      <th className="text-left py-2">Event Type</th>
                      <th className="text-left py-2">Date</th>
                      <th className="text-left py-2">Original Shares</th>
                      <th className="text-left py-2">Corrected Shares</th>
                      <th className="text-left py-2">Delta %</th>
                      <th className="text-left py-2">Triggered By</th>
                      <th className="text-left py-2">Run ID</th>
                    </tr>
                  </thead>
                  <tbody>
                    {corrections.map((row, idx) => (
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
          </div>
        </>
      )}
    </div>
  );
}
