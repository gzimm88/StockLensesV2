import React from "react";

function fmt(value) {
  if (value === null || value === undefined) return "-";
  if (typeof value !== "number") return String(value);
  return value.toLocaleString(undefined, { maximumFractionDigits: 10 });
}

function hasIntegrityFailure(unexplainedDelta) {
  if (typeof unexplainedDelta !== "number") return false;
  return unexplainedDelta !== 0;
}

export default function AttributionPanel({ attribution, diff, performanceBreakdown }) {
  const data = attribution || {};
  const breakdown = data?.breakdown_by_ticker || {};
  const rows = Object.entries(breakdown);
  const integrityFailed = hasIntegrityFailure(data?.unexplained_delta);

  return (
    <div className="space-y-4" data-testid="attribution-panel">
      {integrityFailed && (
        <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-700" role="alert">
          Attribution integrity check failed.
        </div>
      )}

      <div className="grid md:grid-cols-4 gap-3 text-sm">
        <div>
          <p className="text-slate-500">Previous NAV</p>
          <p className="font-medium">{fmt(data?.previous_nav)}</p>
        </div>
        <div>
          <p className="text-slate-500">Current NAV</p>
          <p className="font-medium">{fmt(data?.current_nav)}</p>
        </div>
        <div>
          <p className="text-slate-500">Total Explained Delta</p>
          <p className="font-medium">{fmt(data?.total_explained_delta)}</p>
        </div>
        <div>
          <p className="text-slate-500">Unexplained Delta</p>
          <p className={`font-medium ${integrityFailed ? "text-red-700" : ""}`}>{fmt(data?.unexplained_delta)}</p>
        </div>
      </div>

      <div className="rounded-md border border-slate-200 dark:border-slate-800 p-3">
        <h3 className="text-sm font-semibold mb-2">Component Breakdown</h3>
        <div className="grid md:grid-cols-4 gap-2 text-sm">
          <div>
            <p className="text-slate-500">Transaction impact</p>
            <p className="font-medium">{fmt(data?.transaction_delta)}</p>
          </div>
          <div>
            <p className="text-slate-500">Price impact</p>
            <p className="font-medium">{fmt(data?.price_delta)}</p>
          </div>
          <div>
            <p className="text-slate-500">FX impact</p>
            <p className="font-medium">{fmt(data?.fx_delta)}</p>
          </div>
          <div>
            <p className="text-slate-500">Corporate action impact</p>
            <p className="font-medium">{fmt(data?.corporate_action_delta)}</p>
          </div>
        </div>
      </div>

      <div className="rounded-md border border-slate-200 dark:border-slate-800 p-3">
        <h3 className="text-sm font-semibold mb-2">Dividends</h3>
        <div className="grid md:grid-cols-3 gap-2 text-sm">
          <div>
            <p className="text-slate-500">Total dividend gain</p>
            <p className="font-medium">{fmt(performanceBreakdown?.dividend_gain ?? 0)}</p>
          </div>
          <div>
            <p className="text-slate-500">Total gain (incl. dividends)</p>
            <p className="font-medium">{fmt(performanceBreakdown?.total_gain)}</p>
          </div>
          <div>
            <p className="text-slate-500">Total gain %</p>
            <p className="font-medium">{fmt(performanceBreakdown?.total_gain_pct)}</p>
          </div>
        </div>
      </div>

      {diff?.previous_snapshot_id && (
        <div className="rounded-md border border-slate-200 dark:border-slate-800 p-3 text-sm">
          <h3 className="text-sm font-semibold mb-2">Prior Snapshot Comparison</h3>
          <div className="grid md:grid-cols-3 gap-2">
            <div>
              <p className="text-slate-500">Previous NAV</p>
              <p className="font-medium">{fmt(diff?.previous_nav)}</p>
            </div>
            <div>
              <p className="text-slate-500">Latest NAV</p>
              <p className="font-medium">{fmt(diff?.latest_nav)}</p>
            </div>
            <div>
              <p className="text-slate-500">NAV Delta</p>
              <p className="font-medium">{fmt(diff?.nav_delta)}</p>
            </div>
          </div>
        </div>
      )}

      <div className="overflow-x-auto">
        <table className="w-full text-sm border-collapse">
          <thead>
            <tr className="border-b">
              <th className="text-left py-2">Ticker</th>
              <th className="text-left py-2">Price delta</th>
              <th className="text-left py-2">FX delta</th>
              <th className="text-left py-2">Transaction delta</th>
              <th className="text-left py-2">Corporate action delta</th>
              <th className="text-left py-2">Total delta</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={6} className="py-3 text-slate-500">No attribution breakdown available.</td>
              </tr>
            ) : (
              rows.map(([ticker, row]) => (
                <tr key={ticker} className="border-b">
                  <td className="py-2 font-mono">{ticker}</td>
                  <td className="py-2">{fmt(row?.price_delta)}</td>
                  <td className="py-2">{fmt(row?.fx_delta)}</td>
                  <td className="py-2">{fmt(row?.transaction_delta)}</td>
                  <td className="py-2">{fmt(row?.corporate_action_delta)}</td>
                  <td className="py-2">{fmt(row?.total_delta)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
