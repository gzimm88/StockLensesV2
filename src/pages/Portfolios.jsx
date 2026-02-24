import React from "react";
import { Link } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { createPageUrl } from "@/utils";
import { getLatestPortfolioRunMetadata, listPortfolios } from "@/api/portfolio";

function runStatus(meta) {
  if (!meta) return "Never Processed";
  if ((meta.correction_event_count || 0) > 0) return "Corrections";
  if ((meta.warnings_count || 0) > 0) return "Warnings";
  return "OK";
}

export default function Portfolios() {
  const [rows, setRows] = React.useState([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

  React.useEffect(() => {
    (async () => {
      setLoading(true);
      setError("");
      try {
        const list = await listPortfolios();
        const portfolios = list?.data?.portfolios || [];
        const metas = await Promise.all(
          portfolios.map(async (p) => {
            try {
              const m = await getLatestPortfolioRunMetadata(p.id);
              return [p.id, m?.data || null];
            } catch {
              return [p.id, null];
            }
          })
        );
        const metaById = Object.fromEntries(metas);
        setRows(portfolios.map((p) => ({ ...p, meta: metaById[p.id] || null })));
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load portfolios.");
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">All Portfolios</h1>
        <Link to={createPageUrl("Portfolio")}>
          <Button>Back to Portfolio</Button>
        </Link>
      </div>
      <div className="rounded-xl border border-slate-200 bg-white p-6">
        {loading && <p className="text-sm text-slate-500">Loading...</p>}
        {error && <p className="text-sm text-red-600">{error}</p>}
        {!loading && !error && (
          <div className="overflow-x-auto">
            <table className="w-full text-sm border-collapse">
              <thead>
                <tr className="border-b">
                  <th className="text-left py-2">Name</th>
                  <th className="text-left py-2">Base Currency</th>
                  <th className="text-left py-2">Last NAV</th>
                  <th className="text-left py-2">Last Processed</th>
                  <th className="text-left py-2">Run Status</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.id} className="border-b">
                    <td className="py-2 font-medium">{r.name}</td>
                    <td className="py-2">{r.base_currency}</td>
                    <td className="py-2">{r.last_nav ?? "-"}</td>
                    <td className="py-2">{r.last_processed_at ?? "-"}</td>
                    <td className="py-2">{runStatus(r.meta)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
