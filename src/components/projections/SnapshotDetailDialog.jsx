import React, { useMemo } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Camera, ArrowDown, ArrowUp, Calendar } from "lucide-react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";

function fmtMoney(v) {
  if (v == null || !Number.isFinite(Number(v))) return "—";
  return `$${Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}
function fmtPct(v, decimals = 1) {
  if (v == null || !Number.isFinite(Number(v))) return "—";
  return `${(v * 100).toFixed(decimals)}%`;
}
function fmtPctVal(v, decimals = 2) {
  if (v == null || !Number.isFinite(Number(v))) return "—";
  return `${v > 0 ? "+" : ""}${Number(v).toFixed(decimals)}%`;
}
function fmtDateTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleString(undefined, { year: "numeric", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

function statusBadge(status) {
  switch (status) {
    case "active":
      return <Badge className="bg-blue-100 text-blue-800 hover:bg-blue-100 dark:bg-blue-950 dark:text-blue-200">Active</Badge>;
    case "buy_triggered":
      return <Badge className="bg-emerald-100 text-emerald-800 hover:bg-emerald-100 dark:bg-emerald-950 dark:text-emerald-200">Buy triggered</Badge>;
    case "sell_triggered":
      return <Badge className="bg-red-100 text-red-800 hover:bg-red-100 dark:bg-red-950 dark:text-red-200">Sell triggered</Badge>;
    case "archived":
      return <Badge variant="outline">Archived</Badge>;
    default:
      return <Badge variant="outline">{status}</Badge>;
  }
}

function Field({ label, value, mono = false, color }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wide text-slate-500 dark:text-slate-400">{label}</span>
      <span className={`${mono ? "font-mono" : ""} text-sm font-semibold ${color || "text-slate-800 dark:text-slate-200"}`}>
        {value}
      </span>
    </div>
  );
}

export default function SnapshotDetailDialog({ open, onOpenChange, snapshot, currentPrice }) {
  const inputs = snapshot?.inputs || {};
  const outputs = snapshot?.outputs || {};
  const triggers = snapshot?.triggers || {};
  const yearly = snapshot?.yearly_data || null;

  const chartData = useMemo(() => {
    if (!yearly?.years || !yearly.epsPath) return [];
    const scenarioKey = inputs.scenario === "trend" ? "current" : (inputs.scenario || "current");
    const pricePath = yearly.pricePaths?.[scenarioKey] || yearly.pricePaths?.current || [];
    const targetCagr = inputs.target_cagr;
    const requiredEntry = outputs.required_entry;
    const targetBase =
      requiredEntry != null && Number.isFinite(requiredEntry) && requiredEntry > 0
        ? requiredEntry
        : inputs.current_price;
    return yearly.years.map((y, i) => ({
      year: y,
      projected: pricePath[i] != null ? Number(pricePath[i]) : null,
      target:
        targetCagr != null && targetBase
          ? Number((Number(targetBase) * Math.pow(1 + Number(targetCagr), y)).toFixed(2))
          : null,
    }));
  }, [yearly, inputs, outputs]);

  if (!snapshot) return null;

  const buyGap =
    currentPrice && triggers.buy_trigger_price
      ? ((triggers.buy_trigger_price - currentPrice) / currentPrice) * 100
      : null;
  const sellGap =
    currentPrice && triggers.sell_trigger_price
      ? ((triggers.sell_trigger_price - currentPrice) / currentPrice) * 100
      : null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Camera className="w-4 h-4 text-indigo-500" />
            <span className="font-mono">{snapshot.ticker_symbol}</span>
            <span className="text-slate-400 font-normal">·</span>
            <span className="text-base font-normal text-slate-700 dark:text-slate-300">{snapshot.name}</span>
            {statusBadge(snapshot.status)}
          </DialogTitle>
          <DialogDescription className="flex items-center gap-3 text-xs">
            <span className="inline-flex items-center gap-1">
              <Calendar className="w-3 h-3" /> Saved {fmtDateTime(snapshot.created_at)}
            </span>
            {snapshot.updated_at && snapshot.updated_at !== snapshot.created_at && (
              <span className="text-slate-400">· Updated {fmtDateTime(snapshot.updated_at)}</span>
            )}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-5">
          {snapshot.notes && (
            <div className="rounded-lg border border-slate-200 dark:border-slate-700 p-3 text-xs text-slate-600 dark:text-slate-400 italic">
              {snapshot.notes}
            </div>
          )}

          {/* Triggers */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="rounded-lg border border-emerald-200 dark:border-emerald-800 bg-emerald-50 dark:bg-emerald-950/30 p-3">
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs font-medium text-emerald-700 dark:text-emerald-300 flex items-center gap-1">
                  <ArrowDown className="w-3 h-3" /> BUY trigger
                </span>
                {currentPrice && (
                  <span className="text-[10px] text-slate-500">
                    Gap: {buyGap == null ? "—" : fmtPctVal(buyGap)}
                  </span>
                )}
              </div>
              <div className="text-2xl font-bold font-mono text-emerald-800 dark:text-emerald-200">
                {fmtMoney(triggers.buy_trigger_price)}
              </div>
              <p className="text-[11px] text-slate-500 mt-1">Fires when price ≤ this</p>
            </div>
            <div className="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-950/30 p-3">
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs font-medium text-red-700 dark:text-red-300 flex items-center gap-1">
                  <ArrowUp className="w-3 h-3" /> SELL trigger
                </span>
                {currentPrice && (
                  <span className="text-[10px] text-slate-500">
                    Gap: {sellGap == null ? "—" : fmtPctVal(sellGap)}
                  </span>
                )}
              </div>
              <div className="text-2xl font-bold font-mono text-red-800 dark:text-red-200">
                {fmtMoney(triggers.sell_trigger_price)}
              </div>
              <p className="text-[11px] text-slate-500 mt-1">
                Fires when price ≥ this · Overvalued % = {triggers.overvalued_pct}%
              </p>
            </div>
          </div>

          {/* Trigger fire state */}
          {triggers.triggered_at && (
            <div className="rounded-lg border border-amber-200 dark:border-amber-800 bg-amber-50 dark:bg-amber-950/30 p-3 text-sm text-amber-800 dark:text-amber-200">
              <strong>{triggers.triggered_type?.toUpperCase()}</strong> trigger fired at{" "}
              {fmtMoney(triggers.triggered_price)} on {fmtDateTime(triggers.triggered_at)}.
            </div>
          )}

          {/* Frozen Inputs */}
          <div>
            <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-2">Frozen Inputs</h3>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
              <Field label="Price at snapshot" value={fmtMoney(inputs.current_price)} mono />
              <Field label="EPS at snapshot" value={fmtMoney(inputs.current_eps)} mono />
              <Field label="Growth rate" value={fmtPct(inputs.growth_rate)} mono />
              <Field label="Target CAGR" value={fmtPct(inputs.target_cagr)} mono />
              <Field label="Years" value={inputs.years ?? "—"} mono />
              <Field label="Scenario" value={inputs.scenario ?? "—"} />
              <Field label="Current P/E" value={inputs.current_pe != null ? `${Number(inputs.current_pe).toFixed(2)}x` : "—"} mono />
              <Field
                label="P/E band (bear/mid/bull)"
                value={`${inputs.pe_bear ?? "—"} / ${inputs.pe_mid ?? "—"} / ${inputs.pe_bull ?? "—"}`}
                mono
              />
              {inputs.pe_custom_terminal != null && (
                <Field label="Custom terminal P/E" value={`${Number(inputs.pe_custom_terminal).toFixed(2)}x`} mono />
              )}
            </div>
          </div>

          {/* Frozen Outputs */}
          <div>
            <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-2">Frozen Outputs</h3>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-3 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
              <Field label="Terminal EPS" value={fmtMoney(outputs.terminal_eps)} mono />
              <Field label="Exit P/E" value={outputs.exit_pe != null ? `${Number(outputs.exit_pe).toFixed(2)}x` : "—"} mono />
              <Field label="Terminal price" value={fmtMoney(outputs.terminal_price)} mono color="text-blue-700 dark:text-blue-300" />
              <Field label="Implied CAGR" value={fmtPct(outputs.implied_cagr)} mono color="text-emerald-700 dark:text-emerald-400" />
              <Field label="Required entry" value={fmtMoney(outputs.required_entry)} mono color="text-emerald-700 dark:text-emerald-400" />
              <Field
                label="Margin of safety"
                value={fmtPct(outputs.margin_of_safety)}
                mono
                color={outputs.margin_of_safety > 0 ? "text-emerald-700 dark:text-emerald-400" : "text-red-700 dark:text-red-400"}
              />
            </div>
          </div>

          {/* Yearly Evolution Chart */}
          {chartData.length > 0 && (
            <div>
              <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-2">
                Projection vs Target ({inputs.scenario || "current"} scenario)
              </h3>
              <div className="rounded-lg border border-slate-200 dark:border-slate-700 p-3">
                <div className="h-56">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                      <XAxis dataKey="year" stroke="#64748b" tick={{ fontSize: 11 }} />
                      <YAxis stroke="#64748b" tick={{ fontSize: 11 }} tickFormatter={(v) => `$${v.toFixed(0)}`} />
                      <Tooltip
                        formatter={(v, name) => [fmtMoney(v), name === "projected" ? "Projected" : "Target"]}
                        labelFormatter={(l) => `Year ${l}`}
                        contentStyle={{ fontSize: 11 }}
                      />
                      <Legend wrapperStyle={{ fontSize: 11 }} />
                      <Line
                        type="monotone"
                        dataKey="projected"
                        stroke="#0f172a"
                        strokeWidth={2}
                        dot={{ r: 3 }}
                        name="Projected price"
                      />
                      <Line
                        type="monotone"
                        dataKey="target"
                        stroke="#f59e0b"
                        strokeWidth={2}
                        strokeDasharray="5 5"
                        dot={{ r: 3 }}
                        name="Target (CAGR)"
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>
          )}

          {/* Yearly Evolution Table */}
          {yearly?.years && yearly.epsPath && (
            <div>
              <h3 className="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-2">Yearly Evolution</h3>
              <div className="rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
                <table className="w-full text-xs">
                  <thead className="bg-slate-50 dark:bg-slate-900">
                    <tr>
                      <th className="text-left px-3 py-2 font-medium text-slate-600 dark:text-slate-400">Year</th>
                      <th className="text-right px-3 py-2 font-medium text-slate-600 dark:text-slate-400">EPS</th>
                      <th className="text-right px-3 py-2 font-medium text-slate-600 dark:text-slate-400">P/E</th>
                      <th className="text-right px-3 py-2 font-medium text-slate-600 dark:text-slate-400">Price</th>
                      <th className="text-right px-3 py-2 font-medium text-slate-600 dark:text-slate-400">Target Path</th>
                    </tr>
                  </thead>
                  <tbody>
                    {chartData.map((row, i) => {
                      const scenarioKey = inputs.scenario === "trend" ? "current" : (inputs.scenario || "current");
                      const pe = yearly.pePaths?.[scenarioKey]?.[i];
                      const eps = yearly.epsPath[i];
                      return (
                        <tr key={row.year} className="border-t border-slate-100 dark:border-slate-800">
                          <td className="px-3 py-1.5 font-medium">{row.year}</td>
                          <td className="px-3 py-1.5 text-right font-mono">{fmtMoney(eps)}</td>
                          <td className="px-3 py-1.5 text-right font-mono">{pe != null ? `${Number(pe).toFixed(1)}x` : "—"}</td>
                          <td className="px-3 py-1.5 text-right font-mono">{fmtMoney(row.projected)}</td>
                          <td className="px-3 py-1.5 text-right font-mono text-amber-700 dark:text-amber-400">
                            {fmtMoney(row.target)}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
