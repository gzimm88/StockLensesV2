import React, { useState, useEffect, useCallback } from "react";
import { Link } from "react-router-dom";
import { Bell, X, ArrowDown, ArrowUp, ArrowRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { AlertNotification } from "@/api/entities";

function fmtMoney(v) {
  if (v == null || !Number.isFinite(Number(v))) return "—";
  return `$${Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function relTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  const diff = Date.now() - d.getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

export default function TriggerAlertsWidget() {
  const [alerts, setAlerts] = useState([]);

  const load = useCallback(async () => {
    try {
      const res = await AlertNotification.list({ dismissed: false, limit: 5 });
      setAlerts(res?.data?.alerts || []);
    } catch {
      // silent
    }
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, 60_000);
    return () => clearInterval(id);
  }, [load]);

  const handleDismiss = async (id) => {
    try {
      await AlertNotification.dismiss(id);
      setAlerts((prev) => prev.filter((a) => a.id !== id));
    } catch {}
  };

  if (!alerts.length) return null;

  return (
    <div className="rounded-lg border border-indigo-200 dark:border-indigo-900 bg-indigo-50 dark:bg-indigo-950/30">
      <div className="flex items-center justify-between px-4 py-2 border-b border-indigo-200 dark:border-indigo-900">
        <div className="flex items-center gap-2 text-sm font-medium text-indigo-800 dark:text-indigo-200">
          <Bell className="w-4 h-4" /> Triggered Alerts
          <Badge className="bg-indigo-200 text-indigo-800 hover:bg-indigo-200 dark:bg-indigo-800 dark:text-indigo-100">
            {alerts.length}
          </Badge>
        </div>
        <Link
          to="/notifications"
          className="text-xs text-indigo-700 dark:text-indigo-300 flex items-center gap-1 hover:underline"
        >
          View all <ArrowRight className="w-3 h-3" />
        </Link>
      </div>
      <div className="divide-y divide-indigo-100 dark:divide-indigo-900">
        {alerts.map((a) => {
          const isBuy = a.alert_type === "buy";
          const Icon = isBuy ? ArrowDown : ArrowUp;
          const color = isBuy ? "text-emerald-700 dark:text-emerald-300" : "text-red-700 dark:text-red-300";
          return (
            <div key={a.id} className="flex items-center justify-between gap-3 px-4 py-2 text-sm">
              <div className="flex items-center gap-3 min-w-0 flex-1">
                <Icon className={`w-4 h-4 flex-shrink-0 ${color}`} />
                <span className="font-semibold text-slate-800 dark:text-slate-200 font-mono">
                  {a.ticker_symbol}
                </span>
                <Badge
                  variant="outline"
                  className={`text-xs ${isBuy ? "border-emerald-300 text-emerald-700 dark:text-emerald-300" : "border-red-300 text-red-700 dark:text-red-300"}`}
                >
                  {isBuy ? "BUY" : "SELL"}
                </Badge>
                <span className="text-slate-600 dark:text-slate-400 text-xs truncate">
                  hit {fmtMoney(a.triggered_price)} (threshold {fmtMoney(a.threshold_price)})
                  {a.snapshot_name && <span className="text-slate-400"> · {a.snapshot_name}</span>}
                </span>
              </div>
              <div className="flex items-center gap-2 flex-shrink-0">
                <span className="text-xs text-slate-500">{relTime(a.triggered_at)}</span>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6"
                  title="Dismiss from dashboard"
                  onClick={() => handleDismiss(a.id)}
                >
                  <X className="w-3.5 h-3.5" />
                </Button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
