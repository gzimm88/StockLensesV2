import React from "react";
import { Skeleton } from "@/components/ui/skeleton";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";

function fmt(val, decimals = 2) {
  if (val == null) return "--";
  return Number(val).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export default function MarketIndicesBar({ data, loading }) {
  if (loading || !data) {
    return (
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        {[1, 2, 3, 4].map((i) => (
          <Skeleton key={i} className="h-14 rounded-lg" />
        ))}
      </div>
    );
  }

  const indices = data?.indices || [];
  if (indices.length === 0) return null;

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
      {indices.map((idx) => {
        const hasPrice = idx.price != null;
        const hasChange = idx.change_pct != null;
        const isPositive = hasChange && idx.change_pct >= 0;
        const Icon = !hasChange ? Minus : isPositive ? TrendingUp : TrendingDown;
        const changeColor = !hasChange
          ? "text-slate-400"
          : isPositive
          ? "text-emerald-600 dark:text-emerald-400"
          : "text-red-600 dark:text-red-400";

        return (
          <div
            key={idx.symbol}
            className="flex items-center justify-between rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 px-3 py-2.5"
          >
            <div>
              <p className="text-xs font-medium text-slate-500 dark:text-slate-400">{idx.label}</p>
              <p className="text-sm font-semibold text-slate-800 dark:text-slate-100 font-mono">
                {hasPrice ? (idx.symbol === "EUR/USD" ? fmt(idx.price, 4) : `$${fmt(idx.price)}`) : "--"}
              </p>
            </div>
            {hasChange && (
              <div className={`flex items-center gap-0.5 text-xs font-medium ${changeColor}`}>
                <Icon className="w-3 h-3" />
                {isPositive ? "+" : ""}{fmt(idx.change_pct, 2)}%
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
