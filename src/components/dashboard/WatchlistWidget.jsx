import React from "react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Eye, ArrowRight, TrendingDown, TrendingUp, Lock } from "lucide-react";

function fmt(val, decimals = 2) {
  if (val == null) return "--";
  return Number(val).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export default function WatchlistWidget({ data, loading }) {
  if (loading) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Eye className="w-4 h-4" /> Watchlist
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-2">
          {[1, 2, 3, 4].map((i) => (
            <Skeleton key={i} className="h-8 w-full" />
          ))}
        </CardContent>
      </Card>
    );
  }

  const items = data?.items || [];

  if (items.length === 0) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Eye className="w-4 h-4" /> Watchlist
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-slate-500 dark:text-slate-400">
            Add tickers to your watchlist to track entry price targets.
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base flex items-center justify-between">
          <span className="flex items-center gap-2">
            <Eye className="w-4 h-4" /> Watchlist
          </span>
          <Link to="/watchlist" className="text-xs text-blue-600 dark:text-blue-400 flex items-center gap-1 hover:underline">
            View all <ArrowRight className="w-3 h-3" />
          </Link>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-1">
          {/* Header */}
          <div className="grid grid-cols-4 gap-2 text-xs text-slate-500 dark:text-slate-400 pb-1 border-b border-slate-100 dark:border-slate-800">
            <span>Ticker</span>
            <span className="text-right">Price</span>
            <span className="text-right">Entry</span>
            <span className="text-right">Gap</span>
          </div>
          {items.slice(0, 8).map((item) => {
            const hasGap = item.gap_pct != null;
            const isBelow = hasGap && item.gap_pct < 0;
            const isNearEntry = hasGap && item.gap_pct >= -10 && item.gap_pct <= 0;
            return (
              <div
                key={item.symbol}
                className={`grid grid-cols-4 gap-2 text-xs py-1.5 rounded px-1 -mx-1 ${
                  isNearEntry
                    ? "bg-amber-50 dark:bg-amber-950/30 hover:bg-amber-100 dark:hover:bg-amber-950/50"
                    : "hover:bg-slate-50 dark:hover:bg-slate-800/50"
                }`}
              >
                <span className="font-semibold text-slate-800 dark:text-slate-200 truncate">
                  {item.symbol}
                </span>
                <span className="text-right text-slate-600 dark:text-slate-400 font-mono">
                  ${fmt(item.current_price)}
                </span>
                <span className="text-right text-slate-500 dark:text-slate-500 font-mono">
                  {item.entry_price ? (
                    <span className="inline-flex items-center justify-end gap-0.5">
                      {item.is_frozen && <Lock className="w-2.5 h-2.5 text-amber-500" title="Price frozen at projection save" />}
                      ${fmt(item.entry_price)}
                    </span>
                  ) : (
                    <span className="text-slate-400 italic">No target</span>
                  )}
                </span>
                <span className={`text-right font-mono font-medium flex items-center justify-end gap-0.5 ${
                  !hasGap ? "text-slate-400" : isBelow ? "text-emerald-600 dark:text-emerald-400" : "text-red-600 dark:text-red-400"
                }`}>
                  {hasGap ? (
                    <>
                      {isBelow ? <TrendingDown className="w-3 h-3" /> : <TrendingUp className="w-3 h-3" />}
                      {item.gap_pct > 0 ? "+" : ""}{fmt(item.gap_pct, 1)}%
                    </>
                  ) : "--"}
                </span>
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}
