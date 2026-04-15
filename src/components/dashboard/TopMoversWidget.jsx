import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Activity, ArrowUp, ArrowDown } from "lucide-react";

function fmt(val, decimals = 2) {
  if (val == null) return "--";
  return Number(val).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export default function TopMoversWidget({ data, loading }) {
  if (loading) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Activity className="w-4 h-4" /> Top Movers
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-2">
          {[1, 2, 3, 4, 5].map((i) => (
            <Skeleton key={i} className="h-7 w-full" />
          ))}
        </CardContent>
      </Card>
    );
  }

  const movers = data || [];

  if (movers.length === 0) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Activity className="w-4 h-4" /> Top Movers
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-slate-500 dark:text-slate-400">No market data yet.</p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base flex items-center gap-2">
          <Activity className="w-4 h-4" /> Top Movers
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-1.5">
          {movers.slice(0, 7).map((item) => {
            const isPositive = item.change_pct >= 0;
            return (
              <div
                key={item.ticker}
                className="flex items-center justify-between py-1.5 px-1 -mx-1 hover:bg-slate-50 dark:hover:bg-slate-800/50 rounded text-xs"
              >
                <div className="flex items-center gap-2">
                  <span className="font-semibold text-slate-800 dark:text-slate-200">{item.ticker}</span>
                  <Badge variant="outline" className="text-[10px] px-1.5 py-0">
                    {item.source}
                  </Badge>
                </div>
                <span className={`flex items-center gap-1 font-mono font-medium ${
                  isPositive ? "text-emerald-600 dark:text-emerald-400" : "text-red-600 dark:text-red-400"
                }`}>
                  {isPositive ? <ArrowUp className="w-3 h-3" /> : <ArrowDown className="w-3 h-3" />}
                  {isPositive ? "+" : ""}{fmt(item.change_pct, 2)}%
                </span>
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}
