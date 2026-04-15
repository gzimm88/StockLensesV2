import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { History, ArrowDownLeft, ArrowUpRight, Banknote, Repeat } from "lucide-react";
import { formatDistanceToNow } from "date-fns";

function fmt(val, decimals = 2) {
  if (val == null) return "--";
  return Number(val).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

const TX_ICONS = {
  buy: ArrowDownLeft,
  sell: ArrowUpRight,
  dividend: Banknote,
  corporate_action: Repeat,
};

const TX_COLORS = {
  buy: "text-emerald-600 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-950",
  sell: "text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-950",
  dividend: "text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-950",
  corporate_action: "text-amber-600 dark:text-amber-400 bg-amber-50 dark:bg-amber-950",
};

function describeTransaction(tx) {
  const type = tx.tx_type || "buy";
  const shares = fmt(tx.shares, tx.shares % 1 === 0 ? 0 : 2);
  const price = fmt(tx.price);
  const ticker = tx.ticker || tx.ticker_symbol_normalized || "???";

  switch (type) {
    case "buy":
      return `Bought ${shares} shares of ${ticker} at $${price}`;
    case "sell":
      return `Sold ${shares} shares of ${ticker} at $${price}`;
    case "dividend":
      return `Dividend from ${ticker}: $${fmt(tx.gross_amount || tx.price)}`;
    case "corporate_action":
      return `Corporate action on ${ticker}`;
    default:
      return `${type} — ${ticker}`;
  }
}

export default function RecentActivityFeed({ data, loading }) {
  if (loading) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <History className="w-4 h-4" /> Recent Activity
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }

  const items = data || [];

  if (items.length === 0) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <History className="w-4 h-4" /> Recent Activity
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-slate-500 dark:text-slate-400">No recent activity.</p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base flex items-center gap-2">
          <History className="w-4 h-4" /> Recent Activity
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-1">
          {items.map((tx, idx) => {
            const type = tx.tx_type || "buy";
            const Icon = TX_ICONS[type] || ArrowDownLeft;
            const colorClass = TX_COLORS[type] || TX_COLORS.buy;
            const timeAgo = tx.trade_date
              ? formatDistanceToNow(new Date(tx.trade_date), { addSuffix: true })
              : "";

            return (
              <div
                key={tx.id || idx}
                className="flex items-center gap-3 py-2 px-1 -mx-1 hover:bg-slate-50 dark:hover:bg-slate-800/50 rounded"
              >
                <div className={`w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 ${colorClass}`}>
                  <Icon className="w-3.5 h-3.5" />
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-xs text-slate-700 dark:text-slate-300 truncate">
                    {describeTransaction(tx)}
                  </p>
                </div>
                <span className="text-[10px] text-slate-400 dark:text-slate-500 whitespace-nowrap flex-shrink-0">
                  {timeAgo}
                </span>
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}
