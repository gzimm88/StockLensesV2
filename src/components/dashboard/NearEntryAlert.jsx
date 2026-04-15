import React from "react";
import { AlertCircle } from "lucide-react";

export default function NearEntryAlert({ items }) {
  if (!items || !items.length) return null;

  const nearEntry = items.filter(
    (i) => i.gap_pct != null && i.gap_pct >= -10 && i.gap_pct < 0 && i.has_projection
  );

  if (nearEntry.length === 0) return null;

  return (
    <div className="flex items-center gap-3 rounded-lg border border-amber-200 dark:border-amber-800 bg-amber-50 dark:bg-amber-950/40 px-4 py-2.5 text-sm">
      <AlertCircle className="w-4 h-4 text-amber-600 dark:text-amber-400 flex-shrink-0" />
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
        <span className="font-medium text-amber-800 dark:text-amber-200">Near Entry:</span>
        {nearEntry.map((item) => (
          <span key={item.symbol} className="font-mono text-amber-700 dark:text-amber-300">
            {item.symbol}{" "}
            <span className="text-amber-600 dark:text-amber-400">{item.gap_pct.toFixed(1)}%</span>
          </span>
        ))}
      </div>
    </div>
  );
}
