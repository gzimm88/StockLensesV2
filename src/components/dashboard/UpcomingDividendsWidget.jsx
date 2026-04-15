import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { CalendarDays } from "lucide-react";

function fmtDate(iso) {
  if (!iso) return "--";
  const d = new Date(iso + "T00:00:00");
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

export default function UpcomingDividendsWidget({ data, loading }) {
  if (loading) {
    return (
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm flex items-center gap-2">
            <CalendarDays className="w-4 h-4" /> Upcoming Dividends
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-2">
          {[1, 2, 3].map((i) => (
            <Skeleton key={i} className="h-5 w-full" />
          ))}
        </CardContent>
      </Card>
    );
  }

  const items = data || [];

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center gap-2">
          <CalendarDays className="w-4 h-4" /> Upcoming Dividends
        </CardTitle>
      </CardHeader>
      <CardContent>
        {items.length === 0 ? (
          <p className="text-xs text-slate-500 dark:text-slate-400">
            No upcoming dividends in the next 30 days.
          </p>
        ) : (
          <div className="space-y-1.5">
            {items.map((d, idx) => (
              <div key={idx} className="flex items-center justify-between text-xs">
                <span className="font-semibold text-slate-700 dark:text-slate-300">{d.ticker}</span>
                <span className="text-slate-500 dark:text-slate-400">{fmtDate(d.ex_date)}</span>
                <span className="font-mono text-slate-600 dark:text-slate-400">
                  {d.amount != null ? `$${d.amount.toFixed(2)}` : "--"}/sh
                </span>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
