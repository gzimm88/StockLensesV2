import React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Clock, Globe, TrendingUp, TrendingDown } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import WorldMapSvg from "./WorldMapSvg";

function formatCountdown(totalSeconds) {
  if (totalSeconds <= 0) return "00:00:00";
  const h = Math.floor(totalSeconds / 3600);
  const m = Math.floor((totalSeconds % 3600) / 60);
  const s = totalSeconds % 60;
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function SessionCard({ session }) {
  const [secondsLeft, setSecondsLeft] = React.useState(session.seconds_until);

  React.useEffect(() => {
    setSecondsLeft(session.seconds_until);
  }, [session.seconds_until]);

  React.useEffect(() => {
    const timer = setInterval(() => {
      setSecondsLeft((prev) => (prev > 0 ? prev - 1 : 0));
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  const isOpen = session.status === "open";

  return (
    <div className="flex flex-col gap-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 p-4">
      <div className="flex items-center justify-between">
        <h3 className="font-semibold text-sm text-slate-900 dark:text-slate-100">{session.region}</h3>
        <Badge variant={isOpen ? "default" : "secondary"} className={isOpen ? "bg-emerald-500 hover:bg-emerald-600 text-white" : ""}>
          {isOpen ? "OPEN" : "CLOSED"}
        </Badge>
      </div>
      <p className="text-xs text-slate-500 dark:text-slate-400">{session.exchanges.join(", ")}</p>
      <div className="flex items-center gap-2 mt-1">
        <Clock className="w-3.5 h-3.5 text-slate-400" />
        <span className="font-mono text-lg font-bold text-slate-800 dark:text-slate-200">
          {formatCountdown(secondsLeft)}
        </span>
        <span className="text-xs text-slate-500 dark:text-slate-400">
          {isOpen ? "until close" : "until open"}
        </span>
      </div>
      {session.index && session.index.change_pct != null && (
        <div className="flex items-center justify-between pt-1 mt-1 border-t border-slate-100 dark:border-slate-800">
          <span className="text-xs text-slate-500 dark:text-slate-400">{session.index.label}</span>
          <span
            className={`inline-flex items-center gap-0.5 text-xs font-semibold ${
              session.index.change_pct >= 0
                ? "text-emerald-600 dark:text-emerald-400"
                : "text-red-600 dark:text-red-400"
            }`}
            title={`${session.index.symbol} as of ${session.index.as_of} — $${session.index.price?.toFixed?.(2) ?? "—"}`}
          >
            {session.index.change_pct >= 0 ? (
              <TrendingUp className="w-3 h-3" />
            ) : (
              <TrendingDown className="w-3 h-3" />
            )}
            {session.index.change_pct >= 0 ? "+" : ""}
            {session.index.change_pct.toFixed(2)}%
          </span>
        </div>
      )}
    </div>
  );
}

export default function MarketSessionClock({ data, loading }) {
  const sessions = data?.sessions;

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base flex items-center gap-2">
          <Globe className="w-4 h-4" /> Global Markets
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* Map always renders independently */}
        <WorldMapSvg />

        {/* Session cards: show skeletons while loading, real data when available */}
        {loading || !sessions ? (
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-24 rounded-xl" />
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            {sessions.map((session) => (
              <SessionCard key={session.region} session={session} />
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
