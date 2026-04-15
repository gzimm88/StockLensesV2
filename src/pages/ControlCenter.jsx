import React from "react";
import {
  getMarketSessions,
  getWatchlistSummary,
  getDashboardOverview,
  getMarketIndices,
  ensureIndices,
} from "@/api/dashboard";
import MarketSessionClock from "@/components/dashboard/MarketSessionClock";
import QuickActionsPanel from "@/components/dashboard/QuickActionsPanel";
import NearEntryAlert from "@/components/dashboard/NearEntryAlert";
import TriggerAlertsWidget from "@/components/dashboard/TriggerAlertsWidget";
import MarketIndicesBar from "@/components/dashboard/MarketIndicesBar";
import PortfolioSummaryWidget from "@/components/dashboard/PortfolioSummaryWidget";
import WatchlistWidget from "@/components/dashboard/WatchlistWidget";
import TopMoversWidget from "@/components/dashboard/TopMoversWidget";
import BloombergTvWidget from "@/components/dashboard/BloombergTvWidget";
import CurrencyExposureWidget from "@/components/dashboard/CurrencyExposureWidget";
import UpcomingDividendsWidget from "@/components/dashboard/UpcomingDividendsWidget";
import RecentActivityFeed from "@/components/dashboard/RecentActivityFeed";

export default function ControlCenter() {
  const [sessions, setSessions] = React.useState(null);
  const [watchlist, setWatchlist] = React.useState(null);
  const [overview, setOverview] = React.useState(null);
  const [indices, setIndices] = React.useState(null);
  const [loading, setLoading] = React.useState(true);

  const fetchAll = React.useCallback(async () => {
    const results = await Promise.allSettled([
      getMarketSessions(),
      getWatchlistSummary(),
      getDashboardOverview(),
      getMarketIndices(),
    ]);
    if (results[0].status === "fulfilled") setSessions(results[0].value);
    if (results[1].status === "fulfilled") setWatchlist(results[1].value);
    if (results[2].status === "fulfilled") setOverview(results[2].value);
    if (results[3].status === "fulfilled") setIndices(results[3].value);
    setLoading(false);
  }, []);

  const indicesTriggered = React.useRef(false);

  React.useEffect(() => {
    // Fire-and-forget: auto-onboard index ETFs on first load
    if (!indicesTriggered.current) {
      indicesTriggered.current = true;
      ensureIndices().catch(() => {});
    }
    fetchAll();
    const interval = setInterval(fetchAll, 60_000);
    return () => clearInterval(interval);
  }, [fetchAll]);

  return (
    <div className="space-y-4">
      {/* Row 1: Market Clock + Quick Actions */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          <MarketSessionClock data={sessions} loading={loading} />
        </div>
        <QuickActionsPanel />
      </div>

      {/* Triggered alerts (from projection snapshots) */}
      <TriggerAlertsWidget />

      {/* Near-entry alerts (self-hiding) */}
      <NearEntryAlert items={watchlist?.items} />

      {/* Market Indices Bar */}
      <MarketIndicesBar data={indices} loading={loading} />

      {/* Row 2: Portfolio + Top Movers + Watchlist */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        <PortfolioSummaryWidget data={overview?.portfolio} loading={loading} />
        <TopMoversWidget data={overview?.top_movers} loading={loading} />
        <WatchlistWidget data={watchlist} loading={loading} />
      </div>

      {/* Row 3: Bloomberg + Currency Exposure + Dividends */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          <BloombergTvWidget />
        </div>
        <div className="space-y-4">
          <CurrencyExposureWidget
            data={overview?.portfolio?.currency_exposure}
            loading={loading}
          />
          <UpcomingDividendsWidget
            data={overview?.upcoming_dividends}
            loading={loading}
          />
        </div>
      </div>

      {/* Row 4: Recent Activity */}
      <RecentActivityFeed data={overview?.recent_activity} loading={loading} />
    </div>
  );
}
