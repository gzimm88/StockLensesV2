import React from "react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Briefcase, TrendingUp, TrendingDown, ArrowRight } from "lucide-react";
import {
  PieChart, Pie, Cell, ResponsiveContainer, Tooltip,
  AreaChart, Area, LineChart, Line, ReferenceLine,
} from "recharts";

const COLORS = [
  "hsl(var(--chart-1))",
  "hsl(var(--chart-2))",
  "hsl(var(--chart-3))",
  "hsl(var(--chart-4))",
  "hsl(var(--chart-5))",
];

function fmt(val, decimals = 2) {
  if (val == null) return "--";
  return Number(val).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export default function PortfolioSummaryWidget({ data, loading }) {
  if (loading) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Briefcase className="w-4 h-4" /> Portfolio
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <Skeleton className="h-8 w-32" />
          <Skeleton className="h-24 w-24 rounded-full mx-auto" />
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-4 w-3/4" />
        </CardContent>
      </Card>
    );
  }

  if (!data) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Briefcase className="w-4 h-4" /> Portfolio
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-slate-500 dark:text-slate-400">
            No portfolio found.{" "}
            <Link to="/portfolios" className="text-blue-600 dark:text-blue-400 underline">
              Create one
            </Link>
          </p>
        </CardContent>
      </Card>
    );
  }

  if (data.needs_processing) {
    return (
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base flex items-center justify-between">
            <span className="flex items-center gap-2">
              <Briefcase className="w-4 h-4" /> {data.portfolio_name || "Portfolio"}
            </span>
            <Link to={`/portfolio?id=${data.portfolio_id}`} className="text-xs text-blue-600 dark:text-blue-400 flex items-center gap-1 hover:underline">
              View <ArrowRight className="w-3 h-3" />
            </Link>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-slate-500 dark:text-slate-400">
            Portfolio needs processing.{" "}
            <Link to={`/portfolio?id=${data.portfolio_id}`} className="text-blue-600 dark:text-blue-400 underline">
              Open portfolio
            </Link>{" "}
            to build equity history.
          </p>
        </CardContent>
      </Card>
    );
  }

  const dayChange = data.day_change ?? 0;
  const dayChangePct = data.day_change_pct ?? 0;
  const isPositive = dayChange >= 0;

  const pieData = (data.top_holdings || []).map((h) => ({
    name: h.ticker,
    value: h.weight,
  }));

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base flex items-center justify-between">
          <span className="flex items-center gap-2">
            <Briefcase className="w-4 h-4" /> {data.portfolio_name || "Portfolio"}
          </span>
          <Link to={`/portfolio?id=${data.portfolio_id}`} className="text-xs text-blue-600 dark:text-blue-400 flex items-center gap-1 hover:underline">
            View <ArrowRight className="w-3 h-3" />
          </Link>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div>
          <p className="text-2xl font-bold text-slate-900 dark:text-slate-100">
            ${fmt(data.nav)}
          </p>
          <div className={`flex items-center gap-1 text-sm font-medium ${isPositive ? "text-emerald-600 dark:text-emerald-400" : "text-red-600 dark:text-red-400"}`}>
            {isPositive ? <TrendingUp className="w-3.5 h-3.5" /> : <TrendingDown className="w-3.5 h-3.5" />}
            {isPositive ? "+" : ""}{fmt(dayChange)} ({isPositive ? "+" : ""}{fmt(dayChangePct)}%)
          </div>
          {data.cash_balance > 0 && (
            <p className="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
              Cash: ${fmt(data.cash_balance)}
            </p>
          )}
        </div>

        {/* Benchmark comparison chart (portfolio vs S&P 500) */}
        {data.benchmark_series && data.benchmark_series.length > 2 && data.benchmark_series.some(d => d.spy_pct != null) ? (
          <div className="h-20 -mx-2">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.benchmark_series} margin={{ top: 4, right: 4, bottom: 0, left: 4 }}>
                <ReferenceLine y={0} stroke="#94a3b8" strokeWidth={0.5} />
                <Line
                  type="monotone"
                  dataKey="portfolio_pct"
                  stroke="#10b981"
                  strokeWidth={1.5}
                  dot={false}
                  isAnimationActive={false}
                  name="Portfolio"
                />
                <Line
                  type="monotone"
                  dataKey="spy_pct"
                  stroke="#3b82f6"
                  strokeWidth={1}
                  strokeDasharray="4 3"
                  dot={false}
                  isAnimationActive={false}
                  name="S&P 500"
                  connectNulls
                />
                <Tooltip
                  formatter={(v, name) => [`${v > 0 ? "+" : ""}${v}%`, name]}
                  contentStyle={{ fontSize: 11, padding: "4px 8px" }}
                  labelStyle={{ display: "none" }}
                />
              </LineChart>
            </ResponsiveContainer>
            <div className="flex items-center justify-center gap-3 text-[10px] text-slate-500 dark:text-slate-400 -mt-1">
              <span className="flex items-center gap-1"><span className="w-3 h-0.5 bg-emerald-500 inline-block" /> Portfolio</span>
              <span className="flex items-center gap-1"><span className="w-3 h-0.5 bg-blue-500 inline-block border-dashed" style={{ borderTop: "1px dashed #3b82f6", height: 0 }} /> S&P 500</span>
            </div>
          </div>
        ) : data.equity_sparkline && data.equity_sparkline.length > 2 ? (
          <div className="h-14 -mx-2">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={data.equity_sparkline} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
                <defs>
                  <linearGradient id="sparklineFill" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={isPositive ? "#10b981" : "#ef4444"} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={isPositive ? "#10b981" : "#ef4444"} stopOpacity={0.02} />
                  </linearGradient>
                </defs>
                <Area
                  type="monotone"
                  dataKey="value"
                  stroke={isPositive ? "#10b981" : "#ef4444"}
                  strokeWidth={1.5}
                  fill="url(#sparklineFill)"
                  dot={false}
                  isAnimationActive={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        ) : null}

        {pieData.length > 0 && (
          <div className="flex items-center gap-4">
            <div className="w-24 h-24 flex-shrink-0">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={pieData}
                    cx="50%"
                    cy="50%"
                    innerRadius={20}
                    outerRadius={40}
                    dataKey="value"
                    stroke="none"
                  >
                    {pieData.map((_, idx) => (
                      <Cell key={idx} fill={COLORS[idx % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip
                    formatter={(value) => `${fmt(value)}%`}
                    contentStyle={{ fontSize: 12 }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="flex-1 space-y-1">
              {pieData.slice(0, 5).map((item, idx) => (
                <div key={item.name} className="flex items-center justify-between text-xs">
                  <span className="flex items-center gap-1.5">
                    <span
                      className="w-2 h-2 rounded-full inline-block"
                      style={{ backgroundColor: COLORS[idx % COLORS.length] }}
                    />
                    <span className="font-medium text-slate-700 dark:text-slate-300">{item.name}</span>
                  </span>
                  <span className="text-slate-500 dark:text-slate-400">{fmt(item.value, 1)}%</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
