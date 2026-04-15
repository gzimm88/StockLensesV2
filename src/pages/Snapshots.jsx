import React, { useState, useEffect, useCallback } from "react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Tabs,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import {
  Camera,
  Archive,
  Trash2,
  RefreshCw,
  ArrowDown,
  ArrowUp,
  Target,
  Eye,
} from "lucide-react";
import { ProjectionSnapshot, Metrics } from "@/api/entities";
import { useToast } from "@/components/ui/use-toast";
import SnapshotDetailDialog from "@/components/projections/SnapshotDetailDialog";

function fmtMoney(v) {
  if (v == null || !Number.isFinite(Number(v))) return "—";
  return `$${Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function fmtDate(iso) {
  if (!iso) return "—";
  return new Date(iso).toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
}

function statusBadge(status) {
  switch (status) {
    case "active":
      return <Badge className="bg-blue-100 text-blue-800 hover:bg-blue-100 dark:bg-blue-950 dark:text-blue-200">Active</Badge>;
    case "buy_triggered":
      return <Badge className="bg-emerald-100 text-emerald-800 hover:bg-emerald-100 dark:bg-emerald-950 dark:text-emerald-200">Buy triggered</Badge>;
    case "sell_triggered":
      return <Badge className="bg-red-100 text-red-800 hover:bg-red-100 dark:bg-red-950 dark:text-red-200">Sell triggered</Badge>;
    case "archived":
      return <Badge variant="outline">Archived</Badge>;
    default:
      return <Badge variant="outline">{status}</Badge>;
  }
}

export default function Snapshots() {
  const { toast } = useToast();
  const [snapshots, setSnapshots] = useState([]);
  const [prices, setPrices] = useState({}); // ticker -> price
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState("all"); // all | active | triggered | archived
  const [detailOpen, setDetailOpen] = useState(false);
  const [detailSnapshot, setDetailSnapshot] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await ProjectionSnapshot.list({});
      const snaps = res?.data?.snapshots || [];
      setSnapshots(snaps);

      // Batch-load current prices for all tickers
      const tickers = [...new Set(snaps.map((s) => s.ticker_symbol))];
      const priceMap = {};
      await Promise.all(
        tickers.map(async (t) => {
          try {
            const m = await Metrics.filter({ ticker_symbol: t });
            // Accept either array of rows or single row; pick the most recent price_current
            const rows = Array.isArray(m?.data?.metrics) ? m.data.metrics : Array.isArray(m) ? m : [];
            const withPrice = rows.filter((r) => r?.price_current);
            if (withPrice.length) {
              const latest = withPrice.sort(
                (a, b) => new Date(b.as_of_date || 0) - new Date(a.as_of_date || 0)
              )[0];
              priceMap[t] = Number(latest.price_current);
            }
          } catch {
            // fall back to snapshot.current_price
          }
        })
      );
      setPrices(priceMap);
    } catch (e) {
      toast({ title: "Couldn't load snapshots", description: String(e?.message || e), variant: "destructive" });
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    load();
  }, [load]);

  const handleArchive = async (id) => {
    try {
      await ProjectionSnapshot.update(id, { status: "archived" });
      toast({ title: "Snapshot archived" });
      load();
    } catch (e) {
      toast({ title: "Archive failed", description: String(e?.message || e), variant: "destructive" });
    }
  };

  const handleDelete = async (id) => {
    try {
      await ProjectionSnapshot.delete(id);
      toast({ title: "Snapshot deleted" });
      load();
    } catch (e) {
      toast({ title: "Delete failed", description: String(e?.message || e), variant: "destructive" });
    }
  };

  const filtered = snapshots.filter((s) => {
    if (filter === "all") return true;
    if (filter === "active") return s.status === "active";
    if (filter === "triggered") return s.status === "buy_triggered" || s.status === "sell_triggered";
    if (filter === "archived") return s.status === "archived";
    return true;
  });

  const counts = {
    all: snapshots.length,
    active: snapshots.filter((s) => s.status === "active").length,
    triggered: snapshots.filter((s) => s.status === "buy_triggered" || s.status === "sell_triggered").length,
    archived: snapshots.filter((s) => s.status === "archived").length,
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 dark:text-slate-100 flex items-center gap-2">
            <Camera className="w-6 h-6" /> Projection Snapshots
          </h1>
          <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">
            All saved projections with armed BUY/SELL price triggers.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={load} disabled={loading}>
            <RefreshCw className={`w-4 h-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
          </Button>
          <Button asChild size="sm">
            <Link to="/Projection">
              <Target className="w-4 h-4 mr-2" /> New snapshot
            </Link>
          </Button>
        </div>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <Tabs value={filter} onValueChange={setFilter}>
            <TabsList>
              <TabsTrigger value="all">
                All {counts.all > 0 && <Badge variant="secondary" className="ml-2">{counts.all}</Badge>}
              </TabsTrigger>
              <TabsTrigger value="active">
                Active
                {counts.active > 0 && <Badge className="ml-2 bg-blue-100 text-blue-800 hover:bg-blue-100">{counts.active}</Badge>}
              </TabsTrigger>
              <TabsTrigger value="triggered">
                Triggered
                {counts.triggered > 0 && <Badge className="ml-2 bg-emerald-100 text-emerald-800 hover:bg-emerald-100">{counts.triggered}</Badge>}
              </TabsTrigger>
              <TabsTrigger value="archived">
                Archived {counts.archived > 0 && <Badge variant="secondary" className="ml-2">{counts.archived}</Badge>}
              </TabsTrigger>
            </TabsList>
          </Tabs>
        </CardHeader>
        <CardContent>
          {loading && snapshots.length === 0 ? (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          ) : filtered.length === 0 ? (
            <div className="py-12 text-center text-slate-500 dark:text-slate-400">
              <Camera className="w-10 h-10 mx-auto mb-3 text-slate-300 dark:text-slate-700" />
              <p className="text-sm">
                {filter === "all"
                  ? "No snapshots yet. Save one from the Projection page to arm BUY/SELL triggers."
                  : "No snapshots in this filter."}
              </p>
              {filter === "all" && (
                <Button asChild variant="outline" size="sm" className="mt-4">
                  <Link to="/Projection">
                    <Target className="w-4 h-4 mr-2" /> Go to Projection
                  </Link>
                </Button>
              )}
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Ticker</TableHead>
                  <TableHead>Snapshot</TableHead>
                  <TableHead>Saved</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">
                    <ArrowDown className="w-3 h-3 inline mr-1" />BUY
                  </TableHead>
                  <TableHead className="text-right">
                    <ArrowUp className="w-3 h-3 inline mr-1" />SELL
                  </TableHead>
                  <TableHead className="text-right">Current</TableHead>
                  <TableHead className="text-right">Gap BUY</TableHead>
                  <TableHead className="text-right">Gap SELL</TableHead>
                  <TableHead className="text-right w-24">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filtered.map((s) => {
                  const buy = s.triggers?.buy_trigger_price;
                  const sell = s.triggers?.sell_trigger_price;
                  const current = prices[s.ticker_symbol] || s.inputs?.current_price || null;
                  const gapBuy = current && buy ? ((buy - current) / current) * 100 : null;
                  const gapSell = current && sell ? ((sell - current) / current) * 100 : null;
                  const isActive = s.status === "active";
                  const openDetail = () => {
                    setDetailSnapshot(s);
                    setDetailOpen(true);
                  };
                  return (
                    <TableRow
                      key={s.id}
                      className="cursor-pointer hover:bg-slate-50 dark:hover:bg-slate-900"
                      onClick={openDetail}
                    >
                      <TableCell className="font-mono font-semibold">
                        <button
                          type="button"
                          onClick={(e) => {
                            e.stopPropagation();
                            openDetail();
                          }}
                          className="hover:underline inline-flex items-center gap-1"
                        >
                          {s.ticker_symbol}
                        </button>
                      </TableCell>
                      <TableCell className="max-w-[240px] truncate text-xs text-slate-600 dark:text-slate-400" title={s.name}>
                        {s.name}
                      </TableCell>
                      <TableCell className="text-xs text-slate-500">{fmtDate(s.created_at)}</TableCell>
                      <TableCell>{statusBadge(s.status)}</TableCell>
                      <TableCell className="text-right font-mono text-xs text-emerald-700 dark:text-emerald-400">
                        {fmtMoney(buy)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-xs text-red-700 dark:text-red-400">
                        {fmtMoney(sell)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-xs">{fmtMoney(current)}</TableCell>
                      <TableCell
                        className={`text-right font-mono text-xs ${
                          gapBuy == null
                            ? "text-slate-400"
                            : gapBuy <= 0
                            ? "text-emerald-700 dark:text-emerald-400"
                            : "text-slate-600"
                        }`}
                      >
                        {gapBuy == null ? "—" : `${gapBuy > 0 ? "+" : ""}${gapBuy.toFixed(1)}%`}
                      </TableCell>
                      <TableCell
                        className={`text-right font-mono text-xs ${
                          gapSell == null
                            ? "text-slate-400"
                            : gapSell >= 0
                            ? "text-red-700 dark:text-red-400"
                            : "text-slate-600"
                        }`}
                      >
                        {gapSell == null ? "—" : `${gapSell > 0 ? "+" : ""}${gapSell.toFixed(1)}%`}
                      </TableCell>
                      <TableCell className="text-right" onClick={(e) => e.stopPropagation()}>
                        <div className="flex justify-end gap-1">
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7"
                            title="View details"
                            onClick={(e) => {
                              e.stopPropagation();
                              openDetail();
                            }}
                          >
                            <Eye className="w-3.5 h-3.5" />
                          </Button>
                          {isActive && (
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-7 w-7"
                              title="Archive"
                              onClick={() => handleArchive(s.id)}
                            >
                              <Archive className="w-3.5 h-3.5" />
                            </Button>
                          )}
                          <AlertDialog>
                            <AlertDialogTrigger asChild>
                              <Button variant="ghost" size="icon" className="h-7 w-7 text-red-500" title="Delete">
                                <Trash2 className="w-3.5 h-3.5" />
                              </Button>
                            </AlertDialogTrigger>
                            <AlertDialogContent>
                              <AlertDialogHeader>
                                <AlertDialogTitle>Delete this snapshot?</AlertDialogTitle>
                                <AlertDialogDescription>
                                  This will remove "{s.name}" and all its alerts. This cannot be undone.
                                </AlertDialogDescription>
                              </AlertDialogHeader>
                              <AlertDialogFooter>
                                <AlertDialogCancel>Cancel</AlertDialogCancel>
                                <AlertDialogAction onClick={() => handleDelete(s.id)}>Delete</AlertDialogAction>
                              </AlertDialogFooter>
                            </AlertDialogContent>
                          </AlertDialog>
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <SnapshotDetailDialog
        open={detailOpen}
        onOpenChange={setDetailOpen}
        snapshot={detailSnapshot}
        currentPrice={detailSnapshot ? prices[detailSnapshot.ticker_symbol] : null}
      />
    </div>
  );
}
