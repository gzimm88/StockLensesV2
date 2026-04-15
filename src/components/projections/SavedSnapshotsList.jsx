import React, { useState, useEffect, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
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
import { Archive, History, Trash2, RefreshCw, Eye } from "lucide-react";
import { ProjectionSnapshot } from "@/api/entities";
import { useToast } from "@/components/ui/use-toast";
import SnapshotDetailDialog from "./SnapshotDetailDialog";

function fmtMoney(v) {
  if (v == null || !Number.isFinite(v)) return "—";
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

export default function SavedSnapshotsList({ tickerSymbol, currentPrice, refreshKey }) {
  const { toast } = useToast();
  const [snapshots, setSnapshots] = useState([]);
  const [loading, setLoading] = useState(false);
  const [detailOpen, setDetailOpen] = useState(false);
  const [detailSnapshot, setDetailSnapshot] = useState(null);

  const load = useCallback(async () => {
    if (!tickerSymbol) {
      setSnapshots([]);
      return;
    }
    setLoading(true);
    try {
      const res = await ProjectionSnapshot.list({ ticker_symbol: tickerSymbol });
      setSnapshots(res?.data?.snapshots || []);
    } catch (e) {
      toast({ title: "Couldn't load snapshots", description: String(e?.message || e), variant: "destructive" });
    } finally {
      setLoading(false);
    }
  }, [tickerSymbol, toast]);

  useEffect(() => {
    load();
  }, [load, refreshKey]);

  const handleArchive = async (snap) => {
    try {
      await ProjectionSnapshot.update(snap.id, { status: "archived" });
      toast({ title: "Snapshot archived" });
      load();
    } catch (e) {
      toast({ title: "Archive failed", description: String(e?.message || e), variant: "destructive" });
    }
  };

  const handleDelete = async (snap) => {
    try {
      await ProjectionSnapshot.delete(snap.id);
      toast({ title: "Snapshot deleted" });
      load();
    } catch (e) {
      toast({ title: "Delete failed", description: String(e?.message || e), variant: "destructive" });
    }
  };

  if (!tickerSymbol) return null;

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center justify-between text-base">
          <span className="flex items-center gap-2">
            <History className="w-4 h-4" /> Saved Snapshots — {tickerSymbol}
          </span>
          <Button variant="ghost" size="sm" onClick={load} disabled={loading}>
            <RefreshCw className={`w-3 h-3 ${loading ? "animate-spin" : ""}`} />
          </Button>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {loading && snapshots.length === 0 ? (
          <div className="space-y-2">
            {[1, 2].map((i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        ) : snapshots.length === 0 ? (
          <p className="text-sm text-slate-500 dark:text-slate-400 py-4 text-center">
            No snapshots saved for {tickerSymbol}. Click "Save Snapshot" to freeze this projection and arm price triggers.
          </p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Saved</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">BUY @</TableHead>
                <TableHead className="text-right">SELL @</TableHead>
                <TableHead className="text-right">Current</TableHead>
                <TableHead className="text-right">Gap to BUY</TableHead>
                <TableHead className="text-right w-24">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {snapshots.map((s) => {
                const buy = s.triggers?.buy_trigger_price;
                const sell = s.triggers?.sell_trigger_price;
                const gap =
                  currentPrice && buy ? ((buy - currentPrice) / currentPrice) * 100 : null;
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
                    <TableCell className="max-w-[200px] truncate" title={s.name}>
                      <button
                        type="button"
                        onClick={(e) => { e.stopPropagation(); openDetail(); }}
                        className="hover:underline text-left"
                      >
                        {s.name}
                      </button>
                    </TableCell>
                    <TableCell className="text-xs text-slate-500">{fmtDate(s.created_at)}</TableCell>
                    <TableCell>{statusBadge(s.status)}</TableCell>
                    <TableCell className="text-right font-mono text-xs text-emerald-700 dark:text-emerald-400">
                      {fmtMoney(buy)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-red-700 dark:text-red-400">
                      {fmtMoney(sell)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs">
                      {currentPrice ? fmtMoney(currentPrice) : "—"}
                    </TableCell>
                    <TableCell
                      className={`text-right font-mono text-xs ${
                        gap == null ? "text-slate-400" : gap > 0 ? "text-slate-600" : "text-emerald-700 dark:text-emerald-400"
                      }`}
                    >
                      {gap == null ? "—" : `${gap > 0 ? "+" : ""}${gap.toFixed(1)}%`}
                    </TableCell>
                    <TableCell className="text-right" onClick={(e) => e.stopPropagation()}>
                      <div className="flex justify-end gap-1">
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7"
                          title="View details"
                          onClick={(e) => { e.stopPropagation(); openDetail(); }}
                        >
                          <Eye className="w-3.5 h-3.5" />
                        </Button>
                        {isActive && (
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7"
                            title="Archive"
                            onClick={() => handleArchive(s)}
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
                              <AlertDialogAction onClick={() => handleDelete(s)}>Delete</AlertDialogAction>
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
      <SnapshotDetailDialog
        open={detailOpen}
        onOpenChange={setDetailOpen}
        snapshot={detailSnapshot}
        currentPrice={currentPrice}
      />
    </Card>
  );
}
