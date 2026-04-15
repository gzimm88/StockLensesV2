import React, { useState, useEffect, useCallback } from "react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
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
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
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
import { Textarea } from "@/components/ui/textarea";
import {
  Eye,
  Camera,
  Lock,
  Pencil,
  Trash2,
  ShoppingCart,
  CheckCircle2,
  RotateCcw,
  RefreshCw,
  TrendingDown,
  TrendingUp,
  ExternalLink,
} from "lucide-react";
import { Watchlist, ProjectionSnapshot } from "@/api/entities";
import { useToast } from "@/components/ui/use-toast";

function fmtMoney(v) {
  if (v == null || !Number.isFinite(Number(v))) return "—";
  return `$${Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function fmtPct(v, decimals = 1) {
  if (v == null || !Number.isFinite(Number(v))) return "—";
  return `${v > 0 ? "+" : ""}${Number(v).toFixed(decimals)}%`;
}

function fmtDate(iso) {
  if (!iso) return "—";
  return new Date(iso).toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
}

function StatusBadge({ status }) {
  switch (status) {
    case "bought":
      return <Badge className="bg-emerald-100 text-emerald-800 hover:bg-emerald-100 dark:bg-emerald-950 dark:text-emerald-200">Bought</Badge>;
    case "closed":
      return <Badge className="bg-slate-100 text-slate-700 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-300">Closed</Badge>;
    default:
      return <Badge className="bg-blue-100 text-blue-800 hover:bg-blue-100 dark:bg-blue-950 dark:text-blue-200">Watching</Badge>;
  }
}

function SourceBadge({ source, snapshot }) {
  if (snapshot) {
    return (
      <Badge variant="outline" className="text-[10px] gap-1 border-indigo-300 text-indigo-700 dark:text-indigo-300">
        <Camera className="w-2.5 h-2.5" /> Snapshot
      </Badge>
    );
  }
  if (source === "projection") return <Badge variant="outline" className="text-[10px]">Projection</Badge>;
  if (source === "manual") return <Badge variant="outline" className="text-[10px]">Manual</Badge>;
  return <span className="text-[10px] text-slate-400">—</span>;
}

export default function WatchlistPage() {
  const { toast } = useToast();
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState("all"); // all | watching | bought | closed
  const [editOpen, setEditOpen] = useState(false);
  const [editItem, setEditItem] = useState(null);
  const [editPrice, setEditPrice] = useState("");
  const [editBuyTrigger, setEditBuyTrigger] = useState("");
  const [editSellTrigger, setEditSellTrigger] = useState("");
  const [actionOpen, setActionOpen] = useState(null); // { item, action: "buy" | "close" }
  const [actionPrice, setActionPrice] = useState("");
  const [actionNotes, setActionNotes] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await Watchlist.summary();
      setItems(res?.items || []);
    } catch (e) {
      toast({ title: "Couldn't load watchlist", description: String(e?.message || e), variant: "destructive" });
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    load();
  }, [load]);

  const handleDelete = async (symbol) => {
    try {
      await Watchlist.remove(symbol);
      toast({ title: `${symbol} removed from watchlist` });
      load();
    } catch (e) {
      toast({ title: "Delete failed", description: String(e?.message || e), variant: "destructive" });
    }
  };

  const openEdit = (item) => {
    setEditItem(item);
    setEditPrice(item.entry_price != null ? String(item.entry_price) : "");
    setEditBuyTrigger(item.buy_trigger_price != null ? String(item.buy_trigger_price) : "");
    setEditSellTrigger(item.sell_trigger_price != null ? String(item.sell_trigger_price) : "");
    setEditOpen(true);
  };

  const handleSaveEdit = async () => {
    if (!editItem) return;
    setBusy(true);
    try {
      const parsedEntry = editPrice.trim() === "" ? null : Number(editPrice);
      const parsedBuy = editBuyTrigger.trim() === "" ? null : Number(editBuyTrigger);
      const parsedSell = editSellTrigger.trim() === "" ? null : Number(editSellTrigger);
      if (parsedEntry != null && !Number.isFinite(parsedEntry)) throw new Error("Invalid entry price");
      if (parsedBuy != null && !Number.isFinite(parsedBuy)) throw new Error("Invalid BUY trigger");
      if (parsedSell != null && !Number.isFinite(parsedSell)) throw new Error("Invalid SELL trigger");

      const buyFromSnapshot = editItem.buy_trigger_source === "snapshot";
      const sellFromSnapshot = editItem.sell_trigger_source === "snapshot";
      const hasActiveSnapshot = !!editItem.snapshot?.id && editItem.snapshot.status === "active";

      // 1) Update the watchlist entry: entry price + custom triggers (only if not driven by snapshot)
      const wlPatch = {};
      if (parsedEntry == null) wlPatch.clear_custom = true;
      else wlPatch.custom_entry_price = parsedEntry;
      if (!buyFromSnapshot) {
        if (parsedBuy == null) wlPatch.clear_buy_trigger = true;
        else wlPatch.custom_buy_trigger_price = parsedBuy;
      }
      if (!sellFromSnapshot) {
        if (parsedSell == null) wlPatch.clear_sell_trigger = true;
        else wlPatch.custom_sell_trigger_price = parsedSell;
      }
      await Watchlist.update(editItem.symbol, wlPatch);

      // 2) Update the snapshot triggers if either side is sourced from snapshot
      if (hasActiveSnapshot && (buyFromSnapshot || sellFromSnapshot)) {
        const snapPatch = {};
        if (buyFromSnapshot) {
          if (parsedBuy == null) snapPatch.clear_buy_trigger = true;
          else if (parsedBuy !== editItem.snapshot.buy_trigger_price) snapPatch.buy_trigger_price = parsedBuy;
        }
        if (sellFromSnapshot) {
          if (parsedSell == null) snapPatch.clear_sell_trigger = true;
          else if (parsedSell !== editItem.snapshot.sell_trigger_price) snapPatch.sell_trigger_price = parsedSell;
        }
        if (Object.keys(snapPatch).length > 0) {
          await ProjectionSnapshot.update(editItem.snapshot.id, snapPatch);
        }
      }

      toast({ title: `${editItem.symbol} updated` });
      setEditOpen(false);
      load();
    } catch (e) {
      toast({ title: "Save failed", description: String(e?.message || e), variant: "destructive" });
    } finally {
      setBusy(false);
    }
  };

  const openAction = (item, action) => {
    setActionOpen({ item, action });
    setActionPrice(item.current_price != null ? String(item.current_price) : "");
    setActionNotes("");
  };

  const handleAction = async () => {
    if (!actionOpen) return;
    setBusy(true);
    try {
      const parsed = actionPrice.trim() === "" ? null : Number(actionPrice);
      await Watchlist.action(actionOpen.item.symbol, {
        action: actionOpen.action,
        price: parsed,
        notes: actionNotes.trim() || null,
      });
      const verb = actionOpen.action === "buy" ? "marked as bought" : "closed";
      toast({ title: `${actionOpen.item.symbol} ${verb}` });
      setActionOpen(null);
      load();
    } catch (e) {
      toast({ title: "Action failed", description: String(e?.message || e), variant: "destructive" });
    } finally {
      setBusy(false);
    }
  };

  const handleReopen = async (symbol) => {
    try {
      await Watchlist.action(symbol, { action: "reopen" });
      toast({ title: `${symbol} reopened` });
      load();
    } catch (e) {
      toast({ title: "Reopen failed", description: String(e?.message || e), variant: "destructive" });
    }
  };

  const filtered = items.filter((i) => {
    if (filter === "all") return true;
    return i.status === filter;
  });

  const counts = {
    all: items.length,
    watching: items.filter((i) => i.status === "watching" || !i.status).length,
    bought: items.filter((i) => i.status === "bought").length,
    closed: items.filter((i) => i.status === "closed").length,
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 dark:text-slate-100 flex items-center gap-2">
            <Eye className="w-6 h-6" /> Watchlist
          </h1>
          <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">
            Manage everything on your radar. Mark items bought or closed to track decisions.
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={load} disabled={loading}>
          <RefreshCw className={`w-4 h-4 mr-2 ${loading ? "animate-spin" : ""}`} /> Refresh
        </Button>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <Tabs value={filter} onValueChange={setFilter}>
            <TabsList>
              <TabsTrigger value="all">
                All {counts.all > 0 && <Badge variant="secondary" className="ml-2">{counts.all}</Badge>}
              </TabsTrigger>
              <TabsTrigger value="watching">
                Watching {counts.watching > 0 && <Badge className="ml-2 bg-blue-100 text-blue-800 hover:bg-blue-100">{counts.watching}</Badge>}
              </TabsTrigger>
              <TabsTrigger value="bought">
                Bought {counts.bought > 0 && <Badge className="ml-2 bg-emerald-100 text-emerald-800 hover:bg-emerald-100">{counts.bought}</Badge>}
              </TabsTrigger>
              <TabsTrigger value="closed">
                Closed {counts.closed > 0 && <Badge variant="secondary" className="ml-2">{counts.closed}</Badge>}
              </TabsTrigger>
            </TabsList>
          </Tabs>
        </CardHeader>
        <CardContent>
          {loading && items.length === 0 ? (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          ) : filtered.length === 0 ? (
            <div className="py-12 text-center text-slate-500 dark:text-slate-400">
              <Eye className="w-10 h-10 mx-auto mb-3 text-slate-300 dark:text-slate-700" />
              <p className="text-sm">
                {filter === "all"
                  ? "Your watchlist is empty. Add tickers via Screener or save a Projection snapshot."
                  : "No items match this filter."}
              </p>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Ticker</TableHead>
                  <TableHead>Source</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Current</TableHead>
                  <TableHead className="text-right">Entry</TableHead>
                  <TableHead className="text-right">Gap</TableHead>
                  <TableHead className="text-right">BUY @</TableHead>
                  <TableHead className="text-right">Gap BUY</TableHead>
                  <TableHead className="text-right">SELL @</TableHead>
                  <TableHead className="text-right">Gap SELL</TableHead>
                  <TableHead>Acted</TableHead>
                  <TableHead className="text-right w-40">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filtered.map((item) => {
                  const isWatching = (item.status || "watching") === "watching";
                  const isBought = item.status === "bought";
                  const isClosed = item.status === "closed";
                  const snap = item.snapshot;
                  const gapColor =
                    item.gap_pct == null
                      ? "text-slate-400"
                      : item.gap_pct < 0
                      ? "text-emerald-700 dark:text-emerald-400"
                      : "text-red-700 dark:text-red-400";
                  return (
                    <TableRow key={item.symbol} className={isClosed ? "opacity-70" : ""}>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <span className="font-mono font-semibold">{item.symbol}</span>
                          {snap && (
                            <Link to="/snapshots" title="View snapshot">
                              <Camera className="w-3 h-3 text-indigo-500" />
                            </Link>
                          )}
                        </div>
                        <div className="text-xs text-slate-500 truncate max-w-[160px]" title={item.name}>
                          {item.name}
                        </div>
                      </TableCell>
                      <TableCell>
                        <SourceBadge source={item.source} snapshot={snap} />
                      </TableCell>
                      <TableCell>
                        <StatusBadge status={item.status || "watching"} />
                      </TableCell>
                      <TableCell className="text-right font-mono text-xs">{fmtMoney(item.current_price)}</TableCell>
                      <TableCell className="text-right font-mono text-xs">
                        {item.entry_price != null ? (
                          <span className="inline-flex items-center gap-1 justify-end">
                            {item.is_frozen && <Lock className="w-2.5 h-2.5 text-amber-500" />}
                            {fmtMoney(item.entry_price)}
                          </span>
                        ) : (
                          <span className="text-slate-400 italic">No target</span>
                        )}
                      </TableCell>
                      <TableCell className={`text-right font-mono text-xs ${gapColor}`}>
                        <span className="inline-flex items-center gap-0.5 justify-end">
                          {item.gap_pct != null && (item.gap_pct < 0 ? <TrendingDown className="w-3 h-3" /> : <TrendingUp className="w-3 h-3" />)}
                          {item.gap_pct == null ? "—" : fmtPct(item.gap_pct)}
                        </span>
                      </TableCell>
                      <TableCell className="text-right font-mono text-xs">
                        {item.buy_trigger_price != null ? (
                          <span className="text-emerald-700 dark:text-emerald-400">
                            {fmtMoney(item.buy_trigger_price)}
                            {item.buy_trigger_source === "snapshot" && (
                              <Camera className="inline w-2.5 h-2.5 ml-1 text-indigo-500" />
                            )}
                          </span>
                        ) : (
                          <span className="text-slate-400">—</span>
                        )}
                      </TableCell>
                      <TableCell
                        className={`text-right font-mono text-xs ${
                          item.gap_to_buy_pct == null
                            ? "text-slate-400"
                            : item.gap_to_buy_pct >= 0
                            ? "text-slate-600"
                            : "text-emerald-700 dark:text-emerald-400"
                        }`}
                      >
                        {item.gap_to_buy_pct == null ? "—" : fmtPct(item.gap_to_buy_pct)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-xs">
                        {item.sell_trigger_price != null ? (
                          <span className="text-red-700 dark:text-red-400">
                            {fmtMoney(item.sell_trigger_price)}
                            {item.sell_trigger_source === "snapshot" && (
                              <Camera className="inline w-2.5 h-2.5 ml-1 text-indigo-500" />
                            )}
                          </span>
                        ) : (
                          <span className="text-slate-400">—</span>
                        )}
                      </TableCell>
                      <TableCell
                        className={`text-right font-mono text-xs ${
                          item.gap_to_sell_pct == null
                            ? "text-slate-400"
                            : item.gap_to_sell_pct <= 0
                            ? "text-red-700 dark:text-red-400"
                            : "text-slate-600"
                        }`}
                      >
                        {item.gap_to_sell_pct == null ? "—" : fmtPct(item.gap_to_sell_pct)}
                      </TableCell>
                      <TableCell className="text-xs text-slate-500">
                        {item.acted_at ? (
                          <span title={`@ ${fmtMoney(item.acted_price)}`}>
                            {fmtDate(item.acted_at)}
                            {item.acted_price != null && (
                              <span className="text-slate-400"> · {fmtMoney(item.acted_price)}</span>
                            )}
                          </span>
                        ) : (
                          "—"
                        )}
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="flex items-center justify-end gap-1">
                          {isWatching && (
                            <>
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-7 px-2 text-xs text-emerald-700 dark:text-emerald-400 border-emerald-200 dark:border-emerald-800"
                                onClick={() => openAction(item, "buy")}
                              >
                                <ShoppingCart className="w-3 h-3 mr-1" /> Buy
                              </Button>
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-7 px-2 text-xs text-slate-600 dark:text-slate-300"
                                onClick={() => openAction(item, "close")}
                              >
                                <CheckCircle2 className="w-3 h-3 mr-1" /> Close
                              </Button>
                            </>
                          )}
                          {isBought && (
                            <Button
                              variant="outline"
                              size="sm"
                              className="h-7 px-2 text-xs"
                              onClick={() => openAction(item, "close")}
                            >
                              <CheckCircle2 className="w-3 h-3 mr-1" /> Sell / Close
                            </Button>
                          )}
                          {isClosed && (
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-7 w-7"
                              title="Reopen"
                              onClick={() => handleReopen(item.symbol)}
                            >
                              <RotateCcw className="w-3.5 h-3.5" />
                            </Button>
                          )}
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7"
                            title="Edit entry price"
                            onClick={() => openEdit(item)}
                          >
                            <Pencil className="w-3.5 h-3.5" />
                          </Button>
                          <AlertDialog>
                            <AlertDialogTrigger asChild>
                              <Button variant="ghost" size="icon" className="h-7 w-7 text-red-500" title="Remove from watchlist">
                                <Trash2 className="w-3.5 h-3.5" />
                              </Button>
                            </AlertDialogTrigger>
                            <AlertDialogContent>
                              <AlertDialogHeader>
                                <AlertDialogTitle>Remove {item.symbol} from watchlist?</AlertDialogTitle>
                                <AlertDialogDescription>
                                  This removes the ticker from your watchlist. Any projection or snapshot data stays intact.
                                  {snap && (
                                    <span className="block mt-2 text-amber-700 dark:text-amber-400">
                                      Note: {item.symbol} has an active snapshot. Removing the watchlist entry will not delete the snapshot.
                                    </span>
                                  )}
                                </AlertDialogDescription>
                              </AlertDialogHeader>
                              <AlertDialogFooter>
                                <AlertDialogCancel>Cancel</AlertDialogCancel>
                                <AlertDialogAction onClick={() => handleDelete(item.symbol)}>Remove</AlertDialogAction>
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

      {/* Edit entry price + BUY/SELL triggers dialog */}
      <Dialog open={editOpen} onOpenChange={setEditOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Edit — {editItem?.symbol}</DialogTitle>
            <DialogDescription>
              Adjust the entry-price target and BUY / SELL triggers. Leave any field empty to clear it.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4">
            <div className="grid gap-1.5">
              <label className="text-sm font-medium">Entry price ($)</label>
              <Input
                type="number"
                step="0.01"
                value={editPrice}
                onChange={(e) => setEditPrice(e.target.value)}
                placeholder="e.g. 235.74"
              />
              <p className="text-xs text-slate-500">
                Custom entry override. Empty falls back to
                {editItem?.snapshot
                  ? " snapshot's frozen price."
                  : editItem?.source === "projection"
                  ? " projection-derived price."
                  : " default price."}
              </p>
            </div>

            <div className="rounded-lg border border-slate-200 dark:border-slate-700 p-3 bg-slate-50 dark:bg-slate-900 space-y-3">
              <div className="flex items-center justify-between text-xs font-medium text-slate-700 dark:text-slate-300">
                <span className="flex items-center gap-2">
                  {editItem?.snapshot ? (
                    <>
                      <Camera className="w-3.5 h-3.5 text-indigo-500" />
                      Triggers — fires once when crossed
                    </>
                  ) : (
                    <>Triggers — fires once when crossed</>
                  )}
                </span>
                {editItem?.snapshot && (
                  <span className="text-[10px] text-indigo-600 dark:text-indigo-400 font-normal">
                    Snapshot
                  </span>
                )}
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div className="grid gap-1.5">
                  <label className="text-xs font-medium text-emerald-700 dark:text-emerald-400">
                    BUY trigger ($)
                  </label>
                  <Input
                    type="number"
                    step="0.01"
                    value={editBuyTrigger}
                    onChange={(e) => setEditBuyTrigger(e.target.value)}
                    placeholder="empty = no trigger"
                  />
                  <p className="text-[11px] text-slate-500">Fires when price ≤ this</p>
                </div>
                <div className="grid gap-1.5">
                  <label className="text-xs font-medium text-red-700 dark:text-red-400">
                    SELL trigger ($)
                  </label>
                  <Input
                    type="number"
                    step="0.01"
                    value={editSellTrigger}
                    onChange={(e) => setEditSellTrigger(e.target.value)}
                    placeholder="empty = no trigger"
                  />
                  <p className="text-[11px] text-slate-500">Fires when price ≥ this</p>
                </div>
              </div>
              {editItem?.snapshot && editItem.snapshot.status !== "active" && (
                <p className="text-xs text-amber-700 dark:text-amber-400">
                  Snapshot is <strong>{editItem.snapshot.status}</strong> — trigger changes won't re-arm it.
                  Save a new snapshot from the Projection page to re-arm.
                </p>
              )}
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditOpen(false)}>Cancel</Button>
            <Button onClick={handleSaveEdit} disabled={busy}>Save</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Action dialog (Buy / Close) */}
      <Dialog open={!!actionOpen} onOpenChange={(o) => !o && setActionOpen(null)}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>
              {actionOpen?.action === "buy" ? "Mark as bought" : "Close position"} — {actionOpen?.item.symbol}
            </DialogTitle>
            <DialogDescription>
              {actionOpen?.action === "buy"
                ? "Record that you bought this ticker. This is a tracking marker — it does not create a portfolio transaction."
                : "Record that you sold or closed this ticker. The entry stays for reference."}
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-3">
            <div className="grid gap-1.5">
              <label className="text-sm font-medium">Price</label>
              <Input
                type="number"
                step="0.01"
                value={actionPrice}
                onChange={(e) => setActionPrice(e.target.value)}
                placeholder={actionOpen?.item.current_price ? String(actionOpen.item.current_price) : "Current price"}
              />
              <p className="text-xs text-slate-500">Defaults to latest known price if empty.</p>
            </div>
            <div className="grid gap-1.5">
              <label className="text-sm font-medium">Notes (optional)</label>
              <Textarea rows={2} value={actionNotes} onChange={(e) => setActionNotes(e.target.value)} />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setActionOpen(null)}>Cancel</Button>
            <Button onClick={handleAction} disabled={busy}>
              {actionOpen?.action === "buy" ? "Mark bought" : "Mark closed"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
