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
  TabsContent,
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
  Bell,
  BellOff,
  CheckCheck,
  ArrowDown,
  ArrowUp,
  Mail,
  MailX,
  Trash2,
  Target,
} from "lucide-react";
import { AlertNotification } from "@/api/entities";
import { useToast } from "@/components/ui/use-toast";

function fmtMoney(v) {
  if (v == null || !Number.isFinite(Number(v))) return "—";
  return `$${Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function fmtDateTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleString(undefined, { year: "numeric", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

export default function Notifications() {
  const { toast } = useToast();
  const [alerts, setAlerts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState("all"); // "all" | "unread" | "buy" | "sell"

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await AlertNotification.list({ limit: 200 });
      setAlerts(res?.data?.alerts || []);
    } catch (e) {
      toast({ title: "Couldn't load notifications", description: String(e?.message || e), variant: "destructive" });
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    load();
  }, [load]);

  const handleMarkRead = async (id) => {
    try {
      await AlertNotification.markRead(id);
      setAlerts((prev) => prev.map((a) => (a.id === id ? { ...a, read: true, read_at: new Date().toISOString() } : a)));
    } catch {}
  };

  const handleMarkAllRead = async () => {
    try {
      await AlertNotification.markAllRead();
      setAlerts((prev) => prev.map((a) => ({ ...a, read: true })));
      toast({ title: "All marked as read" });
    } catch (e) {
      toast({ title: "Couldn't mark all as read", description: String(e?.message || e), variant: "destructive" });
    }
  };

  const handleDismiss = async (id) => {
    try {
      await AlertNotification.dismiss(id);
      setAlerts((prev) => prev.map((a) => (a.id === id ? { ...a, dismissed: true, read: true } : a)));
    } catch {}
  };

  const handleDelete = async (id) => {
    try {
      await AlertNotification.delete(id);
      setAlerts((prev) => prev.filter((a) => a.id !== id));
    } catch (e) {
      toast({ title: "Delete failed", description: String(e?.message || e), variant: "destructive" });
    }
  };

  const filtered = alerts.filter((a) => {
    if (filter === "unread") return !a.read;
    if (filter === "buy") return a.alert_type === "buy";
    if (filter === "sell") return a.alert_type === "sell";
    return true;
  });

  const unreadCount = alerts.filter((a) => !a.read).length;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 dark:text-slate-100 flex items-center gap-2">
            <Bell className="w-6 h-6" /> Notification Center
          </h1>
          <p className="text-sm text-slate-500 dark:text-slate-400 mt-1">
            Complete history of your price-trigger alerts from saved projection snapshots.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {unreadCount > 0 && (
            <Button variant="outline" size="sm" onClick={handleMarkAllRead}>
              <CheckCheck className="w-4 h-4 mr-2" /> Mark all read
            </Button>
          )}
        </div>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <Tabs value={filter} onValueChange={setFilter}>
            <TabsList>
              <TabsTrigger value="all">
                All
                {alerts.length > 0 && (
                  <Badge variant="secondary" className="ml-2">
                    {alerts.length}
                  </Badge>
                )}
              </TabsTrigger>
              <TabsTrigger value="unread">
                Unread
                {unreadCount > 0 && <Badge className="ml-2 bg-indigo-100 text-indigo-800 hover:bg-indigo-100">{unreadCount}</Badge>}
              </TabsTrigger>
              <TabsTrigger value="buy">
                <ArrowDown className="w-3 h-3 mr-1" /> Buy
              </TabsTrigger>
              <TabsTrigger value="sell">
                <ArrowUp className="w-3 h-3 mr-1" /> Sell
              </TabsTrigger>
            </TabsList>
          </Tabs>
        </CardHeader>
        <CardContent>
          {loading && alerts.length === 0 ? (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          ) : filtered.length === 0 ? (
            <div className="py-12 text-center text-slate-500 dark:text-slate-400">
              <BellOff className="w-10 h-10 mx-auto mb-3 text-slate-300 dark:text-slate-700" />
              <p className="text-sm">
                {filter === "all"
                  ? "No notifications yet. Save a projection snapshot with triggers to get alerted when prices cross."
                  : "No notifications match this filter."}
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
                  <TableHead className="w-6"></TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Ticker</TableHead>
                  <TableHead>Snapshot</TableHead>
                  <TableHead className="text-right">Triggered @</TableHead>
                  <TableHead className="text-right">Threshold</TableHead>
                  <TableHead>When</TableHead>
                  <TableHead>Email</TableHead>
                  <TableHead className="w-20"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filtered.map((a) => {
                  const isBuy = a.alert_type === "buy";
                  const Icon = isBuy ? ArrowDown : ArrowUp;
                  return (
                    <TableRow key={a.id} className={!a.read ? "font-medium bg-indigo-50/40 dark:bg-indigo-950/20" : ""}>
                      <TableCell>
                        {!a.read && <span className="inline-block w-2 h-2 rounded-full bg-indigo-500" />}
                      </TableCell>
                      <TableCell>
                        <Badge
                          variant="outline"
                          className={
                            isBuy
                              ? "border-emerald-300 text-emerald-700 dark:text-emerald-300"
                              : "border-red-300 text-red-700 dark:text-red-300"
                          }
                        >
                          <Icon className="w-3 h-3 mr-1" />
                          {isBuy ? "BUY" : "SELL"}
                        </Badge>
                      </TableCell>
                      <TableCell className="font-mono font-semibold">{a.ticker_symbol}</TableCell>
                      <TableCell className="max-w-[240px] truncate text-xs text-slate-600 dark:text-slate-400" title={a.snapshot_name}>
                        {a.snapshot_name || a.snapshot_id.slice(0, 8)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-xs">{fmtMoney(a.triggered_price)}</TableCell>
                      <TableCell className="text-right font-mono text-xs text-slate-500">{fmtMoney(a.threshold_price)}</TableCell>
                      <TableCell className="text-xs text-slate-500">{fmtDateTime(a.triggered_at)}</TableCell>
                      <TableCell>
                        {a.email_sent ? (
                          <span title="Email sent" className="inline-flex items-center gap-1 text-xs text-emerald-600 dark:text-emerald-400">
                            <Mail className="w-3 h-3" /> sent
                          </span>
                        ) : a.email_error ? (
                          <span title={a.email_error} className="inline-flex items-center gap-1 text-xs text-slate-500">
                            <MailX className="w-3 h-3" /> skipped
                          </span>
                        ) : (
                          <span className="text-xs text-slate-400">—</span>
                        )}
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          {!a.read && (
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-7 w-7"
                              title="Mark as read"
                              onClick={() => handleMarkRead(a.id)}
                            >
                              <CheckCheck className="w-3.5 h-3.5" />
                            </Button>
                          )}
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7 text-red-500"
                            title="Delete"
                            onClick={() => handleDelete(a.id)}
                          >
                            <Trash2 className="w-3.5 h-3.5" />
                          </Button>
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
    </div>
  );
}
