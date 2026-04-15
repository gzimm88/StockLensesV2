import React, { useState, useEffect, useMemo } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Lock, ArrowDown, ArrowUp, Bell, AlertTriangle } from "lucide-react";
import { ProjectionSnapshot, UserEmail } from "@/api/entities";
import { useToast } from "@/components/ui/use-toast";

const SCENARIO_KEYS = ["bear", "trend", "bull", "constant", "custom"];

function serializeYearlyData(yd) {
  if (!yd) return null;
  const out = {
    years: yd.years,
    epsPath: yd.epsPath,
    pePaths: {},
    pricePaths: {},
  };
  for (const key of ["current", "bear", "bull", "constant", "custom"]) {
    try {
      out.pePaths[key] = yd.pePaths?.[key] || null;
      out.pricePaths[key] = yd.pricePaths ? yd.pricePaths(key) : null;
    } catch {
      // skip
    }
  }
  return out;
}

function parseNum(val) {
  if (val === "" || val == null) return null;
  const n = Number(val);
  return Number.isFinite(n) ? n : null;
}

function formatMoney(val) {
  if (val == null || !Number.isFinite(val)) return "—";
  return `$${Number(val).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

export default function SaveSnapshotDialog({
  open,
  onOpenChange,
  tickerSymbol,
  inputs,
  manualPE,
  scenario,
  yearlyData,
  results,
  onSaved,
}) {
  const { toast } = useToast();
  const today = useMemo(() => new Date().toISOString().slice(0, 10), []);
  const [name, setName] = useState("");
  const [notes, setNotes] = useState("");
  const [overvaluedPct, setOvervaluedPct] = useState(15);
  const [buyPrice, setBuyPrice] = useState("");
  const [sellPrice, setSellPrice] = useState("");
  const [sellAuto, setSellAuto] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);
  const [userEmail, setUserEmail] = useState(null);
  const [emailInput, setEmailInput] = useState("");
  const [savingEmail, setSavingEmail] = useState(false);

  // Initialize / reset when dialog opens
  useEffect(() => {
    if (!open) return;
    setName(`${tickerSymbol || "Snapshot"} - ${today}`);
    setNotes("");
    setOvervaluedPct(15);
    setBuyPrice(
      results?.requiredEntry != null && Number.isFinite(results.requiredEntry)
        ? Number(results.requiredEntry).toFixed(2)
        : ""
    );
    setSellPrice(
      results?.terminalPrice != null && Number.isFinite(results.terminalPrice)
        ? (Number(results.terminalPrice) * 1.15).toFixed(2)
        : ""
    );
    setSellAuto(true);
    setError(null);
    // Load user email for the reminder
    UserEmail.get()
      .then((res) => {
        const e = res?.data?.email || null;
        setUserEmail(e);
        setEmailInput(e || "");
      })
      .catch(() => {});
  }, [open, tickerSymbol, today, results]);

  // Recompute sell when overvalued% changes AND sellAuto is still on
  useEffect(() => {
    if (!open || !sellAuto) return;
    if (results?.terminalPrice != null && Number.isFinite(results.terminalPrice)) {
      setSellPrice((Number(results.terminalPrice) * (1 + overvaluedPct / 100)).toFixed(2));
    }
  }, [overvaluedPct, sellAuto, open, results?.terminalPrice]);

  const handleSellChange = (v) => {
    setSellAuto(false);
    setSellPrice(v);
  };

  const handleSaveEmail = async () => {
    setSavingEmail(true);
    try {
      const res = await UserEmail.update(emailInput.trim() || null);
      setUserEmail(res?.data?.email || null);
      toast({ title: "Email saved", description: "You'll get notifications for triggered alerts." });
    } catch (e) {
      toast({ title: "Couldn't save email", description: String(e?.message || e), variant: "destructive" });
    } finally {
      setSavingEmail(false);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      const payload = {
        ticker_symbol: tickerSymbol,
        name: name.trim() || `${tickerSymbol} - ${today}`,
        notes: notes.trim() || null,
        current_price: parseNum(inputs?.priceToday),
        current_eps: parseNum(inputs?.EPS0),
        growth_rate: parseNum(inputs?.growthRate),
        years: parseNum(inputs?.years) != null ? Math.round(parseNum(inputs.years)) : null,
        target_cagr: parseNum(inputs?.targetCAGR),
        pe_bear: parseNum(inputs?.peBear),
        pe_mid: parseNum(inputs?.peMid),
        pe_bull: parseNum(inputs?.peBull),
        pe_custom_terminal: parseNum(inputs?.peCustomTerminal),
        current_pe: parseNum(manualPE),
        scenario: scenario || null,
        terminal_eps: parseNum(results?.terminalEPS),
        exit_pe: parseNum(results?.exitPE),
        terminal_price: parseNum(results?.terminalPrice),
        implied_cagr: parseNum(results?.impliedCAGR),
        required_entry: parseNum(results?.requiredEntry),
        margin_of_safety: parseNum(results?.marginOfSafety),
        yearly_data: serializeYearlyData(yearlyData),
        overvalued_pct: Number(overvaluedPct) || 15,
        buy_trigger_price: parseNum(buyPrice),
        sell_trigger_price: parseNum(sellPrice),
      };
      const res = await ProjectionSnapshot.create(payload);
      toast({
        title: "Snapshot saved",
        description: `Triggers armed for ${tickerSymbol}. BUY ${formatMoney(payload.buy_trigger_price)} · SELL ${formatMoney(payload.sell_trigger_price)}.`,
      });
      onOpenChange(false);
      if (onSaved) onSaved(res?.data?.snapshot);
    } catch (e) {
      setError(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  };

  const currentPrice = parseNum(inputs?.priceToday);
  const buyNum = parseNum(buyPrice);
  const sellNum = parseNum(sellPrice);
  const buyGap =
    currentPrice && buyNum ? ((buyNum - currentPrice) / currentPrice) * 100 : null;
  const sellGap =
    currentPrice && sellNum ? ((sellNum - currentPrice) / currentPrice) * 100 : null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Lock className="w-4 h-4" /> Save projection snapshot
          </DialogTitle>
          <DialogDescription>
            Freezes all inputs, computed outputs, and yearly data. Sets BUY and SELL price triggers that fire
            once when crossed.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="grid gap-2">
            <Label htmlFor="snap-name">Snapshot name</Label>
            <Input id="snap-name" value={name} onChange={(e) => setName(e.target.value)} maxLength={200} />
          </div>

          <div className="grid gap-2">
            <Label htmlFor="snap-notes">Notes (optional)</Label>
            <Textarea
              id="snap-notes"
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              rows={2}
              placeholder="Rationale, catalysts, or context for future reference…"
            />
          </div>

          <div className="rounded-lg border border-slate-200 dark:border-slate-700 p-3 bg-slate-50 dark:bg-slate-900">
            <div className="flex items-center gap-2 mb-3">
              <Bell className="w-4 h-4 text-slate-500" />
              <span className="text-sm font-medium">Price triggers (fire once, then armed status ends)</span>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="grid gap-1.5">
                <Label htmlFor="snap-buy" className="flex items-center gap-1 text-emerald-700 dark:text-emerald-400">
                  <ArrowDown className="w-3 h-3" /> BUY trigger
                </Label>
                <Input
                  id="snap-buy"
                  type="number"
                  step="0.01"
                  value={buyPrice}
                  onChange={(e) => setBuyPrice(e.target.value)}
                />
                <p className="text-xs text-slate-500">
                  Fires when price ≤ {formatMoney(buyNum)}
                  {buyGap != null && (
                    <>
                      {" "}
                      ({buyGap > 0 ? "+" : ""}
                      {buyGap.toFixed(1)}% from ${currentPrice?.toFixed(2)})
                    </>
                  )}
                </p>
              </div>
              <div className="grid gap-1.5">
                <Label htmlFor="snap-sell" className="flex items-center gap-1 text-red-700 dark:text-red-400">
                  <ArrowUp className="w-3 h-3" /> SELL trigger
                </Label>
                <Input
                  id="snap-sell"
                  type="number"
                  step="0.01"
                  value={sellPrice}
                  onChange={(e) => handleSellChange(e.target.value)}
                />
                <p className="text-xs text-slate-500">
                  Fires when price ≥ {formatMoney(sellNum)}
                  {sellGap != null && (
                    <>
                      {" "}
                      ({sellGap > 0 ? "+" : ""}
                      {sellGap.toFixed(1)}% from ${currentPrice?.toFixed(2)})
                    </>
                  )}
                </p>
              </div>
            </div>

            <div className="grid gap-1.5 mt-3">
              <Label htmlFor="snap-overpct" className="text-xs text-slate-600 dark:text-slate-400">
                Overvalued threshold (% above terminal price){" "}
                <span className="text-slate-400">
                  — default SELL = terminal × (1 + this%)
                </span>
              </Label>
              <Input
                id="snap-overpct"
                type="number"
                step="1"
                value={overvaluedPct}
                onChange={(e) => setOvervaluedPct(Number(e.target.value) || 0)}
                className="max-w-[140px]"
              />
            </div>
          </div>

          {!userEmail && (
            <div className="rounded-lg border border-amber-200 dark:border-amber-800 bg-amber-50 dark:bg-amber-950/30 p-3 text-sm">
              <div className="flex items-center gap-2 text-amber-800 dark:text-amber-200 mb-2">
                <AlertTriangle className="w-4 h-4" />
                <span className="font-medium">No email set</span>
              </div>
              <p className="text-xs text-amber-700 dark:text-amber-300 mb-2">
                Set an email to receive trigger notifications (in-app alerts will work regardless).
              </p>
              <div className="flex gap-2">
                <Input
                  type="email"
                  placeholder="you@example.com"
                  value={emailInput}
                  onChange={(e) => setEmailInput(e.target.value)}
                  className="h-8 text-sm"
                />
                <Button size="sm" onClick={handleSaveEmail} disabled={savingEmail || !emailInput.trim()}>
                  Save email
                </Button>
              </div>
            </div>
          )}

          {error && (
            <div className="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-950/30 p-3 text-sm text-red-700 dark:text-red-300">
              {error}
            </div>
          )}

          <div className="text-xs text-slate-500 dark:text-slate-400">
            <Badge variant="outline" className="mr-1">Note</Badge>
            Saving archives any previous active snapshot for {tickerSymbol}. A snapshot fires once — create a new one
            to re-arm after a trigger.
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={saving}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={saving || !tickerSymbol || !results}>
            {saving ? "Saving…" : "Save snapshot & arm triggers"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
