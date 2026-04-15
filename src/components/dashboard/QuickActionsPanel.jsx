import React from "react";
import { useNavigate } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Plus, RefreshCw, Search, Briefcase, Target, Layers3, Zap, Mail, Check, Pencil } from "lucide-react";
import { toast } from "sonner";
import { UserEmail } from "@/api/entities";

export default function QuickActionsPanel() {
  const navigate = useNavigate();
  const [refreshing, setRefreshing] = React.useState(false);
  const [email, setEmail] = React.useState(null);
  const [editingEmail, setEditingEmail] = React.useState(false);
  const [emailDraft, setEmailDraft] = React.useState("");
  const [savingEmail, setSavingEmail] = React.useState(false);

  React.useEffect(() => {
    UserEmail.get()
      .then((res) => {
        const e = res?.data?.email || null;
        setEmail(e);
        setEmailDraft(e || "");
        if (!e) setEditingEmail(true);
      })
      .catch(() => {});
  }, []);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      const res = await fetch("/api/market-data/refresh", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
      });
      if (!res.ok) throw new Error("Refresh failed");
      toast.success("Market data refresh triggered");
    } catch {
      toast.error("Failed to refresh market data");
    } finally {
      setRefreshing(false);
    }
  };

  const handleSaveEmail = async () => {
    setSavingEmail(true);
    try {
      const res = await UserEmail.update(emailDraft.trim() || null);
      setEmail(res?.data?.email || null);
      setEditingEmail(false);
      toast.success("Notification email saved");
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setSavingEmail(false);
    }
  };

  const actions = [
    { label: "Add Ticker", icon: Plus, onClick: () => navigate("/screener") },
    { label: "Refresh Prices", icon: RefreshCw, onClick: handleRefresh, loading: refreshing },
    { label: "Screener", icon: Search, onClick: () => navigate("/screener") },
    { label: "Portfolio", icon: Briefcase, onClick: () => navigate("/portfolio") },
    { label: "Projections", icon: Target, onClick: () => navigate("/projection") },
    { label: "Lenses", icon: Layers3, onClick: () => navigate("/lenses") },
  ];

  return (
    <Card className="h-full">
      <CardHeader className="pb-3">
        <CardTitle className="text-base flex items-center gap-2">
          <Zap className="w-4 h-4" /> Quick Actions
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="grid grid-cols-2 gap-2">
          {actions.map((action) => {
            const Icon = action.icon;
            return (
              <Button
                key={action.label}
                variant="outline"
                size="sm"
                className="flex items-center gap-2 justify-start h-10 text-xs"
                onClick={action.onClick}
                disabled={action.loading}
              >
                <Icon className={`w-3.5 h-3.5 ${action.loading ? "animate-spin" : ""}`} />
                {action.label}
              </Button>
            );
          })}
        </div>

        {/* Notification email */}
        <div className="pt-2 border-t border-slate-100 dark:border-slate-800">
          <div className="flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400 mb-1.5">
            <Mail className="w-3 h-3" />
            <span>Alert email</span>
          </div>
          {editingEmail ? (
            <div className="flex gap-1.5">
              <Input
                type="email"
                placeholder="you@example.com"
                value={emailDraft}
                onChange={(e) => setEmailDraft(e.target.value)}
                className="h-7 text-xs"
              />
              <Button
                size="sm"
                className="h-7 px-2"
                onClick={handleSaveEmail}
                disabled={savingEmail}
              >
                <Check className="w-3 h-3" />
              </Button>
            </div>
          ) : (
            <div className="flex items-center justify-between gap-2">
              <span className="text-xs font-mono text-slate-700 dark:text-slate-300 truncate" title={email || ""}>
                {email || "—"}
              </span>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6"
                onClick={() => setEditingEmail(true)}
                title="Edit email"
              >
                <Pencil className="w-3 h-3" />
              </Button>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
