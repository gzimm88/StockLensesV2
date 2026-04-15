import React from "react";
import { useNavigate } from "react-router-dom";
import { TrendingUp } from "lucide-react";

import { useAuth } from "@/auth/AuthContext";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { createPageUrl } from "@/utils";

export default function Login() {
  const navigate = useNavigate();
  const { login, user } = useAuth();
  const [username, setUsername] = React.useState("Admin");
  const [password, setPassword] = React.useState("Admin1234");
  const [submitting, setSubmitting] = React.useState(false);
  const [error, setError] = React.useState("");

  React.useEffect(() => {
    if (user) navigate("/", { replace: true });
  }, [user, navigate]);

  const onSubmit = async (e) => {
    e.preventDefault();
    setSubmitting(true);
    setError("");
    try {
      await login(username, password);
      navigate("/", { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top_left,_rgba(251,191,36,0.15),_transparent_30%),linear-gradient(180deg,_#f8fafc_0%,_#eef2ff_100%)] px-4 py-12 text-slate-950">
      <div className="mx-auto flex max-w-5xl flex-col gap-10 lg:flex-row lg:items-center">
        <div className="max-w-xl space-y-5">
          <div className="inline-flex items-center gap-3 rounded-2xl bg-slate-950 px-4 py-3 text-white shadow-lg">
            <TrendingUp className="h-6 w-6 text-amber-400" />
            <div>
              <p className="text-lg font-semibold">AlphaStock</p>
              <p className="text-xs text-slate-300">Account Workspace</p>
            </div>
          </div>
          <div className="space-y-3">
            <h1 className="text-4xl font-semibold tracking-tight">Sign in to the platform workspace.</h1>
            <p className="text-base text-slate-600">
              This release introduces real accounts and ownership boundaries for portfolios. Use the
              master account first, then create trial accounts from the admin panel.
            </p>
          </div>
        </div>

        <Card className="w-full max-w-md border-slate-200 bg-white/95 shadow-2xl backdrop-blur">
          <CardHeader>
            <CardTitle>Login</CardTitle>
          </CardHeader>
          <CardContent>
            <form className="space-y-4" onSubmit={onSubmit}>
              <div className="space-y-2">
                <Label htmlFor="username">Username</Label>
                <Input id="username" value={username} onChange={(e) => setUsername(e.target.value)} />
              </div>
              <div className="space-y-2">
                <Label htmlFor="password">Password</Label>
                <Input
                  id="password"
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                />
              </div>
              {error && <p className="text-sm text-red-600">{error}</p>}
              <Button className="w-full" disabled={submitting} type="submit">
                {submitting ? "Signing in..." : "Sign In"}
              </Button>
            </form>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
