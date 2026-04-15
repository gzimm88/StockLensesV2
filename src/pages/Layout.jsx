
import React from "react";
import { Link, useLocation } from "react-router-dom";
import { createPageUrl } from "@/utils";
import { useAuth } from "@/auth/AuthContext";
import {
  TrendingUp,
  Search,
  Target,
  Layers3,
  Briefcase,
  Moon,
  Sun,
  LayoutDashboard,
  Camera,
  Eye
} from "lucide-react";
import { useTheme } from "next-themes";
import { Button } from "@/components/ui/button";
import NotificationsBell from "@/components/layout/NotificationsBell";

export default function Layout({ children, currentPageName }) {
  const location = useLocation();
  const { theme, setTheme } = useTheme();
  const [mounted, setMounted] = React.useState(false);
  React.useEffect(() => setMounted(true), []);
  
  const navigation = [
    { name: "Dashboard", href: "/", icon: LayoutDashboard },
    { name: "Screener", href: createPageUrl("Screener"), icon: Search },
    { name: "Lenses", href: createPageUrl("Lenses"), icon: Layers3 },
    { name: "Projection", href: createPageUrl("Projection"), icon: Target },
    { name: "Watchlist", href: "/watchlist", icon: Eye },
    { name: "Snapshots", href: "/snapshots", icon: Camera },
    { name: "Portfolio", href: createPageUrl("Portfolio"), icon: Briefcase },
  ];

  const isActive = (href) => location.pathname === href;

  return (
    <div className="min-h-screen bg-background text-foreground transition-colors">
      {/* Header */}
      <header className="bg-white dark:bg-slate-950/95 shadow-sm border-b border-slate-200 dark:border-slate-800">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center space-x-8">
              <Link to="/" className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-gradient-to-br from-slate-800 to-slate-900 rounded-xl flex items-center justify-center">
                  <TrendingUp className="w-6 h-6 text-amber-400" />
                </div>
                <div>
                  <h1 className="text-xl font-bold text-slate-900 dark:text-slate-100">AlphaStock</h1>
                  <p className="text-xs text-slate-500 dark:text-slate-400">Deterministic Analysis</p>
                </div>
              </Link>
              
              <nav className="hidden md:flex space-x-8">
                {navigation.map((item) => {
                  const Icon = item.icon;
                  return (
                    <Link
                      key={item.name}
                      to={item.href}
                      className={`flex items-center space-x-2 px-3 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${
                        isActive(item.href)
                          ? "bg-slate-100 dark:bg-slate-800 text-slate-900 dark:text-slate-100"
                          : "text-slate-600 dark:text-slate-300 hover:text-slate-900 dark:hover:text-slate-100 hover:bg-slate-50 dark:hover:bg-slate-900"
                      }`}
                    >
                      <Icon className="w-4 h-4" />
                      <span>{item.name}</span>
                    </Link>
                  );
                })}
              </nav>
            </div>
            
            <div className="flex items-center space-x-4">
              {accountActions.map((item) => {
                const Icon = item.icon;
                return (
                  <Link
                    key={item.name}
                    to={item.href}
                    className={`hidden md:flex items-center space-x-2 px-3 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${
                      isActive(item.href)
                        ? "bg-slate-100 dark:bg-slate-800 text-slate-900 dark:text-slate-100"
                        : "text-slate-600 dark:text-slate-300 hover:text-slate-900 dark:hover:text-slate-100 hover:bg-slate-50 dark:hover:bg-slate-900"
                    }`}
                  >
                    <Icon className="w-4 h-4" />
                    <span>{item.name}</span>
                  </Link>
                );
              })}
              <NotificationsBell />
              <Button
                variant="outline"
                size="icon"
                aria-label="Toggle night mode"
                onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
                className="border-slate-300 dark:border-slate-700"
              >
                {mounted && theme === "dark" ? (
                  <Sun className="w-4 h-4" />
                ) : (
                  <Moon className="w-4 h-4" />
                )}
              </Button>
              <div className="text-right">
                <p className="text-sm font-medium text-slate-900 dark:text-slate-100">Professional</p>
                <p className="text-xs text-slate-500 dark:text-slate-400">Analytics Suite</p>
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Mobile Navigation */}
      <nav className="md:hidden bg-white dark:bg-slate-950/95 border-b border-slate-200 dark:border-slate-800">
        <div className="flex space-x-1 px-4 py-3">
          {navigation.map((item) => {
            const Icon = item.icon;
            return (
              <Link
                key={item.name}
                to={item.href}
                className={`flex items-center space-x-2 px-3 py-2 rounded-lg text-sm font-medium flex-1 justify-center transition-all duration-200 ${
                  isActive(item.href)
                    ? "bg-slate-100 dark:bg-slate-800 text-slate-900 dark:text-slate-100"
                    : "text-slate-600 dark:text-slate-300 hover:text-slate-900 dark:hover:text-slate-100 hover:bg-slate-50 dark:hover:bg-slate-900"
                }`}
              >
                <Icon className="w-4 h-4" />
                <span>{item.name}</span>
              </Link>
            );
          })}
        </div>
      </nav>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {children}
      </main>
    </div>
  );
}
