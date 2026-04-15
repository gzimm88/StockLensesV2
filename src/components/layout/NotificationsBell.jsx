import React, { useEffect, useState, useCallback } from "react";
import { Link } from "react-router-dom";
import { Bell } from "lucide-react";
import { AlertNotification } from "@/api/entities";

export default function NotificationsBell() {
  const [unreadCount, setUnreadCount] = useState(0);

  const load = useCallback(async () => {
    try {
      const res = await AlertNotification.list({ limit: 1 });
      setUnreadCount(Number(res?.data?.unread_count) || 0);
    } catch {
      // silent — bell just won't show a badge
    }
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, 60_000);
    return () => clearInterval(id);
  }, [load]);

  return (
    <Link
      to="/notifications"
      className="relative inline-flex items-center justify-center w-9 h-9 rounded-lg border border-slate-300 dark:border-slate-700 text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-900 transition-colors"
      aria-label={`Notifications${unreadCount > 0 ? ` (${unreadCount} unread)` : ""}`}
      title="Notifications"
    >
      <Bell className="w-4 h-4" />
      {unreadCount > 0 && (
        <span className="absolute -top-1 -right-1 min-w-[18px] h-[18px] px-1 rounded-full bg-indigo-600 text-white text-[10px] font-semibold flex items-center justify-center">
          {unreadCount > 99 ? "99+" : unreadCount}
        </span>
      )}
    </Link>
  );
}
