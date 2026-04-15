"""
Email service with graceful fallback.

Reads SMTP config from environment variables:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM

If any are missing, send_email() returns (False, "SMTP not configured") without raising.
Designed to be safe to call from scheduler jobs — never blocks, never crashes.
"""
from __future__ import annotations

import logging
import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Any, Tuple

logger = logging.getLogger(__name__)


def _smtp_config() -> dict | None:
    host = os.environ.get("SMTP_HOST")
    port = os.environ.get("SMTP_PORT")
    user = os.environ.get("SMTP_USER")
    pw = os.environ.get("SMTP_PASS")
    sender = os.environ.get("SMTP_FROM") or user
    if not (host and port and user and pw and sender):
        return None
    try:
        return {"host": host, "port": int(port), "user": user, "pass": pw, "from": sender}
    except ValueError:
        return None


def send_email(to_email: str, subject: str, html_body: str, text_body: str | None = None) -> Tuple[bool, str | None]:
    cfg = _smtp_config()
    if cfg is None:
        logger.info("SMTP not configured; skipping email to %s", to_email)
        return False, "SMTP not configured"
    if not to_email or "@" not in to_email:
        return False, "invalid recipient"
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg["from"]
    msg["To"] = to_email
    msg.set_content(text_body or "See HTML version of this message.")
    msg.add_alternative(html_body, subtype="html")
    try:
        ctx = ssl.create_default_context()
        # Prefer SSL (port 465) for most providers; fallback to STARTTLS (587) if explicit
        if cfg["port"] == 465:
            with smtplib.SMTP_SSL(cfg["host"], cfg["port"], context=ctx, timeout=15) as server:
                server.login(cfg["user"], cfg["pass"])
                server.send_message(msg)
        else:
            with smtplib.SMTP(cfg["host"], cfg["port"], timeout=15) as server:
                server.starttls(context=ctx)
                server.login(cfg["user"], cfg["pass"])
                server.send_message(msg)
        return True, None
    except Exception as exc:  # noqa: BLE001
        logger.warning("Email send failed to %s: %s", to_email, exc)
        return False, str(exc)[:500]


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value * 100:.1f}%"


def _fmt_money(value: float | None) -> str:
    if value is None:
        return "—"
    return f"${value:,.2f}"


def build_trigger_email(snapshot: Any, alert_type: str, current_price: float) -> tuple[str, str, str]:
    """Build subject, html, text for a BUY/SELL trigger email. Returns (subject, html, text)."""
    ticker = snapshot.ticker_symbol
    is_buy = alert_type == "buy"
    if is_buy:
        subject = f"[AlphaStock] BUY trigger hit: {ticker} @ {_fmt_money(current_price)}"
        headline = "BUY trigger reached"
        threshold = snapshot.buy_trigger_price
        color = "#059669"
        emoji = "🎯"
        direction_text = "dropped to or below your entry target"
    else:
        subject = f"[AlphaStock] SELL trigger hit: {ticker} @ {_fmt_money(current_price)}"
        headline = "SELL trigger reached"
        threshold = snapshot.sell_trigger_price
        color = "#dc2626"
        emoji = "🚀"
        direction_text = "rose to or above your overvalued threshold"

    created = snapshot.created_at.strftime("%Y-%m-%d") if snapshot.created_at else "—"
    growth = _fmt_pct(snapshot.growth_rate)
    target = _fmt_pct(snapshot.target_cagr)
    peBand = f"{snapshot.pe_bear or '—'} / {snapshot.pe_mid or '—'} / {snapshot.pe_bull or '—'}"

    html = f"""\
<html>
  <body style="font-family: -apple-system, system-ui, sans-serif; color: #0f172a; background: #f8fafc; margin: 0; padding: 24px;">
    <div style="max-width: 560px; margin: 0 auto; padding: 28px; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px;">
      <p style="margin: 0 0 4px; color: #64748b; font-size: 12px; letter-spacing: 0.05em; text-transform: uppercase;">AlphaStock Alert {emoji}</p>
      <h2 style="margin: 0 0 8px; color: {color}; font-size: 22px;">{headline}: {ticker}</h2>
      <p style="margin: 0 0 16px; color: #334155; font-size: 15px;">
        <strong>{ticker}</strong> at <strong>{_fmt_money(current_price)}</strong> {direction_text} of <strong>{_fmt_money(threshold)}</strong>.
      </p>
      <table style="border-collapse: collapse; width: 100%; margin-top: 16px; font-size: 14px;">
        <tr><td style="padding: 6px 0; color: #64748b; width: 45%;">Snapshot</td><td style="padding: 6px 0;">{snapshot.name} ({created})</td></tr>
        <tr><td style="padding: 6px 0; color: #64748b;">Growth rate</td><td style="padding: 6px 0;">{growth}</td></tr>
        <tr><td style="padding: 6px 0; color: #64748b;">Target CAGR</td><td style="padding: 6px 0;">{target}</td></tr>
        <tr><td style="padding: 6px 0; color: #64748b;">Horizon</td><td style="padding: 6px 0;">{snapshot.years or '—'} years</td></tr>
        <tr><td style="padding: 6px 0; color: #64748b;">P/E band (bear / mid / bull)</td><td style="padding: 6px 0;">{peBand}</td></tr>
        <tr><td style="padding: 6px 0; color: #64748b;">Terminal price</td><td style="padding: 6px 0;">{_fmt_money(snapshot.terminal_price)}</td></tr>
        <tr><td style="padding: 6px 0; color: #64748b;">Required entry</td><td style="padding: 6px 0;">{_fmt_money(snapshot.required_entry)}</td></tr>
      </table>
      <p style="margin-top: 24px; color: #94a3b8; font-size: 12px; line-height: 1.5;">
        This snapshot is now marked <strong>{alert_type}_triggered</strong> and will not fire again.
        Open AlphaStock to review and create a new snapshot to re-arm.
      </p>
    </div>
  </body>
</html>"""

    text = (
        f"{headline}: {ticker}\n"
        f"Current {_fmt_money(current_price)} {direction_text} of {_fmt_money(threshold)}.\n\n"
        f"Snapshot: {snapshot.name} ({created})\n"
        f"Growth {growth}, Target {target}, Horizon {snapshot.years or '—'} yrs\n"
        f"P/E band: {peBand}\n"
        f"Terminal price: {_fmt_money(snapshot.terminal_price)}\n"
        f"Required entry: {_fmt_money(snapshot.required_entry)}\n\n"
        f"This snapshot is now marked {alert_type}_triggered and will not fire again."
    )
    return subject, html, text


def send_trigger_email(to_email: str, snapshot: Any, alert_type: str, current_price: float) -> Tuple[bool, str | None]:
    subject, html, text = build_trigger_email(snapshot, alert_type, current_price)
    return send_email(to_email, subject, html, text)
