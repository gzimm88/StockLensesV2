"""
Projection trigger checker job.

Runs every 20 minutes (staggered after price fetch). For each active snapshot:
- Compare LatestPrice against buy_trigger_price / sell_trigger_price
- If crossed, mark snapshot as triggered (fire-once), create AlertNotification, send email
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from backend.database import SessionLocal
from backend.models import (
    AccountWatchlistEntry,
    AlertNotification,
    LatestPrice,
    ProjectionSnapshot,
    User,
)
from backend.services.email_service import send_trigger_email

logger = logging.getLogger(__name__)


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def run_projection_trigger_check_job() -> dict[str, int]:
    ts = _utcnow_naive()
    db = SessionLocal()
    checked = 0
    triggered = 0
    emailed = 0
    try:
        rows = db.query(ProjectionSnapshot).filter(ProjectionSnapshot.status == "active").all()
        # Custom watchlist triggers (no snapshot or in addition to snapshot)
        wl_with_custom = (
            db.query(AccountWatchlistEntry)
            .filter(
                (AccountWatchlistEntry.custom_buy_trigger_price.is_not(None))
                | (AccountWatchlistEntry.custom_sell_trigger_price.is_not(None))
            )
            .all()
        )

        if not rows and not wl_with_custom:
            return {"checked": 0, "triggered": 0, "emailed": 0}

        symbols = {r.ticker_symbol for r in rows} | {w.ticker_symbol for w in wl_with_custom}
        prices: dict[str, LatestPrice] = {
            p.ticker: p for p in db.query(LatestPrice).filter(LatestPrice.ticker.in_(list(symbols))).all()
        }
        user_ids = {r.user_id for r in rows} | {w.user_id for w in wl_with_custom}
        users: dict[str, User] = {
            u.id: u for u in db.query(User).filter(User.id.in_(list(user_ids))).all()
        }

        # Snapshots: skip checking the directional trigger that the watchlist entry overrides
        # (active snapshot triggers always take precedence over custom watchlist triggers).
        for snap in rows:
            checked += 1
            lp = prices.get(snap.ticker_symbol)
            if lp is None or lp.price is None:
                continue
            try:
                price = float(lp.price)
            except (TypeError, ValueError):
                continue
            fired: str | None = None
            if snap.buy_trigger_price is not None and price <= float(snap.buy_trigger_price):
                fired = "buy"
            elif snap.sell_trigger_price is not None and price >= float(snap.sell_trigger_price):
                fired = "sell"
            if fired is None:
                continue

            threshold = float(snap.buy_trigger_price if fired == "buy" else snap.sell_trigger_price)
            # Fire-once: update snapshot status + create alert
            snap.status = f"{fired}_triggered"
            snap.triggered_at = ts
            snap.triggered_type = fired
            snap.triggered_price = price
            snap.updated_at = ts

            alert = AlertNotification(
                id=str(uuid.uuid4()),
                user_id=snap.user_id,
                ticker_symbol=snap.ticker_symbol,
                snapshot_id=snap.id,
                alert_type=fired,
                threshold_price=threshold,
                triggered_price=price,
                triggered_at=ts,
                email_sent=False,
                read=False,
                dismissed=False,
                created_at=ts,
            )
            db.add(alert)
            db.commit()
            triggered += 1

            # Email (best-effort)
            user = users.get(snap.user_id)
            if user and user.email:
                try:
                    ok, err = send_trigger_email(user.email, snap, fired, price)
                    alert.email_sent = bool(ok)
                    alert.email_error = err
                    db.commit()
                    if ok:
                        emailed += 1
                except Exception as exc:  # noqa: BLE001
                    logger.warning("send_trigger_email raised for %s: %s", snap.ticker_symbol, exc)
                    alert.email_error = str(exc)[:500]
                    db.commit()

        # Custom watchlist triggers (no associated snapshot driving them).
        # Skip tickers that already have an active snapshot for the same user
        # (snapshot triggers take precedence and have already been checked above).
        active_snapshot_keys = {(r.user_id, r.ticker_symbol.upper()) for r in rows if r.status == "active"}
        for wl in wl_with_custom:
            sym = wl.ticker_symbol.upper()
            if (wl.user_id, sym) in active_snapshot_keys:
                continue
            checked += 1
            lp = prices.get(sym)
            if lp is None or lp.price is None:
                continue
            try:
                price = float(lp.price)
            except (TypeError, ValueError):
                continue
            fired_kind: str | None = None
            threshold: float | None = None
            if wl.custom_buy_trigger_price is not None and price <= float(wl.custom_buy_trigger_price):
                fired_kind = "buy"
                threshold = float(wl.custom_buy_trigger_price)
            elif wl.custom_sell_trigger_price is not None and price >= float(wl.custom_sell_trigger_price):
                fired_kind = "sell"
                threshold = float(wl.custom_sell_trigger_price)
            if fired_kind is None or threshold is None:
                continue

            # Fire-once: clear the side that fired so we don't keep alerting
            if fired_kind == "buy":
                wl.custom_buy_trigger_price = None
            else:
                wl.custom_sell_trigger_price = None
            wl.updated_at = ts

            alert = AlertNotification(
                id=str(uuid.uuid4()),
                user_id=wl.user_id,
                ticker_symbol=sym,
                snapshot_id="",  # No snapshot — empty marker
                alert_type=fired_kind,
                threshold_price=threshold,
                triggered_price=price,
                triggered_at=ts,
                email_sent=False,
                read=False,
                dismissed=False,
                created_at=ts,
            )
            db.add(alert)
            db.commit()
            triggered += 1

            user = users.get(wl.user_id)
            if user and user.email:
                try:
                    # Build a lightweight pseudo-snapshot for the email template
                    class _PseudoSnapshot:
                        ticker_symbol = sym
                        name = f"{sym} watchlist trigger"
                        created_at = ts
                        growth_rate = None
                        target_cagr = None
                        years = None
                        pe_bear = None
                        pe_mid = None
                        pe_bull = None
                        terminal_price = None
                        required_entry = None
                        buy_trigger_price = threshold if fired_kind == "buy" else None
                        sell_trigger_price = threshold if fired_kind == "sell" else None

                    ok, err = send_trigger_email(user.email, _PseudoSnapshot(), fired_kind, price)
                    alert.email_sent = bool(ok)
                    alert.email_error = err
                    db.commit()
                    if ok:
                        emailed += 1
                except Exception as exc:  # noqa: BLE001
                    logger.warning("send_trigger_email raised for %s: %s", sym, exc)
                    alert.email_error = str(exc)[:500]
                    db.commit()

        return {"checked": checked, "triggered": triggered, "emailed": emailed}
    except Exception as exc:  # noqa: BLE001
        logger.exception("run_projection_trigger_check_job failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"checked": checked, "triggered": triggered, "emailed": emailed, "error": str(exc)[:200]}
    finally:
        db.close()
