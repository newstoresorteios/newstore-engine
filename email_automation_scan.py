import os
import secrets
from datetime import datetime, timezone

from email_automation_events import notify_email_automation_event


EMAIL_REMAINING_THRESHOLDS = (
    (15, "EMAIL_DRAW_REMAINING_15"),
    (30, "EMAIL_DRAW_REMAINING_30"),
    (50, "EMAIL_DRAW_REMAINING_50"),
    (75, "EMAIL_DRAW_REMAINING_75"),
)


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        value = int(os.getenv(name, "").strip())
        return value if value >= minimum else default
    except Exception:
        return default


def _scan_id() -> str:
    return f"email-scan:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}:{secrets.token_hex(4)}"


def _draw_group(draw_type) -> str:
    return "draw" if (draw_type or "principal") == "principal" else "additional_draw"


def _remaining_threshold(remaining: int):
    for threshold, event_key in EMAIL_REMAINING_THRESHOLDS:
        if remaining <= threshold:
            return threshold, event_key
    return None


def _load_open_draws(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, COALESCE(draw_type, 'principal') AS draw_type, product_name
              FROM public.draws
             WHERE status = 'open'
             ORDER BY id
        """)
        return cur.fetchall() or []


def _load_numbers_snapshot(conn, draw_id: int) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)::int AS total_numbers,
                   COUNT(*) FILTER (WHERE status = 'available')::int AS remaining_numbers,
                   COUNT(*) FILTER (WHERE status <> 'available')::int AS sold_numbers
              FROM public.numbers
             WHERE draw_id = %s
        """, (draw_id,))
        return cur.fetchone() or {"total_numbers": 0, "remaining_numbers": 0, "sold_numbers": 0}


def _load_closed_draws(conn, lookback_hours: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, COALESCE(draw_type, 'principal') AS draw_type,
                   product_name, closed_at
              FROM public.draws
             WHERE status = 'closed'
               AND closed_at >= NOW() - (%s * INTERVAL '1 hour')
             ORDER BY id
        """, (lookback_hours,))
        return cur.fetchall() or []


def _emit(event_key, reference_type, reference_key, metadata, scan_id):
    return notify_email_automation_event(
        event_key=event_key,
        reference_type=reference_type,
        reference_key=reference_key,
        metadata=metadata,
        scan_id=scan_id,
        occurred_at=datetime.now(timezone.utc).isoformat(),
    )


def run_email_automation_scan(conn):
    scan_id = _scan_id()
    lookback_hours = _env_int("EMAIL_AUTOMATION_DEFAULT_LOOKBACK_HOURS", 24)
    summary = {"ok": True, "scan_id": scan_id, "remaining_checked": 0, "closed_checked": 0, "events": 0, "sent": 0, "failed": 0, "by_event_key": {}}

    for draw in _load_open_draws(conn):
        draw_id = int(draw["id"])
        snapshot = _load_numbers_snapshot(conn, draw_id)
        remaining = int(snapshot.get("remaining_numbers") or 0)
        selected = _remaining_threshold(remaining)
        summary["remaining_checked"] += 1
        if not selected:
            continue
        threshold, event_key = selected
        group = _draw_group(draw.get("draw_type"))
        reference_key = f"{group}:{draw_id}:email_remaining:{threshold}"
        result = _emit(event_key, "draw" if group == "draw" else "additional_draw", reference_key, {
            "draw_id": draw_id,
            "draw_type": draw.get("draw_type") or "principal",
            "remaining_numbers": remaining,
            "threshold": threshold,
            "product_name": draw.get("product_name") or None,
        }, scan_id)
        summary["events"] += 1
        summary["by_event_key"][event_key] = summary["by_event_key"].get(event_key, 0) + 1
        summary["sent"] += int(bool(result.get("ok")))
        summary["failed"] += int(not result.get("ok"))

    for draw in _load_closed_draws(conn, lookback_hours):
        draw_id = int(draw["id"])
        group = _draw_group(draw.get("draw_type"))
        event_key = "DRAW_CLOSED"
        reference_key = f"{group}:{draw_id}:closed_email"
        result = _emit(event_key, "draw" if group == "draw" else "additional_draw", reference_key, {
            "draw_id": draw_id,
            "draw_type": draw.get("draw_type") or "principal",
            "closed_at": draw.get("closed_at").isoformat() if hasattr(draw.get("closed_at"), "isoformat") else draw.get("closed_at"),
            "product_name": draw.get("product_name") or None,
        }, scan_id)
        summary["closed_checked"] += 1
        summary["events"] += 1
        summary["by_event_key"][event_key] = summary["by_event_key"].get(event_key, 0) + 1
        summary["sent"] += int(bool(result.get("ok")))
        summary["failed"] += int(not result.get("ok"))
    return summary
