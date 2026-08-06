import os
import secrets
from datetime import date, datetime, time, timezone

from email_automation_events import notify_email_automation_event


EMAIL_REMAINING_THRESHOLDS = (
    (15, "EMAIL_DRAW_REMAINING_15"),
    (30, "EMAIL_DRAW_REMAINING_30"),
    (50, "EMAIL_DRAW_REMAINING_50"),
    (75, "EMAIL_DRAW_REMAINING_75"),
)
EMAIL_BALANCE_EXPIRING_EVENTS = {
    30: "EMAIL_BALANCE_EXPIRING_30_DAYS",
    20: "EMAIL_BALANCE_EXPIRING_20_DAYS",
    10: "EMAIL_BALANCE_EXPIRING_10_DAYS",
    7: "EMAIL_BALANCE_EXPIRING_7_DAYS",
    3: "EMAIL_BALANCE_EXPIRING_3_DAYS",
}
EMAIL_DEFAULT_LOOKBACK_HOURS = 24
EMAIL_PUBLISHED_LOOKBACK_HOURS = 24
EMAIL_CLOSED_LOOKBACK_HOURS = 72


def _env_true(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() == "true"


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        value = int(os.getenv(name, "").strip())
        return value if value >= minimum else default
    except Exception:
        return default


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _scan_id(now: datetime | None = None) -> str:
    scan_time = now or _utc_now()
    return f"email-scan:{scan_time.strftime('%Y%m%dT%H%M%SZ')}:{secrets.token_hex(4)}"


def _normalized_draw_type(draw_type) -> str:
    return str(draw_type or "principal").strip().lower() or "principal"


def _draw_group(draw_type) -> str:
    normalized = _normalized_draw_type(draw_type)
    if normalized == "principal":
        return "draw"
    if normalized in ("adicional", "secundario"):
        return "additional_draw"
    raise ValueError("unsupported_draw_type")


def _draw_type_label(draw_type) -> str:
    normalized = _normalized_draw_type(draw_type)
    if normalized == "adicional":
        return "Sorteio adicional"
    if normalized == "secundario":
        return "Sorteio secundário"
    return "Sorteio principal"


def _draw_name(draw: dict) -> str:
    for field in ("draw_name", "product_name", "banner_title"):
        configured_name = str(draw.get(field) or "").strip()
        if configured_name:
            return configured_name

    draw_id = int(draw["id"])
    draw_type = _normalized_draw_type(draw.get("draw_type"))
    if draw_type == "adicional":
        return f"Sorteio adicional #{draw_id}"
    if draw_type == "secundario":
        return f"Sorteio secundário #{draw_id}"
    return "Sorteio principal"


def _remaining_threshold(remaining: int):
    for threshold, event_key in EMAIL_REMAINING_THRESHOLDS:
        if remaining <= threshold:
            return threshold, event_key
    return None


def _load_published_draws(conn, lookback_hours: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.id,
                   d.status,
                   COALESCE(d.draw_type, 'principal') AS draw_type,
                   NULLIF(BTRIM(to_jsonb(d)->>'product_name'), '') AS product_name,
                   COALESCE(
                       NULLIF(BTRIM(to_jsonb(d)->>'product_name'), ''),
                       NULLIF(BTRIM(to_jsonb(d)->>'name'), ''),
                       NULLIF(BTRIM(to_jsonb(d)->>'title'), '')
                   ) AS draw_name,
                   NULLIF(BTRIM(to_jsonb(d)->>'description'), '') AS draw_description,
                   NULLIF(BTRIM(to_jsonb(d)->>'banner_title'), '') AS banner_title,
                   (
                       SELECT COUNT(*) FILTER (WHERE n.status = 'available')::int
                         FROM public.numbers n
                        WHERE n.draw_id = d.id
                   ) AS remaining_numbers,
                   d.opened_at
              FROM public.draws d
             WHERE d.opened_at IS NOT NULL
               AND d.opened_at >= NOW() - (%s * INTERVAL '1 hour')
               AND d.status = 'open'
               AND COALESCE(d.draw_type, 'principal') IN (
                   'principal',
                   'adicional',
                   'secundario'
               )
             ORDER BY d.id
        """, (lookback_hours,))
        return cur.fetchall() or []


def _load_open_draws(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.id,
                   d.status,
                   COALESCE(d.draw_type, 'principal') AS draw_type,
                   NULLIF(BTRIM(to_jsonb(d)->>'product_name'), '') AS product_name,
                   COALESCE(
                       NULLIF(BTRIM(to_jsonb(d)->>'product_name'), ''),
                       NULLIF(BTRIM(to_jsonb(d)->>'name'), ''),
                       NULLIF(BTRIM(to_jsonb(d)->>'title'), '')
                   ) AS draw_name,
                   NULLIF(BTRIM(to_jsonb(d)->>'description'), '') AS draw_description,
                   NULLIF(BTRIM(to_jsonb(d)->>'banner_title'), '') AS banner_title
              FROM public.draws d
             WHERE d.status = 'open'
               AND COALESCE(d.draw_type, 'principal') IN (
                   'principal',
                   'adicional',
                   'secundario'
               )
             ORDER BY d.id
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
        return cur.fetchone() or {
            "total_numbers": 0,
            "remaining_numbers": 0,
            "sold_numbers": 0,
        }


def _load_closed_draws(conn, lookback_hours: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.id,
                   d.status,
                   COALESCE(d.draw_type, 'principal') AS draw_type,
                   NULLIF(BTRIM(to_jsonb(d)->>'product_name'), '') AS product_name,
                   COALESCE(
                       NULLIF(BTRIM(to_jsonb(d)->>'product_name'), ''),
                       NULLIF(BTRIM(to_jsonb(d)->>'name'), ''),
                       NULLIF(BTRIM(to_jsonb(d)->>'title'), '')
                   ) AS draw_name,
                   NULLIF(BTRIM(to_jsonb(d)->>'description'), '') AS draw_description,
                   NULLIF(BTRIM(to_jsonb(d)->>'banner_title'), '') AS banner_title,
                   (
                       SELECT COUNT(*) FILTER (WHERE n.status = 'available')::int
                         FROM public.numbers n
                        WHERE n.draw_id = d.id
                   ) AS remaining_numbers,
                   d.closed_at
              FROM public.draws d
             WHERE d.closed_at IS NOT NULL
               AND d.closed_at >= NOW() - (%s * INTERVAL '1 hour')
               AND d.status IN ('closed', 'sorteado')
               AND COALESCE(d.draw_type, 'principal') IN (
                   'principal',
                   'adicional',
                   'secundario'
               )
             ORDER BY d.id
        """, (lookback_hours,))
        return cur.fetchall() or []


def _load_balance_rows(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT user_id,
                   name,
                   email,
                   balance_cents,
                   balance_reference_at,
                   expires_at,
                   expires_on,
                   days_to_expire,
                   expiry_source
              FROM public.user_coupon_balance_expiry
             WHERE balance_cents > 0
             ORDER BY expires_on ASC, user_id ASC
        """)
        return cur.fetchall() or []


def _iso_datetime(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


def _as_utc_datetime(value):
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    elif isinstance(value, str) and value.strip():
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _date_key(value):
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        raw = value.strip()
        try:
            return date.fromisoformat(raw[:10]).isoformat()
        except ValueError:
            return None
    return None


def _effective_from(now: datetime):
    raw = os.getenv("EMAIL_BALANCE_AUTOMATION_EFFECTIVE_FROM", "").strip()
    if not raw:
        return now
    parsed = _as_utc_datetime(raw)
    if parsed is None:
        raise ValueError("email_balance_effective_from_invalid")
    return parsed


def _valid_user_id(value) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def _valid_email(value) -> bool:
    if not isinstance(value, str):
        return False
    email = value.strip()
    return bool(email) and "@" in email


def _balance_stage(days_to_expire: int):
    event_key = EMAIL_BALANCE_EXPIRING_EVENTS.get(days_to_expire)
    if event_key:
        return event_key, f"{days_to_expire}_days"
    if days_to_expire < 0:
        return "EMAIL_BALANCE_EXPIRED", "expired"
    return None


def _draw_metadata(draw: dict, remaining_numbers=None, extra=None):
    draw_type = _normalized_draw_type(draw.get("draw_type"))
    metadata = {
        "draw_id": int(draw["id"]),
        "draw_type": draw_type,
        "draw_name": _draw_name(draw),
        "draw_type_label": _draw_type_label(draw_type),
        "status": draw.get("status"),
    }
    if remaining_numbers is None:
        remaining_numbers = draw.get("remaining_numbers")
    if remaining_numbers is not None:
        metadata["remaining_numbers"] = max(0, int(remaining_numbers or 0))
    for field in (
        "draw_description",
        "product_name",
        "banner_title",
    ):
        value = str(draw.get(field) or "").strip()
        if value:
            metadata[field] = value
    if extra:
        metadata.update(extra)
    return metadata


def _event_log_context(event_key, reference_key, metadata):
    fields = (
        ("draw_id", "draw_id"),
        ("draw_type", "draw_type"),
        ("draw_name", "draw_name"),
        ("user_id", "user_id"),
        ("expires_on", "expires_on"),
        ("days_to_expire", "days_to_expire"),
        ("balance_cents", "balance_cents"),
    )
    context = {}
    for output_field, metadata_field in fields:
        if metadata.get(metadata_field) is not None:
            context[output_field] = metadata[metadata_field]
    context["event_key"] = event_key
    context["reference_key"] = reference_key
    return context


def _emit(
    event_key,
    reference_type,
    reference_key,
    metadata,
    scan_id,
    occurred_at=None,
    dry_run=False,
):
    log_context = _event_log_context(event_key, reference_key, metadata)
    if dry_run:
        print("[email-automation] event_detected", {
            **log_context,
            "dry_run": True,
            "would_publish": True,
        })
        return {"ok": True, "dry_run": True, "would_publish": True}

    print("[email-automation] event_detected", log_context)
    try:
        result = notify_email_automation_event(
            event_key=event_key,
            reference_type=reference_type,
            reference_key=reference_key,
            metadata=metadata,
            scan_id=scan_id,
            occurred_at=(
                _iso_datetime(occurred_at)
                if occurred_at is not None
                else _utc_now().isoformat()
            ),
        )
    except Exception as exc:
        result = {
            "ok": False,
            "reason": "backend_request_failed",
            "error_type": exc.__class__.__name__,
        }

    if not isinstance(result, dict) or result.get("ok") is not True:
        print("[email-automation] event_failed", {
            **log_context,
            "status": result.get("status") if isinstance(result, dict) else None,
            "reason": (
                result.get("reason") or "backend_event_failed"
                if isinstance(result, dict)
                else "backend_event_failed"
            ),
        })
    return result


def _nonnegative_count(value) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _delivery_counts(result):
    result_is_dict = isinstance(result, dict)
    result_dict = result if result_is_dict else {}
    backend_result = result_dict.get("data")
    if not isinstance(backend_result, dict):
        backend_result = result_dict
    sent = _nonnegative_count(backend_result.get("sent"))
    failed = _nonnegative_count(backend_result.get("failed"))
    skipped = _nonnegative_count(
        backend_result.get("skipped", backend_result.get("deduped"))
    )
    if (
        not result_is_dict
        or result_dict.get("ok") is not True
        or backend_result.get("ok") is False
    ):
        failed = max(failed, 1)
    if result_dict.get("deduped") and skipped == 0:
        skipped = 1
    if backend_result.get("status") == "skipped" and skipped == 0:
        skipped = 1
    return {"sent": sent, "failed": failed, "skipped": skipped}


def _result_is_deduped(result) -> bool:
    if not isinstance(result, dict):
        return False
    if result.get("deduped"):
        return True
    data = result.get("data")
    if not isinstance(data, dict):
        return False
    return _nonnegative_count(data.get("deduped")) > 0 or data.get("status") == "skipped"


def _add_delivery_counts(summary, result):
    counts = _delivery_counts(result)
    for key in ("sent", "failed", "skipped"):
        summary[key] += counts[key]


def _record_event(summary, event_key, result):
    summary["events"] += 1
    summary["by_event_key"][event_key] = (
        summary["by_event_key"].get(event_key, 0) + 1
    )
    _add_delivery_counts(summary, result)


def _record_balance_event(summary, event_key, result):
    summary["balance_events"] += 1
    is_dry_run = isinstance(result, dict) and result.get("dry_run")
    if not is_dry_run:
        delivery_counts = _delivery_counts(result)
        if (
            not isinstance(result, dict)
            or result.get("ok") is not True
            or delivery_counts["failed"] > 0
        ):
            summary["balance_failed"] += 1
        elif _result_is_deduped(result):
            summary["balance_deduped"] += 1
        else:
            summary["balance_sent"] += 1
    _record_event(summary, event_key, result)


def _rollback_if_possible(conn):
    rollback = getattr(conn, "rollback", None)
    if callable(rollback):
        rollback()


def _close_if_possible(conn):
    close = getattr(conn, "close", None)
    if callable(close):
        close()


def _balance_skip(summary, reason, row, days_to_expire=None):
    summary["balance_skipped"] += 1
    print("[email-automation] balance_skipped", {
        "user_id": row.get("user_id"),
        "expires_on": _date_key(row.get("expires_on")),
        "days_to_expire": days_to_expire,
        "reason": reason,
    })


def _build_balance_candidates(conn, summary, scan_time):
    rows = _load_balance_rows(conn)
    summary["balance_checked"] = len(rows)
    backfill_enabled = _env_true(
        "EMAIL_BALANCE_EXPIRED_BACKFILL_ENABLED",
        False,
    )
    effective_from = None if backfill_enabled else _effective_from(scan_time)
    candidates = []

    for row in rows:
        user_id_raw = row.get("user_id")
        if not _valid_user_id(user_id_raw):
            _balance_skip(summary, "balance_user_id_invalid", row)
            continue
        user_id = int(user_id_raw)

        try:
            balance_cents = int(row.get("balance_cents") or 0)
        except (TypeError, ValueError):
            balance_cents = 0
        if balance_cents <= 0:
            _balance_skip(summary, "balance_not_positive", row)
            continue
        if not _valid_email(row.get("email")):
            _balance_skip(summary, "balance_email_invalid", row)
            continue

        expires_at = _as_utc_datetime(row.get("expires_at"))
        expires_on = _date_key(row.get("expires_on"))
        if expires_at is None or expires_on is None:
            _balance_skip(summary, "balance_expiration_missing", row)
            continue
        summary["balance_with_valid_expiry"] += 1

        try:
            days_to_expire = int(row.get("days_to_expire"))
        except (TypeError, ValueError):
            _balance_skip(summary, "balance_days_to_expire_invalid", row)
            continue
        selected = _balance_stage(days_to_expire)
        if selected is None:
            _balance_skip(
                summary,
                "balance_stage_not_eligible",
                row,
                days_to_expire,
            )
            continue
        event_key, stage = selected

        if (
            stage == "expired"
            and not backfill_enabled
            and expires_at < effective_from
        ):
            summary["balance_expired_before_effective_from"] += 1
            _balance_skip(
                summary,
                "balance_expired_before_effective_from",
                row,
                days_to_expire,
            )
            continue

        reference_key = (
            f"user_balance:{user_id}:expires:{expires_on}:email:{stage}"
        )
        metadata = {
            "user_id": user_id,
            "balance_cents": balance_cents,
            "balance_reference_at": _iso_datetime(
                row.get("balance_reference_at")
            ),
            "expires_at": expires_at.isoformat(),
            "expires_on": expires_on,
            "days_to_expire": days_to_expire,
            "expiry_source": row.get("expiry_source"),
        }
        candidates.append({
            "kind": "balance",
            "event_key": event_key,
            "reference_type": "user_balance",
            "reference_key": reference_key,
            "metadata": metadata,
            "occurred_at": expires_at if stage == "expired" else scan_time,
        })
        summary["balance_eligible"] += 1

    return candidates


def run_email_automation_scan(
    conn,
    close_connection_before_publish: bool = False,
    now: datetime | None = None,
):
    scan_time = _as_utc_datetime(now) if now is not None else _utc_now()
    scan_id = _scan_id(scan_time)
    dry_run = _env_true("EMAIL_AUTOMATION_DRY_RUN", False)
    default_lookback_hours = _env_int(
        "EMAIL_AUTOMATION_DEFAULT_LOOKBACK_HOURS",
        EMAIL_DEFAULT_LOOKBACK_HOURS,
    )
    published_lookback_hours = _env_int(
        "EMAIL_AUTOMATION_PUBLISHED_LOOKBACK_HOURS",
        default_lookback_hours,
    )
    closed_lookback_hours = _env_int(
        "EMAIL_AUTOMATION_CLOSED_LOOKBACK_HOURS",
        EMAIL_CLOSED_LOOKBACK_HOURS,
    )
    summary = {
        "ok": True,
        "dry_run": dry_run,
        "scan_id": scan_id,
        "published_checked": 0,
        "remaining_checked": 0,
        "closed_checked": 0,
        "balance_checked": 0,
        "balance_with_valid_expiry": 0,
        "balance_eligible": 0,
        "balance_events": 0,
        "balance_sent": 0,
        "balance_deduped": 0,
        "balance_failed": 0,
        "balance_skipped": 0,
        "balance_expired_before_effective_from": 0,
        "events": 0,
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "by_event_key": {},
    }
    candidates = []

    for draw in _load_published_draws(conn, published_lookback_hours):
        draw_id = int(draw["id"])
        group = _draw_group(draw.get("draw_type"))
        event_key = "NEW_DRAW_PUBLISHED"
        opened_at = draw.get("opened_at")
        candidates.append({
            "kind": "draw",
            "event_key": event_key,
            "reference_type": "draw" if group == "draw" else "additional_draw",
            "reference_key": f"{group}:{draw_id}:published_email",
            "metadata": _draw_metadata(
                draw,
                extra={"opened_at": _iso_datetime(opened_at)},
            ),
            "occurred_at": opened_at,
        })
        summary["published_checked"] += 1

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
        candidates.append({
            "kind": "draw",
            "event_key": event_key,
            "reference_type": "draw" if group == "draw" else "additional_draw",
            "reference_key": f"{group}:{draw_id}:email_remaining:{threshold}",
            "metadata": _draw_metadata(
                draw,
                remaining_numbers=remaining,
                extra={"threshold": threshold},
            ),
            "occurred_at": scan_time,
        })

    for draw in _load_closed_draws(conn, closed_lookback_hours):
        draw_id = int(draw["id"])
        group = _draw_group(draw.get("draw_type"))
        closed_at = draw.get("closed_at")
        candidates.append({
            "kind": "draw",
            "event_key": "DRAW_CLOSED",
            "reference_type": "draw" if group == "draw" else "additional_draw",
            "reference_key": f"{group}:{draw_id}:closed_email",
            "metadata": _draw_metadata(
                draw,
                extra={"closed_at": _iso_datetime(closed_at)},
            ),
            "occurred_at": closed_at,
        })
        summary["closed_checked"] += 1

    try:
        candidates.extend(_build_balance_candidates(conn, summary, scan_time))
    except Exception as exc:
        _rollback_if_possible(conn)
        summary["balance_failed"] += 1
        summary["failed"] += 1
        print("[email-automation] balance_scan_failed", {
            "reason": "balance_source_unavailable",
            "error_type": exc.__class__.__name__,
        })

    _rollback_if_possible(conn)
    if close_connection_before_publish:
        _close_if_possible(conn)

    for candidate in candidates:
        result = _emit(
            candidate["event_key"],
            candidate["reference_type"],
            candidate["reference_key"],
            candidate["metadata"],
            scan_id,
            occurred_at=candidate["occurred_at"],
            dry_run=dry_run,
        )
        if candidate["kind"] == "balance":
            _record_balance_event(summary, candidate["event_key"], result)
        else:
            _record_event(summary, candidate["event_key"], result)

    summary["ok"] = summary["failed"] == 0
    return summary
