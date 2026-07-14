import os
import secrets
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone

from push_automation_events import notify_push_automation_event


REMAINING_THRESHOLDS = (
    (10, "DRAW_REMAINING_NUMBERS_10"),
    (20, "DRAW_REMAINING_NUMBERS_20"),
    (50, "DRAW_REMAINING_NUMBERS_50"),
    (75, "DRAW_REMAINING_NUMBERS_75"),
)
BALANCE_EXPIRING_EVENTS = {
    30: "BALANCE_EXPIRING_30_DAYS",
    15: "BALANCE_EXPIRING_15_DAYS",
    10: "BALANCE_EXPIRING_10_DAYS",
    7: "BALANCE_EXPIRING_7_DAYS",
}
KNOWN_AUTOMATION_EVENT_KEYS = (
    "NEW_DRAW_PUBLISHED",
    "DRAW_REMAINING_NUMBERS_75",
    "DRAW_REMAINING_NUMBERS_50",
    "DRAW_REMAINING_NUMBERS_20",
    "DRAW_REMAINING_NUMBERS_10",
    "WINNER_DEFINED",
    "BALANCE_EXPIRING_30_DAYS",
    "BALANCE_EXPIRING_15_DAYS",
    "BALANCE_EXPIRING_10_DAYS",
    "BALANCE_EXPIRING_7_DAYS",
    "BALANCE_EXPIRED",
)
WINNER_TEMPORAL_COLUMNS = (
    "winner_defined_at",
    "drawn_at",
    "finished_at",
    "updated_at",
    "created_at",
)
DRAW_TEMPORAL_COLUMNS = (
    "updated_at",
    "created_at",
    "opened_at",
    "started_at",
)
RESERVATION_TEMPORAL_COLUMNS = (
    "paid_at",
    "updated_at",
    "created_at",
)
PAYMENT_TEMPORAL_COLUMNS = (
    "paid_at",
    "approved_at",
    "updated_at",
    "created_at",
)


def run_push_automation_scan(conn):
    config = _scan_config()
    ctx = _new_scan_context(config)
    print("[push-automation] scan:start", _scan_config_snapshot(config, ctx["scan_id"]))

    remaining_summary = emit_remaining_numbers_events(conn, ctx)
    winner_summary = emit_winner_defined_events(conn, ctx)
    balance_summary = emit_balance_expiration_events(conn, ctx)

    final_summary = _final_scan_summary(ctx)
    if config["preview_only"]:
        print("[push-automation] preview:summary", final_summary)
    print("[push-automation] scan:summary", final_summary)

    return {
        "ok": True,
        "scan_id": ctx["scan_id"],
        "remaining_numbers_checked": remaining_summary["checked"],
        "winner_checked": winner_summary["checked"],
        "balance_checked": balance_summary["checked"],
        "events_candidates": final_summary["events_candidates"],
        "events_attempted": final_summary["events_attempted"],
        "events_blocked": final_summary["events_blocked"],
        "events_skipped": final_summary["events_skipped"],
        "events_sent_to_backend": final_summary["events_sent_to_backend"],
        "by_event_key": final_summary["by_event_key"],
        "remaining_numbers": remaining_summary,
        "winner_defined": winner_summary,
        "balance_expiration": balance_summary,
    }


def _env_true(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() == "true"


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        parsed = int(value)
        if parsed < minimum:
            return default
        return parsed
    except Exception:
        return default


def _allowed_event_keys() -> set[str] | None:
    configured = os.getenv("PUSH_AUTOMATION_EVENT_KEYS", "").strip()
    if not configured:
        return None

    return {
        key.strip()
        for key in configured.replace(";", ",").split(",")
        if key.strip()
    }


def _allowed_events_count() -> int:
    allowed = _allowed_event_keys()
    if allowed is None:
        return len(KNOWN_AUTOMATION_EVENT_KEYS)
    return len(allowed)


def _event_key_allowed(event_key: str) -> bool:
    allowed = _allowed_event_keys()
    return allowed is None or event_key in allowed


def _scan_config() -> dict:
    default_lookback = _env_int("PUSH_AUTOMATION_DEFAULT_LOOKBACK_HOURS", 24, 1)
    return {
        "no_backfill": _env_true("PUSH_AUTOMATION_NO_BACKFILL", True),
        "max_events_per_scan": _env_int("PUSH_AUTOMATION_MAX_EVENTS_PER_SCAN", 5, 1),
        "max_events_per_key_per_scan": _env_int("PUSH_AUTOMATION_MAX_EVENTS_PER_KEY_PER_SCAN", 2, 1),
        "require_occurred_at": _env_true("PUSH_AUTOMATION_REQUIRE_OCCURRED_AT", True),
        "default_lookback_hours": default_lookback,
        "allow_large_batch": _env_true("PUSH_AUTOMATION_ALLOW_LARGE_BATCH", False),
        "preview_only": _env_true("PUSH_AUTOMATION_PREVIEW_ONLY", False),
        "winner_lookback_hours": _env_int("PUSH_AUTOMATION_WINNER_LOOKBACK_HOURS", default_lookback, 1),
        "winner_max_events_per_scan": _env_int("PUSH_AUTOMATION_WINNER_MAX_EVENTS_PER_SCAN", 2, 1),
        "remaining_lookback_hours": _env_int("PUSH_AUTOMATION_REMAINING_LOOKBACK_HOURS", default_lookback, 1),
        "remaining_max_events_per_scan": _env_int("PUSH_AUTOMATION_REMAINING_MAX_EVENTS_PER_SCAN", 1, 1),
        "balance_lookback_hours": _env_int("PUSH_AUTOMATION_BALANCE_LOOKBACK_HOURS", default_lookback, 1),
        "balance_max_events_per_scan": _env_int("PUSH_AUTOMATION_BALANCE_MAX_EVENTS_PER_SCAN", 5, 1),
    }


def _coupon_valid_days() -> int:
    return _env_int("TRAY_COUPON_VALID_DAYS", 180, 1)


def _generate_scan_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"push-scan:{timestamp}:{secrets.token_hex(4)}"


def _new_scan_context(config: dict) -> dict:
    return {
        "scan_id": _generate_scan_id(),
        "config": config,
        "events_by_key": defaultdict(_empty_event_key_stats),
    }


def _empty_event_key_stats() -> dict:
    return {
        "events_candidates": 0,
        "events_attempted": 0,
        "events_blocked": 0,
        "events_skipped": 0,
        "events_sent_to_backend": 0,
    }


def _scan_config_snapshot(config: dict, scan_id: str) -> dict:
    return {
        "scan_id": scan_id,
        "enabled": _env_true("PUSH_AUTOMATION_SCAN_ENABLED"),
        "events_enabled": _env_true("PUSH_AUTOMATION_EVENTS_ENABLED"),
        "allowed_events_count": _allowed_events_count(),
        "backend_configured": bool(os.getenv("BACKEND_INTERNAL_API_BASE", "").strip()),
        "token_configured": bool(os.getenv("PUSH_INTERNAL_EVENTS_TOKEN", "").strip()),
        "no_backfill": config["no_backfill"],
        "preview_only": config["preview_only"],
        "require_occurred_at": config["require_occurred_at"],
        "max_events_per_scan": config["max_events_per_scan"],
        "max_events_per_key_per_scan": config["max_events_per_key_per_scan"],
    }


def _empty_summary() -> dict:
    return {
        "checked": 0,
        "events_candidates": 0,
        "events_attempted": 0,
        "events_blocked": 0,
        "events_skipped": 0,
        "events_sent_to_backend": 0,
    }


def _summary_from_results(checked: int, results: list[dict]) -> dict:
    return {
        "checked": checked,
        "events_candidates": len(results),
        "events_attempted": sum(1 for result in results if result.get("attempted")),
        "events_blocked": sum(1 for result in results if result.get("blocked")),
        "events_skipped": sum(1 for result in results if result.get("skipped")),
        "events_sent_to_backend": sum(1 for result in results if result.get("sent_to_backend")),
    }


def _final_scan_summary(ctx: dict) -> dict:
    by_event_key = {
        event_key: dict(stats)
        for event_key, stats in sorted(ctx["events_by_key"].items())
    }
    totals = _empty_event_key_stats()
    for stats in by_event_key.values():
        for key in totals:
            totals[key] += int(stats.get(key, 0) or 0)
    totals["scan_id"] = ctx["scan_id"]
    totals["by_event_key"] = by_event_key
    return totals


def _log_group_done(group: str, summary: dict):
    print(f"[push-automation] {group}:done", {
        "checked": summary["checked"],
        "events_candidates": summary["events_candidates"],
        "events_attempted": summary["events_attempted"],
        "events_blocked": summary["events_blocked"],
        "events_skipped": summary["events_skipped"],
        "events_sent_to_backend": summary["events_sent_to_backend"],
    })


def _record(ctx: dict, event_key: str, field: str, amount: int = 1):
    ctx["events_by_key"][event_key][field] += amount


def _events_count_for_scan(ctx: dict) -> int:
    return sum(
        int(stats.get("events_candidates", 0) or 0)
        for stats in ctx["events_by_key"].values()
    )


def _max_allowed_for_key(ctx: dict, event_key: str, specific_max: int | None = None) -> int:
    config = ctx["config"]
    max_allowed = int(config["max_events_per_key_per_scan"])
    if specific_max is not None:
        max_allowed = min(max_allowed, int(specific_max))
    return max_allowed


def _large_batch_block_reason(
    ctx: dict,
    event_key: str,
    candidates_count: int,
    specific_max: int | None = None,
) -> tuple[str, int] | None:
    config = ctx["config"]
    if config["allow_large_batch"]:
        return None

    key_max = _max_allowed_for_key(ctx, event_key, specific_max)
    if candidates_count > key_max:
        return ("max_events_per_key_per_scan", key_max)

    max_per_scan = int(config["max_events_per_scan"])
    if _events_count_for_scan(ctx) + candidates_count > max_per_scan:
        return ("max_events_per_scan", max_per_scan)

    return None


def _block_large_batch(
    ctx: dict,
    event_key: str,
    candidates_count: int,
    max_allowed: int,
    reason: str,
) -> list[dict]:
    print("[push-automation] safety:block_large_batch", {
        "event_key": event_key,
        "candidates_count": candidates_count,
        "max_allowed": max_allowed,
        "reason": reason,
    })
    _record(ctx, event_key, "events_candidates", candidates_count)
    _record(ctx, event_key, "events_blocked", candidates_count)
    return [
        {"blocked": True, "reason": reason}
        for _ in range(candidates_count)
    ]


def _log_missing_occurred_at(event_key: str, reference_key: str | None, reason: str):
    print("[push-automation] safety:missing_occurred_at", {
        "event_key": event_key,
        "reference_key": reference_key,
        "reason": reason,
    })


def _log_lookback_filter(event_key: str, ignored_count: int, lookback_hours: int, reason: str):
    if ignored_count <= 0:
        return
    print("[push-automation] safety:lookback_filter", {
        "event_key": event_key,
        "ignored_count": ignored_count,
        "lookback_hours": lookback_hours,
        "reason": reason,
    })


def _table_columns(conn, table_name: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = %s
        """, (table_name,))
        return {row["column_name"]: row["data_type"] for row in cur.fetchall() or []}


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _first_existing_column(columns: dict, candidates: tuple[str, ...]) -> str | None:
    for column in candidates:
        if column in columns:
            return column
    return None


def _existing_columns(columns: dict, candidates: tuple[str, ...]) -> list[str]:
    return [column for column in candidates if column in columns]


def _normalize_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_datetime(value) -> str | None:
    parsed = _normalize_datetime(value)
    if parsed is None:
        return None
    return parsed.isoformat()


def _latest_datetime(values: list) -> datetime | None:
    parsed_values = [
        parsed
        for parsed in (_normalize_datetime(value) for value in values)
        if parsed is not None
    ]
    if not parsed_values:
        return None
    return max(parsed_values)


def _lookback_cutoff(hours: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)


def _is_within_lookback(value, hours: int) -> bool:
    parsed = _normalize_datetime(value)
    return parsed is not None and parsed >= _lookback_cutoff(hours)


def _date_key(value) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _get_total_slots_from_config(conn) -> int:
    kv = {}
    app_config_cols = _table_columns(conn, "app_config")
    kv_store_cols = _table_columns(conn, "kv_store")

    with conn.cursor() as cur:
        if {"key", "value"}.issubset(set(app_config_cols)):
            cur.execute("SELECT key, value FROM app_config")
            for row in cur.fetchall() or []:
                kv[str(row["key"] or "").strip().lower()] = row["value"]

        if {"k", "v"}.issubset(set(kv_store_cols)):
            cur.execute("SELECT k, v FROM kv_store")
            for row in cur.fetchall() or []:
                kv[str(row["k"] or "").strip().lower()] = row["v"]

    for key in ("total_numbers", "ticket_count", "ticket_total", "max_number", "range_max"):
        value = kv.get(key)
        if value is None:
            continue
        try:
            parsed = int(str(value))
            if parsed > 0:
                return parsed
        except Exception:
            pass

    return 100


def _max_aliases(prefix: str, columns: list[str]) -> tuple[list[str], list[str]]:
    selects = []
    aliases = []
    for column in columns:
        alias = f"{prefix}_{column}"
        selects.append(f"MAX({prefix}.{_quote_ident(column)}) AS {alias}")
        aliases.append(alias)
    return selects, aliases


def _get_sold_snapshot(conn, draw_id: int) -> dict:
    reservation_cols = _table_columns(conn, "reservations")
    payment_cols = _table_columns(conn, "payments")
    reservation_time_cols = _existing_columns(reservation_cols, RESERVATION_TEMPORAL_COLUMNS)
    payment_time_cols = _existing_columns(payment_cols, PAYMENT_TEMPORAL_COLUMNS)
    reservation_selects, reservation_aliases = _max_aliases("r", reservation_time_cols)
    payment_selects, payment_aliases = _max_aliases("p", payment_time_cols)
    aggregate_selects = reservation_selects + payment_selects
    temporal_select_sql = ""
    if aggregate_selects:
        temporal_select_sql = ",\n                       " + ",\n                       ".join(aggregate_selects)

    with conn.cursor() as cur:
        if "number" in reservation_cols:
            cur.execute(f"""
                SELECT COUNT(DISTINCT r.number) AS sold
                       {temporal_select_sql}
                  FROM reservations r
             LEFT JOIN payments p ON p.id = r.payment_id
                 WHERE r.draw_id = %s
                   AND (r.status = 'paid' OR p.status IN ('approved','paid'))
            """, (draw_id,))
        elif "numbers" in reservation_cols:
            cur.execute(f"""
                SELECT COUNT(DISTINCT flat.num) AS sold
                       {temporal_select_sql}
                  FROM reservations r
             LEFT JOIN payments p ON p.id = r.payment_id
            CROSS JOIN LATERAL UNNEST(r.numbers) AS flat(num)
                 WHERE r.draw_id = %s
                   AND (r.status = 'paid' OR p.status IN ('approved','paid'))
            """, (draw_id,))
        else:
            print("[push-automation] reservations number columns not found")
            return {"sold": 0, "activity_at": None}

        row = cur.fetchone() or {}

    activity_values = [
        row.get(alias)
        for alias in reservation_aliases + payment_aliases
        if alias in row
    ]
    return {
        "sold": int(row.get("sold") or 0),
        "activity_at": _latest_datetime(activity_values),
    }


def _select_remaining_threshold(remaining_numbers: int) -> tuple[int, str] | None:
    for threshold, event_key in REMAINING_THRESHOLDS:
        if remaining_numbers <= threshold:
            return threshold, event_key
    return None


def _notify_event(ctx: dict, candidate: dict):
    try:
        return notify_push_automation_event(
            event_key=candidate["event_key"],
            reference_type=candidate["reference_type"],
            reference_key=candidate["reference_key"],
            metadata=candidate.get("metadata") or {},
            recipient_user_ids=candidate.get("recipient_user_ids"),
            source="engine",
            scan_id=ctx["scan_id"],
            occurred_at=_iso_datetime(candidate.get("occurred_at")),
        )
    except Exception as exc:
        print("[push-automation] event failed:", {
            "event_key": candidate.get("event_key"),
            "reference_key": candidate.get("reference_key"),
            "message": str(exc) or "event_failed",
        })
        return {"ok": False, "reason": "event_failed"}


def _process_candidates(
    ctx: dict,
    candidates: list[dict],
    checked: int,
    group: str,
    specific_max_by_key: dict[str, int] | None = None,
    group_max_events: int | None = None,
) -> dict:
    specific_max_by_key = specific_max_by_key or {}
    results = []
    if (
        group_max_events is not None
        and not ctx["config"]["allow_large_batch"]
        and len(candidates) > group_max_events
    ):
        event_key = "BALANCE_*" if group == "balance_expiration" else (candidates[0]["event_key"] if candidates else group)
        results = _block_large_batch(
            ctx,
            event_key,
            len(candidates),
            group_max_events,
            f"{group}_max_events_per_scan",
        )
        summary = _summary_from_results(checked, results)
        _log_group_done(group, summary)
        return summary

    grouped = defaultdict(list)
    for candidate in candidates:
        grouped[candidate["event_key"]].append(candidate)

    for event_key, event_candidates in grouped.items():
        block = _large_batch_block_reason(
            ctx,
            event_key,
            len(event_candidates),
            specific_max_by_key.get(event_key),
        )
        if block:
            reason, max_allowed = block
            results.extend(_block_large_batch(
                ctx,
                event_key,
                len(event_candidates),
                max_allowed,
                reason,
            ))
            continue

        _record(ctx, event_key, "events_candidates", len(event_candidates))
        for candidate in event_candidates:
            reference_key = candidate["reference_key"]
            occurred_at = candidate.get("occurred_at")

            if not _event_key_allowed(event_key):
                print("[push-automation] skipped: event key not allowed", {
                    "event_key": event_key,
                    "reference_key": reference_key,
                })
                _record(ctx, event_key, "events_skipped")
                results.append({"skipped": True, "reason": "event_key_not_allowed"})
                continue

            if ctx["config"]["require_occurred_at"] and _normalize_datetime(occurred_at) is None:
                _log_missing_occurred_at(event_key, reference_key, "require_occurred_at")
                _record(ctx, event_key, "events_skipped")
                results.append({"skipped": True, "reason": "missing_occurred_at"})
                continue

            if ctx["config"]["preview_only"]:
                print("[push-automation] preview:event_candidate", {
                    "event_key": event_key,
                    "reference_type": candidate["reference_type"],
                    "reference_key": reference_key,
                    "occurred_at": _iso_datetime(occurred_at),
                    "scan_id": ctx["scan_id"],
                })
                results.append({"preview": True})
                continue

            _record(ctx, event_key, "events_attempted")
            response = _notify_event(ctx, candidate)
            attempted = True
            skipped = bool(response.get("skipped")) if isinstance(response, dict) else False
            sent_to_backend = (
                isinstance(response, dict)
                and response.get("ok") is True
                and not skipped
                and not response.get("blocked")
            )
            if skipped:
                _record(ctx, event_key, "events_skipped")
            if sent_to_backend:
                _record(ctx, event_key, "events_sent_to_backend")
            results.append({
                "attempted": attempted,
                "skipped": skipped,
                "sent_to_backend": sent_to_backend,
            })

    summary = _summary_from_results(checked, results)
    _log_group_done(group, summary)
    return summary


def emit_remaining_numbers_events(conn, ctx: dict):
    draws_cols = _table_columns(conn, "draws")
    if "id" not in draws_cols or "status" not in draws_cols:
        print("[push-automation] draws id/status columns not found")
        summary = _empty_summary()
        _log_group_done("remaining_numbers", summary)
        return summary

    draw_time_cols = _existing_columns(draws_cols, DRAW_TEMPORAL_COLUMNS)
    selected_cols = ["id"] + draw_time_cols
    order_col = _first_existing_column(draws_cols, DRAW_TEMPORAL_COLUMNS)
    order_sql = f"{_quote_ident(order_col)} DESC NULLS LAST, id DESC" if order_col else "id DESC"

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT {', '.join(_quote_ident(column) for column in selected_cols)}
              FROM draws
             WHERE status IN ('open', 'active', 'aberto')
               AND COALESCE(draw_type, 'principal') = 'principal'
             ORDER BY {order_sql}
             LIMIT 1
        """)
        draws = cur.fetchall() or []

    if not draws:
        summary = _empty_summary()
        _log_group_done("remaining_numbers", summary)
        return summary

    total_numbers = _get_total_slots_from_config(conn)
    candidates = []
    ignored_by_lookback = 0
    missing_occurred_at = 0

    for draw in draws:
        draw_id = int(draw["id"])
        sold_snapshot = _get_sold_snapshot(conn, draw_id)
        sold_numbers = int(sold_snapshot["sold"])
        remaining_numbers = max(total_numbers - sold_numbers, 0)
        selected_threshold = _select_remaining_threshold(remaining_numbers)
        if selected_threshold is None:
            continue

        occurred_at = _latest_datetime(
            [draw.get(column) for column in draw_time_cols]
            + [sold_snapshot.get("activity_at")]
        )
        if ctx["config"]["require_occurred_at"] and occurred_at is None:
            missing_occurred_at += 1
            _log_missing_occurred_at(
                "DRAW_REMAINING_NUMBERS",
                f"draw:{draw_id}:remaining",
                "draw_or_sale_temporal_column_not_found",
            )
            continue

        lookback_hours = ctx["config"]["remaining_lookback_hours"]
        if ctx["config"]["no_backfill"] and not _is_within_lookback(occurred_at, lookback_hours):
            ignored_by_lookback += 1
            continue

        threshold, event_key = selected_threshold
        candidates.append({
            "event_key": event_key,
            "reference_type": "draw",
            "reference_key": f"draw:{draw_id}:remaining:{threshold}",
            "occurred_at": occurred_at,
            "metadata": {
                "draw_id": draw_id,
                "threshold": threshold,
                "remaining_numbers": remaining_numbers,
                "total_numbers": total_numbers,
            },
        })

    _log_lookback_filter(
        "DRAW_REMAINING_NUMBERS",
        ignored_by_lookback,
        ctx["config"]["remaining_lookback_hours"],
        "draw_or_sale_activity_outside_lookback",
    )
    if missing_occurred_at:
        _record(ctx, "DRAW_REMAINING_NUMBERS", "events_skipped", missing_occurred_at)

    return _process_candidates(
        ctx,
        candidates,
        len(draws),
        "remaining_numbers",
        {
            "DRAW_REMAINING_NUMBERS_75": ctx["config"]["remaining_max_events_per_scan"],
            "DRAW_REMAINING_NUMBERS_50": ctx["config"]["remaining_max_events_per_scan"],
            "DRAW_REMAINING_NUMBERS_20": ctx["config"]["remaining_max_events_per_scan"],
            "DRAW_REMAINING_NUMBERS_10": ctx["config"]["remaining_max_events_per_scan"],
        },
    )


def emit_winner_defined_events(conn, ctx: dict):
    draws_cols = _table_columns(conn, "draws")
    if "id" not in draws_cols or "status" not in draws_cols:
        print("[push-automation] draws id/status columns not found")
        summary = _empty_summary()
        _log_group_done("winner_defined", summary)
        return summary

    temporal_col = _first_existing_column(draws_cols, WINNER_TEMPORAL_COLUMNS)
    if temporal_col is None and (ctx["config"]["no_backfill"] or ctx["config"]["require_occurred_at"]):
        _log_missing_occurred_at(
            "WINNER_DEFINED",
            None,
            "winner_temporal_column_not_found",
        )
        summary = _empty_summary()
        _log_group_done("winner_defined", summary)
        return summary

    selected_cols = ["id"]
    if "winner_number" in draws_cols:
        selected_cols.append("winner_number")
    if temporal_col:
        selected_cols.append(temporal_col)

    defined_conditions = []
    if "winner_number" in draws_cols:
        defined_conditions.append("winner_number IS NOT NULL")
    if "winner_user_id" in draws_cols:
        defined_conditions.append("winner_user_id IS NOT NULL")
    if "realized_at" in draws_cols:
        defined_conditions.append("realized_at IS NOT NULL")

    if not defined_conditions:
        print("[push-automation] winner definition columns not found")
        summary = _empty_summary()
        _log_group_done("winner_defined", summary)
        return summary

    base_where = f"status = 'sorteado' AND ({' OR '.join(defined_conditions)})"
    params = []
    if ctx["config"]["no_backfill"] and temporal_col:
        base_where += f" AND {_quote_ident(temporal_col)} >= NOW() - (%s * INTERVAL '1 hour')"
        params.append(ctx["config"]["winner_lookback_hours"])

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*) AS candidates_count
              FROM draws
             WHERE {base_where}
        """, tuple(params))
        row = cur.fetchone() or {}
        candidates_count = int(row.get("candidates_count") or 0)

    if ctx["config"]["no_backfill"] and temporal_col:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) AS ignored_count
                  FROM draws
                 WHERE status = 'sorteado'
                   AND ({' OR '.join(defined_conditions)})
                   AND ({_quote_ident(temporal_col)} IS NULL
                        OR {_quote_ident(temporal_col)} < NOW() - (%s * INTERVAL '1 hour'))
            """, (ctx["config"]["winner_lookback_hours"],))
            row = cur.fetchone() or {}
            _log_lookback_filter(
                "WINNER_DEFINED",
                int(row.get("ignored_count") or 0),
                ctx["config"]["winner_lookback_hours"],
                "winner_defined_outside_lookback",
            )

    block = _large_batch_block_reason(
        ctx,
        "WINNER_DEFINED",
        candidates_count,
        ctx["config"]["winner_max_events_per_scan"],
    )
    if block:
        reason, max_allowed = block
        results = _block_large_batch(
            ctx,
            "WINNER_DEFINED",
            candidates_count,
            max_allowed,
            reason,
        )
        summary = _summary_from_results(candidates_count, results)
        _log_group_done("winner_defined", summary)
        return summary

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT {', '.join(_quote_ident(column) for column in selected_cols)}
              FROM draws
             WHERE {base_where}
             ORDER BY {_quote_ident(temporal_col) if temporal_col else 'id'} DESC NULLS LAST, id DESC
        """, tuple(params))
        draws = cur.fetchall() or []

    candidates = []
    for draw in draws:
        draw_id = int(draw["id"])
        metadata = {"draw_id": draw_id}
        winner_number = draw.get("winner_number") if "winner_number" in selected_cols else None
        if winner_number is not None:
            metadata["winner_number"] = int(winner_number)

        candidates.append({
            "event_key": "WINNER_DEFINED",
            "reference_type": "draw",
            "reference_key": f"draw:{draw_id}:winner_defined",
            "occurred_at": draw.get(temporal_col) if temporal_col else None,
            "metadata": metadata,
        })

    return _process_candidates(
        ctx,
        candidates,
        candidates_count,
        "winner_defined",
        {"WINNER_DEFINED": ctx["config"]["winner_max_events_per_scan"]},
    )


def emit_balance_expiration_events(conn, ctx: dict):
    user_cols = _table_columns(conn, "users")
    required = {"id", "coupon_value_cents", "coupon_updated_at"}
    if not required.issubset(set(user_cols)):
        print("[push-automation] balance:schema", {
            "ok": False,
            "has_user_id": "id" in user_cols,
            "has_coupon_value_cents": "coupon_value_cents" in user_cols,
            "has_coupon_updated_at": "coupon_updated_at" in user_cols,
        })
        print("[push-automation] users balance expiration columns not found")
        summary = _empty_summary()
        _log_group_done("balance_expiration", summary)
        return summary

    candidates = []
    now = datetime.now(timezone.utc)
    lookback_hours = ctx["config"]["balance_lookback_hours"]
    valid_days = _coupon_valid_days()
    print("[push-automation] balance:schema", {
        "ok": True,
        "source_table": "users",
        "value_column": "coupon_value_cents",
        "updated_at_column": "coupon_updated_at",
        "valid_days": valid_days,
    })

    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS users_checked
              FROM users
             WHERE coupon_value_cents > 0
               AND coupon_updated_at IS NOT NULL
        """)
        row = cur.fetchone() or {}
        users_checked = int(row.get("users_checked") or 0)

        cur.execute("""
            SELECT
                id,
                coupon_value_cents,
                coupon_updated_at,
                coupon_updated_at + (%s * INTERVAL '1 day') AS coupon_expires_at,
                ((coupon_updated_at + (%s * INTERVAL '1 day'))::date - CURRENT_DATE)
                    AS days_until_expiration
              FROM users
             WHERE coupon_value_cents > 0
               AND coupon_updated_at IS NOT NULL
               AND ((coupon_updated_at + (%s * INTERVAL '1 day'))::date - CURRENT_DATE)
                   = ANY(%s::int[])
             ORDER BY coupon_expires_at ASC, id ASC
        """, (
            valid_days,
            valid_days,
            valid_days,
            list(BALANCE_EXPIRING_EVENTS.keys()),
        ))
        expiring_users = cur.fetchall() or []

        expired_where = """
            coupon_value_cents > 0
            AND coupon_updated_at IS NOT NULL
            AND coupon_updated_at + (%s * INTERVAL '1 day') <= NOW()
        """
        expired_params = [valid_days]
        if ctx["config"]["no_backfill"]:
            expired_where += """
                AND coupon_updated_at + (%s * INTERVAL '1 day')
                    >= NOW() - (%s * INTERVAL '1 hour')
            """
            expired_params.append(valid_days)
            expired_params.append(lookback_hours)

        cur.execute(f"""
            SELECT
                id,
                coupon_value_cents,
                coupon_updated_at,
                coupon_updated_at + (%s * INTERVAL '1 day') AS coupon_expires_at,
                ((coupon_updated_at + (%s * INTERVAL '1 day'))::date - CURRENT_DATE)
                    AS days_until_expiration
              FROM users
             WHERE {expired_where}
             ORDER BY coupon_expires_at DESC, id ASC
        """, tuple([valid_days, valid_days] + expired_params))
        expired_users = cur.fetchall() or []

    checked = users_checked

    for user in expiring_users:
        user_id = int(user["id"])
        days_until = int(user["days_until_expiration"])
        event_key = BALANCE_EXPIRING_EVENTS.get(days_until)
        if not event_key:
            continue

        coupon_value_cents = int(user["coupon_value_cents"] or 0)
        expires_at = _date_key(user["coupon_expires_at"])
        coupon_expires_at = _iso_datetime(user["coupon_expires_at"])
        print("[push-automation] balance:event_candidate", {
            "event_key": event_key,
            "reference_type": "user_balance",
            "reference_key": f"user:{user_id}:balance_expiring:{days_until}:{expires_at}",
            "days_until_expiration": days_until,
            "valid_days": valid_days,
        })
        candidates.append({
            "event_key": event_key,
            "reference_type": "user_balance",
            "reference_key": f"user:{user_id}:balance_expiring:{days_until}:{expires_at}",
            "recipient_user_ids": [user_id],
            "occurred_at": now,
            "metadata": {
                "user_id": user_id,
                "balance_cents": coupon_value_cents,
                "coupon_value_cents": coupon_value_cents,
                "coupon_expires_at": coupon_expires_at,
                "days_until_expiration": days_until,
                "valid_days": valid_days,
            },
        })

    for user in expired_users:
        user_id = int(user["id"])
        coupon_value_cents = int(user["coupon_value_cents"] or 0)
        expires_at = _date_key(user["coupon_expires_at"])
        coupon_expires_at = _iso_datetime(user["coupon_expires_at"])
        days_until = int(user["days_until_expiration"])
        print("[push-automation] balance:event_candidate", {
            "event_key": "BALANCE_EXPIRED",
            "reference_type": "user_balance",
            "reference_key": f"user:{user_id}:balance_expired:{expires_at}",
            "days_until_expiration": days_until,
            "valid_days": valid_days,
        })
        candidates.append({
            "event_key": "BALANCE_EXPIRED",
            "reference_type": "user_balance",
            "reference_key": f"user:{user_id}:balance_expired:{expires_at}",
            "recipient_user_ids": [user_id],
            "occurred_at": user["coupon_expires_at"],
            "metadata": {
                "user_id": user_id,
                "balance_cents": coupon_value_cents,
                "coupon_value_cents": coupon_value_cents,
                "coupon_expires_at": coupon_expires_at,
                "days_until_expiration": days_until,
                "valid_days": valid_days,
            },
        })

    if ctx["config"]["no_backfill"]:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS ignored_count
                  FROM users
                 WHERE coupon_value_cents > 0
                   AND coupon_updated_at IS NOT NULL
                   AND coupon_updated_at + (%s * INTERVAL '1 day') <= NOW()
                   AND coupon_updated_at + (%s * INTERVAL '1 day')
                       < NOW() - (%s * INTERVAL '1 hour')
            """, (valid_days, valid_days, lookback_hours))
            row = cur.fetchone() or {}
            _log_lookback_filter(
                "BALANCE_EXPIRED",
                int(row.get("ignored_count") or 0),
                lookback_hours,
                "balance_expired_outside_lookback",
            )

    print("[push-automation] balance:candidates", {
        "valid_days": valid_days,
        "users_checked": checked,
        "candidates_count": len(candidates),
        "expiring_candidates": len(expiring_users),
        "expired_candidates": len(expired_users),
    })

    summary = _process_candidates(
        ctx,
        candidates,
        checked,
        "balance_expiration",
        {event_key: ctx["config"]["balance_max_events_per_scan"] for event_key in (
            "BALANCE_EXPIRING_30_DAYS",
            "BALANCE_EXPIRING_15_DAYS",
            "BALANCE_EXPIRING_10_DAYS",
            "BALANCE_EXPIRING_7_DAYS",
            "BALANCE_EXPIRED",
        )},
        group_max_events=ctx["config"]["balance_max_events_per_scan"],
    )
    print("[push-automation] balance:done", {
        "valid_days": valid_days,
        "users_checked": checked,
        "candidates_count": summary["events_candidates"],
        "events_attempted": summary["events_attempted"],
        "events_blocked": summary["events_blocked"],
        "events_skipped": summary["events_skipped"],
    })
    return summary
