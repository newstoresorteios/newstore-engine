from datetime import date, datetime

from push_automation_events import notify_push_automation_event


REMAINING_THRESHOLDS = (
    (20, "DRAW_REMAINING_NUMBERS_20"),
    (10, "DRAW_REMAINING_NUMBERS_10"),
)
BALANCE_EXPIRING_EVENTS = {
    30: "BALANCE_EXPIRING_30_DAYS",
    10: "BALANCE_EXPIRING_10_DAYS",
    7: "BALANCE_EXPIRING_7_DAYS",
}


def run_push_automation_scan(conn):
    emit_remaining_numbers_events(conn)
    emit_winner_defined_events(conn)
    emit_balance_expiration_events(conn)


def _table_columns(conn, table_name: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = %s
        """, (table_name,))
        return {row["column_name"]: row["data_type"] for row in cur.fetchall() or []}


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


def _get_sold_count(conn, draw_id: int) -> int:
    cols = _table_columns(conn, "reservations")

    with conn.cursor() as cur:
        if "number" in cols:
            cur.execute("""
                SELECT COUNT(DISTINCT r.number) AS sold
                  FROM reservations r
             LEFT JOIN payments p ON p.id = r.payment_id
                 WHERE r.draw_id = %s
                   AND (r.status = 'paid' OR p.status IN ('approved','paid'))
            """, (draw_id,))
        elif "numbers" in cols:
            cur.execute("""
                WITH flat AS (
                    SELECT UNNEST(r.numbers) AS num
                      FROM reservations r
                 LEFT JOIN payments p ON p.id = r.payment_id
                     WHERE r.draw_id = %s
                       AND (r.status = 'paid' OR p.status IN ('approved','paid'))
                )
                SELECT COUNT(DISTINCT num) AS sold FROM flat
            """, (draw_id,))
        else:
            print("[push-automation] reservations number columns not found")
            return 0

        row = cur.fetchone()
        return int(row["sold"] or 0)


def _date_key(value) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _notify_event(**kwargs):
    try:
        return notify_push_automation_event(**kwargs)
    except Exception as exc:
        print("[push-automation] event failed:", {
            "event_key": kwargs.get("event_key"),
            "reference_key": kwargs.get("reference_key"),
            "message": str(exc) or "event_failed",
        })
        return {"ok": False, "reason": "event_failed"}


def emit_remaining_numbers_events(conn):
    draws_cols = _table_columns(conn, "draws")
    if "id" not in draws_cols or "status" not in draws_cols:
        print("[push-automation] draws id/status columns not found")
        return []

    total_numbers = _get_total_slots_from_config(conn)
    emitted = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id
              FROM draws
             WHERE status = 'open'
             ORDER BY id ASC
        """)
        draws = cur.fetchall() or []

    for draw in draws:
        draw_id = int(draw["id"])
        sold_numbers = _get_sold_count(conn, draw_id)
        remaining_numbers = max(total_numbers - sold_numbers, 0)

        for threshold, event_key in REMAINING_THRESHOLDS:
            if remaining_numbers > threshold:
                continue

            reference_key = f"draw:{draw_id}:remaining:{threshold}"
            emitted.append(_notify_event(
                event_key=event_key,
                reference_type="draw",
                reference_key=reference_key,
                metadata={
                    "draw_id": draw_id,
                    "threshold": threshold,
                    "remaining_numbers": remaining_numbers,
                    "total_numbers": total_numbers,
                },
                source="engine",
            ))

    return emitted


def emit_winner_defined_events(conn):
    draws_cols = _table_columns(conn, "draws")
    if "id" not in draws_cols or "status" not in draws_cols:
        print("[push-automation] draws id/status columns not found")
        return []

    selected_cols = ["id"]
    if "winner_number" in draws_cols:
        selected_cols.append("winner_number")

    defined_conditions = []
    if "winner_number" in draws_cols:
        defined_conditions.append("winner_number IS NOT NULL")
    if "winner_user_id" in draws_cols:
        defined_conditions.append("winner_user_id IS NOT NULL")
    if "realized_at" in draws_cols:
        defined_conditions.append("realized_at IS NOT NULL")

    if not defined_conditions:
        print("[push-automation] winner definition columns not found")
        return []

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT {', '.join(selected_cols)}
              FROM draws
             WHERE status = 'sorteado'
               AND ({' OR '.join(defined_conditions)})
             ORDER BY id ASC
        """)
        draws = cur.fetchall() or []

    emitted = []
    for draw in draws:
        draw_id = int(draw["id"])
        metadata = {"draw_id": draw_id}
        winner_number = draw.get("winner_number") if "winner_number" in selected_cols else None
        if winner_number is not None:
            metadata["winner_number"] = int(winner_number)

        emitted.append(_notify_event(
            event_key="WINNER_DEFINED",
            reference_type="draw",
            reference_key=f"draw:{draw_id}:winner_defined",
            metadata=metadata,
            source="engine",
        ))

    return emitted


def emit_balance_expiration_events(conn):
    user_cols = _table_columns(conn, "users")
    required = {"id", "balance_cents", "coupon_expires_at"}
    if not required.issubset(set(user_cols)):
        print("[push-automation] users balance expiration columns not found")
        return []

    emitted = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                id,
                coupon_expires_at,
                (coupon_expires_at::date - CURRENT_DATE) AS days_until
              FROM users
             WHERE balance_cents > 0
               AND coupon_expires_at IS NOT NULL
               AND (coupon_expires_at::date - CURRENT_DATE) = ANY(%s::int[])
             ORDER BY id ASC
        """, (list(BALANCE_EXPIRING_EVENTS.keys()),))
        expiring_users = cur.fetchall() or []

        cur.execute("""
            SELECT id, coupon_expires_at
              FROM users
             WHERE balance_cents > 0
               AND coupon_expires_at IS NOT NULL
               AND coupon_expires_at::date < CURRENT_DATE
             ORDER BY id ASC
        """)
        expired_users = cur.fetchall() or []

    for user in expiring_users:
        user_id = int(user["id"])
        days_until = int(user["days_until"])
        event_key = BALANCE_EXPIRING_EVENTS.get(days_until)
        if not event_key:
            continue

        expires_at = _date_key(user["coupon_expires_at"])
        emitted.append(_notify_event(
            event_key=event_key,
            reference_type="balance",
            reference_key=f"balance:{user_id}:expiring:{expires_at}:{days_until}",
            recipient_user_ids=[user_id],
            metadata={
                "user_id": user_id,
                "days_until_expiry": days_until,
                "expires_at": expires_at,
            },
            source="engine",
        ))

    for user in expired_users:
        user_id = int(user["id"])
        expires_at = _date_key(user["coupon_expires_at"])
        emitted.append(_notify_event(
            event_key="BALANCE_EXPIRED",
            reference_type="balance",
            reference_key=f"balance:{user_id}:expired:{expires_at}",
            recipient_user_ids=[user_id],
            metadata={
                "user_id": user_id,
                "expires_at": expires_at,
            },
            source="engine",
        ))

    return emitted
