import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import requests

from email_automation_events import (
    DEFAULT_BACKEND_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_BACKEND_READ_TIMEOUT_SECONDS,
    _positive_int_env,
    notify_email_automation_event,
)
from email_automation_scan import (
    EMAIL_BALANCE_EXPIRING_EVENTS,
    EMAIL_CLOSED_LOOKBACK_HOURS,
    EMAIL_DEFAULT_LOOKBACK_HOURS,
    EMAIL_PUBLISHED_LOOKBACK_HOURS,
    EMAIL_REMAINING_THRESHOLDS,
    _remaining_threshold,
    run_email_automation_scan,
)


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        self.conn.executions.append((" ".join(sql.split()), params))
        if "opened_at IS NOT NULL" in sql:
            self.rows = self.conn.published_draws
        elif ".status = 'open'" in sql or "status = 'open'" in sql:
            self.rows = self.conn.open_draws
        elif "closed_at IS NOT NULL" in sql:
            self.rows = self.conn.closed_draws
        elif "FROM public.numbers" in sql:
            draw_id = int(params[0])
            self.rows = [self.conn.snapshots[draw_id]]
        elif "FROM public.user_coupon_balance_expiry" in sql:
            self.rows = self.conn.balance_rows
        else:
            self.rows = []

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class FakeConnection:
    def __init__(
        self,
        published_draws=None,
        open_draws=None,
        snapshots=None,
        closed_draws=None,
        balance_rows=None,
    ):
        self.published_draws = published_draws or []
        self.open_draws = open_draws or []
        self.snapshots = snapshots or {}
        self.closed_draws = closed_draws or []
        self.balance_rows = balance_rows or []
        self.executions = []

    def cursor(self):
        return FakeCursor(self)


class EmailAutomationScanTest(unittest.TestCase):
    def _capture_remaining_events(self, draws, remaining_numbers=15):
        conn = FakeConnection(
            open_draws=draws,
            snapshots={
                int(draw["id"]): {
                    "total_numbers": 100,
                    "remaining_numbers": remaining_numbers,
                    "sold_numbers": 100 - remaining_numbers,
                }
                for draw in draws
            },
        )
        with patch(
            "email_automation_scan.notify_email_automation_event",
            return_value={"ok": True},
        ) as notify, patch("builtins.print") as log:
            run_email_automation_scan(conn)
        return conn, notify, log

    def test_principal_draw_uses_configured_name(self):
        _conn, notify, log = self._capture_remaining_events([{
            "id": 133,
            "draw_type": "principal",
            "product_name": "Sorteio Relógio Rolex Submariner",
        }])

        event = notify.call_args.kwargs
        self.assertEqual(
            event["metadata"]["draw_name"],
            "Sorteio Relógio Rolex Submariner",
        )
        log.assert_called_once_with("[email-automation] event_detected", {
            "draw_id": 133,
            "draw_type": "principal",
            "draw_name": "Sorteio Relógio Rolex Submariner",
            "event_key": "EMAIL_DRAW_REMAINING_15",
            "reference_key": "draw:133:email_remaining:15",
        })

    def test_additional_draw_uses_configured_name(self):
        _conn, notify, _log = self._capture_remaining_events([{
            "id": 145,
            "draw_type": "adicional",
            "product_name": "Sorteio adicional de créditos",
        }])

        event = notify.call_args.kwargs
        self.assertEqual(
            event["metadata"]["draw_name"],
            "Sorteio adicional de créditos",
        )
        self.assertEqual(
            event["reference_key"],
            "additional_draw:145:email_remaining:15",
        )

    def test_secondary_draw_uses_configured_name(self):
        _conn, notify, _log = self._capture_remaining_events([{
            "id": 146,
            "draw_type": "secundario",
            "product_name": "Sorteio adicional — Vale-compras R$ 5.000",
        }])

        self.assertEqual(
            notify.call_args.kwargs["metadata"]["draw_name"],
            "Sorteio adicional — Vale-compras R$ 5.000",
        )

    def test_empty_name_uses_principal_fallback(self):
        _conn, notify, _log = self._capture_remaining_events([{
            "id": 147,
            "draw_type": "principal",
            "product_name": "   ",
        }])

        self.assertEqual(
            notify.call_args.kwargs["metadata"]["draw_name"],
            "Sorteio principal",
        )

    def test_null_name_uses_type_specific_fallback(self):
        draws = [
            {"id": 148, "draw_type": "adicional", "product_name": None},
            {"id": 149, "draw_type": "secundario", "product_name": None},
        ]
        _conn, notify, _log = self._capture_remaining_events(draws)

        self.assertEqual(
            [call.kwargs["metadata"]["draw_name"] for call in notify.call_args_list],
            ["Sorteio adicional #148", "Sorteio secundário #149"],
        )

    def test_accented_name_is_preserved(self):
        accented_name = "Sorteio edição verão — créditos à vista"
        _conn, notify, _log = self._capture_remaining_events([{
            "id": 150,
            "draw_type": "adicional",
            "product_name": accented_name,
        }])

        self.assertEqual(
            notify.call_args.kwargs["metadata"]["draw_name"],
            accented_name,
        )

    def test_changed_name_does_not_change_reference_key(self):
        draw = {
            "id": 151,
            "draw_type": "adicional",
            "product_name": "Nome original",
        }
        _conn, first_notify, _log = self._capture_remaining_events([draw])
        draw["product_name"] = "Nome atualizado"
        _conn, second_notify, _log = self._capture_remaining_events([draw])

        self.assertNotEqual(
            first_notify.call_args.kwargs["metadata"]["draw_name"],
            second_notify.call_args.kwargs["metadata"]["draw_name"],
        )
        self.assertEqual(
            first_notify.call_args.kwargs["reference_key"],
            second_notify.call_args.kwargs["reference_key"],
        )
        self.assertEqual(
            second_notify.call_args.kwargs["reference_key"],
            "additional_draw:151:email_remaining:15",
        )

    def test_equal_names_remain_distinct_by_draw_id(self):
        same_name = "Sorteio adicional de créditos"
        draws = [
            {"id": 152, "draw_type": "adicional", "product_name": same_name},
            {"id": 153, "draw_type": "adicional", "product_name": same_name},
        ]
        _conn, notify, _log = self._capture_remaining_events(draws)

        self.assertEqual(
            [call.kwargs["reference_key"] for call in notify.call_args_list],
            [
                "additional_draw:152:email_remaining:15",
                "additional_draw:153:email_remaining:15",
            ],
        )

    def test_published_principal_and_additional_use_backend_dedupe_keys(self):
        opened_at = datetime.now(timezone.utc)
        conn = FakeConnection(published_draws=[
            {
                "id": 133,
                "status": "open",
                "draw_type": "principal",
                "product_name": "Principal",
                "opened_at": opened_at,
            },
            {
                "id": 150,
                "status": "open",
                "draw_type": "adicional",
                "product_name": "Adicional",
                "opened_at": opened_at,
            },
        ])
        with patch(
            "email_automation_scan.notify_email_automation_event",
            return_value={"ok": True, "data": {"sent": 1}},
        ) as notify:
            summary = run_email_automation_scan(conn)

        self.assertEqual(
            [
                (
                    item.kwargs["event_key"],
                    item.kwargs["reference_type"],
                    item.kwargs["reference_key"],
                )
                for item in notify.call_args_list
            ],
            [
                ("NEW_DRAW_PUBLISHED", "draw", "draw:133:published_email"),
                (
                    "NEW_DRAW_PUBLISHED",
                    "additional_draw",
                    "additional_draw:150:published_email",
                ),
            ],
        )
        self.assertEqual(summary["published_checked"], 2)
        self.assertEqual(summary["sent"], 2)

    def test_published_reexecution_keeps_same_reference_key_for_backend_dedupe(self):
        opened_at = datetime.now(timezone.utc)
        conn = FakeConnection(published_draws=[{
            "id": 133,
            "status": "open",
            "draw_type": "principal",
            "product_name": "Principal",
            "opened_at": opened_at,
        }])
        with patch(
            "email_automation_scan.notify_email_automation_event",
            side_effect=[
                {"ok": True, "data": {"sent": 1, "failed": 0, "deduped": 0}},
                {"ok": True, "data": {"sent": 0, "failed": 0, "deduped": 1}},
            ],
        ) as notify:
            first = run_email_automation_scan(conn)
            second = run_email_automation_scan(conn)

        self.assertEqual(first["sent"], 1)
        self.assertEqual(second["sent"], 0)
        self.assertEqual(second["skipped"], 1)
        self.assertEqual(
            [item.kwargs["reference_key"] for item in notify.call_args_list],
            ["draw:133:published_email", "draw:133:published_email"],
        )

    def test_email_thresholds_are_independent_from_push_thresholds(self):
        self.assertEqual(EMAIL_REMAINING_THRESHOLDS, (
            (15, "EMAIL_DRAW_REMAINING_15"),
            (30, "EMAIL_DRAW_REMAINING_30"),
            (50, "EMAIL_DRAW_REMAINING_50"),
            (75, "EMAIL_DRAW_REMAINING_75"),
        ))
        self.assertEqual(_remaining_threshold(72), (75, "EMAIL_DRAW_REMAINING_75"))
        self.assertEqual(_remaining_threshold(48), (50, "EMAIL_DRAW_REMAINING_50"))
        self.assertEqual(_remaining_threshold(28), (30, "EMAIL_DRAW_REMAINING_30"))
        self.assertEqual(_remaining_threshold(12), (15, "EMAIL_DRAW_REMAINING_15"))

    def test_all_thresholds_apply_to_principal_and_additional_draws(self):
        thresholds = (
            (75, "EMAIL_DRAW_REMAINING_75"),
            (50, "EMAIL_DRAW_REMAINING_50"),
            (30, "EMAIL_DRAW_REMAINING_30"),
            (15, "EMAIL_DRAW_REMAINING_15"),
        )
        open_draws = []
        snapshots = {}
        expected = []
        draw_id = 200
        for draw_type, group in (
            ("principal", "draw"),
            ("adicional", "additional_draw"),
        ):
            for remaining, event_key in thresholds:
                open_draws.append({
                    "id": draw_id,
                    "draw_type": draw_type,
                    "product_name": f"{draw_type} {draw_id}",
                })
                snapshots[draw_id] = {
                    "total_numbers": 100,
                    "remaining_numbers": remaining,
                    "sold_numbers": 100 - remaining,
                }
                expected.append((
                    event_key,
                    f"{group}:{draw_id}:email_remaining:{remaining}",
                ))
                draw_id += 1

        conn = FakeConnection(open_draws=open_draws, snapshots=snapshots)
        with patch(
            "email_automation_scan.notify_email_automation_event",
            return_value={"ok": True, "data": {"sent": 1}},
        ) as notify:
            run_email_automation_scan(conn)

        self.assertEqual(
            [
                (item.kwargs["event_key"], item.kwargs["reference_key"])
                for item in notify.call_args_list
            ],
            expected,
        )

    def test_multiple_draws_emit_isolated_reference_keys(self):
        conn = FakeConnection(
            open_draws=[
                {"id": 133, "draw_type": "principal", "product_name": "Principal"},
                {"id": 150, "draw_type": "adicional", "product_name": "Adicional 150"},
                {"id": 151, "draw_type": "secundario", "product_name": "Adicional 151"},
            ],
            snapshots={
                133: {"total_numbers": 100, "remaining_numbers": 75, "sold_numbers": 25},
                150: {"total_numbers": 100, "remaining_numbers": 50, "sold_numbers": 50},
                151: {"total_numbers": 100, "remaining_numbers": 30, "sold_numbers": 70},
            },
        )
        with patch("email_automation_scan.notify_email_automation_event", return_value={"ok": True}) as notify:
            summary = run_email_automation_scan(conn)
        keys = [call.kwargs["reference_key"] for call in notify.call_args_list]
        self.assertEqual(keys, [
            "draw:133:email_remaining:75",
            "additional_draw:150:email_remaining:50",
            "additional_draw:151:email_remaining:30",
        ])
        self.assertEqual(summary["events"], 3)

    def test_closed_draws_use_draw_specific_reference(self):
        conn = FakeConnection(closed_draws=[
            {"id": 133, "draw_type": "principal", "product_name": "Principal", "closed_at": datetime.now(timezone.utc)},
            {"id": 150, "draw_type": "adicional", "product_name": "Adicional", "closed_at": datetime.now(timezone.utc)},
        ])
        with patch("email_automation_scan.notify_email_automation_event", return_value={"ok": True}) as notify:
            run_email_automation_scan(conn)
        keys = [call.kwargs["reference_key"] for call in notify.call_args_list]
        self.assertEqual(keys, ["draw:133:closed_email", "additional_draw:150:closed_email"])

    def test_closed_status_with_closed_at_is_selected(self):
        conn = FakeConnection(closed_draws=[{
            "id": 133,
            "status": "closed",
            "draw_type": "principal",
            "product_name": "Principal",
            "closed_at": datetime.now(timezone.utc),
        }])
        with patch(
            "email_automation_scan.notify_email_automation_event",
            return_value={"ok": True, "data": {"ok": True, "sent": 1, "failed": 0}},
        ) as notify:
            summary = run_email_automation_scan(conn)
        self.assertEqual(notify.call_args.kwargs["event_key"], "DRAW_CLOSED")
        self.assertEqual(summary["closed_checked"], 1)
        self.assertEqual(summary["sent"], 1)

    def test_sorteado_status_with_closed_at_is_still_selected(self):
        conn = FakeConnection(closed_draws=[{
            "id": 133,
            "status": "sorteado",
            "draw_type": "principal",
            "product_name": "Principal",
            "closed_at": datetime.now(timezone.utc),
        }])
        with patch(
            "email_automation_scan.notify_email_automation_event",
            return_value={"ok": True, "data": {"ok": True, "sent": 1, "failed": 0}},
        ) as notify:
            summary = run_email_automation_scan(conn)
        self.assertEqual(notify.call_args.kwargs["reference_key"], "draw:133:closed_email")
        self.assertEqual(summary["sent"], 1)
        closed_sql = next(sql for sql, _params in conn.executions if "closed_at IS NOT NULL" in sql)
        self.assertIn("closed_at IS NOT NULL", closed_sql)
        self.assertNotIn("status = 'closed'", closed_sql)
        self.assertIn("status IN ('closed', 'sorteado')", closed_sql)

    def test_draw_without_closed_at_is_not_selected_as_closed(self):
        conn = FakeConnection()
        with patch(
            "email_automation_scan.notify_email_automation_event",
            return_value={"ok": True},
        ) as notify:
            summary = run_email_automation_scan(conn)

        closed_sql = next(
            sql for sql, _params in conn.executions if "closed_at IS NOT NULL" in sql
        )
        self.assertIn("closed_at IS NOT NULL", closed_sql)
        self.assertEqual(summary["closed_checked"], 0)
        notify.assert_not_called()

    def test_processed_event_is_deduped_by_stable_reference_key(self):
        conn = FakeConnection(closed_draws=[{
            "id": 133,
            "status": "sorteado",
            "draw_type": "principal",
            "product_name": "Principal",
            "closed_at": datetime.now(timezone.utc),
        }])
        with patch(
            "email_automation_scan.notify_email_automation_event",
            side_effect=[
                {"ok": True, "data": {"ok": True, "sent": 2, "failed": 0, "deduped": 0}},
                {"ok": True, "data": {"ok": True, "sent": 0, "failed": 0, "deduped": 2}},
            ],
        ) as notify:
            first = run_email_automation_scan(conn)
            second = run_email_automation_scan(conn)
        self.assertEqual(first["sent"], 2)
        self.assertEqual(second["sent"], 0)
        self.assertEqual(second["skipped"], 2)
        self.assertEqual(
            [item.kwargs["reference_key"] for item in notify.call_args_list],
            ["draw:133:closed_email", "draw:133:closed_email"],
        )

    def test_backend_recipient_counts_are_preserved_in_summary(self):
        conn = FakeConnection(closed_draws=[{
            "id": 133,
            "status": "closed",
            "draw_type": "principal",
            "product_name": "Principal",
            "closed_at": datetime.now(timezone.utc),
        }])
        with patch(
            "email_automation_scan.notify_email_automation_event",
            return_value={
                "ok": True,
                "data": {"ok": True, "status": "processed", "sent": 8, "failed": 2, "deduped": 0},
            },
        ):
            summary = run_email_automation_scan(conn)
        self.assertEqual(summary["sent"], 8)
        self.assertEqual(summary["failed"], 2)
        self.assertEqual(summary["skipped"], 0)
        self.assertFalse(summary["ok"])

    def test_backend_configuration_error_counts_failure_and_preserves_skipped(self):
        conn = FakeConnection(published_draws=[{
            "id": 133,
            "status": "open",
            "draw_type": "principal",
            "product_name": "Principal",
            "opened_at": datetime.now(timezone.utc),
        }])
        backend_data = {
            "ok": False,
            "status": "configuration_error",
            "reason": "manual_email_smtp_not_configured",
            "failed": 0,
            "skipped": 2,
        }
        with patch(
            "email_automation_scan.notify_email_automation_event",
            return_value={
                "ok": False,
                "status": 200,
                "reason": "manual_email_smtp_not_configured",
                "data": backend_data,
            },
        ):
            summary = run_email_automation_scan(conn)

        self.assertGreaterEqual(summary["failed"], 1)
        self.assertEqual(summary["skipped"], 2)
        self.assertFalse(summary["ok"])

    def test_delivery_unknown_timeout_marks_summary_failed_and_logs_reason(self):
        conn = FakeConnection(published_draws=[{
            "id": 133,
            "status": "open",
            "draw_type": "principal",
            "product_name": "Principal",
            "opened_at": datetime.now(timezone.utc),
        }])
        with patch(
            "email_automation_scan.notify_email_automation_event",
            return_value={
                "ok": False,
                "status": None,
                "reason": "backend_response_timeout",
                "delivery_unknown": True,
            },
        ), patch("builtins.print") as log:
            summary = run_email_automation_scan(conn)

        self.assertEqual(summary["failed"], 1)
        self.assertFalse(summary["ok"])
        log.assert_any_call("[email-automation] event_failed", {
            "draw_id": 133,
            "draw_type": "principal",
            "draw_name": "Principal",
            "event_key": "NEW_DRAW_PUBLISHED",
            "reference_key": "draw:133:published_email",
            "status": None,
            "reason": "backend_response_timeout",
        })

    def test_separate_published_and_closed_lookbacks_are_applied(self):
        conn = FakeConnection()
        with patch.dict("os.environ", {}, clear=True):
            run_email_automation_scan(conn)
        published_query = next(
            (sql, params)
            for sql, params in conn.executions
            if "opened_at IS NOT NULL" in sql
        )
        closed_query = next(
            (sql, params)
            for sql, params in conn.executions
            if "closed_at IS NOT NULL" in sql
        )
        self.assertEqual(
            published_query[1],
            (EMAIL_PUBLISHED_LOOKBACK_HOURS,),
        )
        self.assertIn("status = 'open'", published_query[0])
        self.assertNotIn(
            "status IN ('open', 'closed', 'sorteado')",
            published_query[0],
        )
        self.assertIn("closed_at >= NOW()", closed_query[0])
        self.assertEqual(closed_query[1], (EMAIL_CLOSED_LOOKBACK_HOURS,))
        draw_queries = [
            sql for sql, _params in conn.executions if "FROM public.draws" in sql
        ]
        self.assertEqual(len(draw_queries), 3)
        for query in draw_queries:
            self.assertIn(
                "AS draw_name",
                query,
            )
        self.assertEqual(EMAIL_DEFAULT_LOOKBACK_HOURS, 24)
        self.assertEqual(EMAIL_PUBLISHED_LOOKBACK_HOURS, 24)
        self.assertEqual(EMAIL_CLOSED_LOOKBACK_HOURS, 72)

    def test_one_backend_failure_does_not_stop_other_batch_events(self):
        opened_at = datetime.now(timezone.utc)
        conn = FakeConnection(published_draws=[
            {
                "id": 301,
                "status": "open",
                "draw_type": "principal",
                "product_name": "Primeiro",
                "opened_at": opened_at,
            },
            {
                "id": 302,
                "status": "open",
                "draw_type": "principal",
                "product_name": "Segundo",
                "opened_at": opened_at,
            },
            {
                "id": 303,
                "status": "open",
                "draw_type": "principal",
                "product_name": "Terceiro",
                "opened_at": opened_at,
            },
        ])
        with patch(
            "email_automation_scan.notify_email_automation_event",
            side_effect=[
                {"ok": False, "reason": "backend_request_failed"},
                requests.Timeout("timeout"),
                {"ok": True, "data": {"sent": 1}},
            ],
        ) as notify, patch("builtins.print") as log:
            summary = run_email_automation_scan(conn)

        self.assertEqual(notify.call_count, 3)
        self.assertEqual(log.call_count, 5)
        log.assert_any_call("[email-automation] event_failed", {
            "draw_id": 301,
            "draw_type": "principal",
            "draw_name": "Primeiro",
            "event_key": "NEW_DRAW_PUBLISHED",
            "reference_key": "draw:301:published_email",
            "status": None,
            "reason": "backend_request_failed",
        })
        self.assertEqual(summary["events"], 3)
        self.assertEqual(summary["failed"], 2)
        self.assertEqual(summary["sent"], 1)
        self.assertFalse(summary["ok"])


class EmailBalanceAutomationScanTest(unittest.TestCase):
    now = datetime(2026, 8, 6, 13, 0, tzinfo=timezone.utc)
    base_env = {
        "EMAIL_AUTOMATION_DRY_RUN": "false",
        "EMAIL_BALANCE_EXPIRED_BACKFILL_ENABLED": "false",
        "EMAIL_BALANCE_AUTOMATION_EFFECTIVE_FROM": "2026-01-01T00:00:00Z",
    }

    def _row(
        self,
        *,
        user_id=123,
        balance_cents=15000,
        email="cliente@example.com",
        expires_at=None,
        expires_on=None,
        days_to_expire=30,
    ):
        expiry = expires_at or datetime(2026, 9, 5, 3, 0, tzinfo=timezone.utc)
        return {
            "user_id": user_id,
            "name": "Cliente",
            "email": email,
            "balance_cents": balance_cents,
            "balance_reference_at": datetime(
                2026, 3, 5, 3, 0, tzinfo=timezone.utc
            ),
            "expires_at": expiry,
            "expires_on": expires_on if expires_on is not None else expiry.date(),
            "days_to_expire": days_to_expire,
            "expiry_source": "approved_payment",
        }

    def _run(self, rows, result=None, env=None):
        conn = FakeConnection(balance_rows=rows)
        configured_env = {**self.base_env, **(env or {})}
        with patch.dict(os.environ, configured_env, clear=False), patch(
            "email_automation_scan.notify_email_automation_event",
            return_value=result or {"ok": True, "data": {"sent": 1}},
        ) as notify, patch("builtins.print") as log:
            summary = run_email_automation_scan(conn, now=self.now)
        return conn, summary, notify, log

    def _assert_expiring_day(self, days, event_key):
        row = self._row(days_to_expire=days)
        _conn, summary, notify, _log = self._run([row])
        event = notify.call_args.kwargs
        self.assertEqual(event["event_key"], event_key)
        self.assertEqual(
            event["reference_key"],
            f"user_balance:123:expires:2026-09-05:email:{days}_days",
        )
        self.assertEqual(event["reference_type"], "user_balance")
        self.assertEqual(summary["balance_eligible"], 1)

    def test_day_30_generates_event(self):
        self._assert_expiring_day(30, "EMAIL_BALANCE_EXPIRING_30_DAYS")

    def test_day_20_generates_event(self):
        self._assert_expiring_day(20, "EMAIL_BALANCE_EXPIRING_20_DAYS")

    def test_day_10_generates_event(self):
        self._assert_expiring_day(10, "EMAIL_BALANCE_EXPIRING_10_DAYS")

    def test_day_7_generates_event(self):
        self._assert_expiring_day(7, "EMAIL_BALANCE_EXPIRING_7_DAYS")

    def test_day_3_generates_event(self):
        self._assert_expiring_day(3, "EMAIL_BALANCE_EXPIRING_3_DAYS")

    def test_negative_day_generates_expired_event(self):
        row = self._row(
            days_to_expire=-1,
            expires_at=datetime(2026, 8, 5, 3, 0, tzinfo=timezone.utc),
        )
        _conn, summary, notify, _log = self._run([row])
        self.assertEqual(notify.call_args.kwargs["event_key"], "EMAIL_BALANCE_EXPIRED")
        self.assertEqual(
            notify.call_args.kwargs["reference_key"],
            "user_balance:123:expires:2026-08-05:email:expired",
        )
        self.assertEqual(summary["balance_events"], 1)

    def test_intermediate_days_do_not_generate_events(self):
        rows = [
            self._row(user_id=index + 1, days_to_expire=days)
            for index, days in enumerate((29, 19, 9, 6, 2))
        ]
        _conn, summary, notify, _log = self._run(rows)
        notify.assert_not_called()
        self.assertEqual(summary["balance_checked"], 5)
        self.assertEqual(summary["balance_skipped"], 5)

    def test_day_zero_is_not_assumed_expired(self):
        _conn, summary, notify, _log = self._run([
            self._row(days_to_expire=0),
        ])
        notify.assert_not_called()
        self.assertEqual(summary["balance_skipped"], 1)

    def test_zero_balance_is_ignored(self):
        _conn, summary, notify, _log = self._run([
            self._row(balance_cents=0),
        ])
        notify.assert_not_called()
        self.assertEqual(summary["balance_skipped"], 1)

    def test_null_expiration_is_ignored(self):
        row = self._row()
        row["expires_at"] = None
        row["expires_on"] = None
        _conn, summary, notify, _log = self._run([row])
        notify.assert_not_called()
        self.assertEqual(summary["balance_skipped"], 1)

    def test_invalid_user_and_email_are_ignored(self):
        rows = [
            self._row(user_id=None),
            self._row(user_id=124, email="sem-arroba"),
            self._row(user_id=125, email=""),
        ]
        _conn, summary, notify, _log = self._run(rows)
        notify.assert_not_called()
        self.assertEqual(summary["balance_skipped"], 3)

    def test_reference_is_stable_and_balance_change_does_not_change_it(self):
        first = self._row(balance_cents=15000)
        second = self._row(balance_cents=9900)
        _conn, _summary, first_notify, _log = self._run([first])
        _conn, _summary, second_notify, _log = self._run([second])
        self.assertEqual(
            first_notify.call_args.kwargs["reference_key"],
            second_notify.call_args.kwargs["reference_key"],
        )

    def test_expiration_change_creates_new_reference(self):
        first = self._row()
        second = self._row(
            expires_at=datetime(2026, 9, 6, 3, 0, tzinfo=timezone.utc),
        )
        _conn, _summary, first_notify, _log = self._run([first])
        _conn, _summary, second_notify, _log = self._run([second])
        self.assertNotEqual(
            first_notify.call_args.kwargs["reference_key"],
            second_notify.call_args.kwargs["reference_key"],
        )

    def test_old_expired_balance_is_blocked(self):
        row = self._row(
            days_to_expire=-30,
            expires_at=datetime(2026, 7, 1, 3, 0, tzinfo=timezone.utc),
        )
        _conn, summary, notify, log = self._run([row], env={
            "EMAIL_BALANCE_AUTOMATION_EFFECTIVE_FROM": "2026-08-01T00:00:00Z",
        })
        notify.assert_not_called()
        self.assertEqual(summary["balance_expired_before_effective_from"], 1)
        self.assertEqual(summary["balance_skipped"], 1)
        self.assertIn(
            "balance_expired_before_effective_from",
            str(log.call_args_list),
        )

    def test_enabled_backfill_allows_old_expired_balance(self):
        row = self._row(
            days_to_expire=-30,
            expires_at=datetime(2026, 7, 1, 3, 0, tzinfo=timezone.utc),
        )
        _conn, summary, notify, _log = self._run([row], env={
            "EMAIL_BALANCE_AUTOMATION_EFFECTIVE_FROM": "2026-08-01T00:00:00Z",
            "EMAIL_BALANCE_EXPIRED_BACKFILL_ENABLED": "true",
        })
        self.assertEqual(notify.call_count, 1)
        self.assertEqual(summary["balance_eligible"], 1)

    def test_balance_payload_uses_canonical_fields_without_name_or_email(self):
        _conn, _summary, notify, _log = self._run([self._row()])
        metadata = notify.call_args.kwargs["metadata"]
        self.assertEqual(metadata, {
            "user_id": 123,
            "balance_cents": 15000,
            "balance_reference_at": "2026-03-05T03:00:00+00:00",
            "expires_at": "2026-09-05T03:00:00+00:00",
            "expires_on": "2026-09-05",
            "days_to_expire": 30,
            "expiry_source": "approved_payment",
        })
        self.assertNotIn("name", metadata)
        self.assertNotIn("email", metadata)

    def test_failure_does_not_interrupt_other_balance_events(self):
        rows = [self._row(user_id=123), self._row(user_id=124)]
        conn = FakeConnection(balance_rows=rows)
        with patch.dict(os.environ, self.base_env, clear=False), patch(
            "email_automation_scan.notify_email_automation_event",
            side_effect=[
                {"ok": False, "reason": "backend_http_error"},
                {"ok": True, "data": {"sent": 1}},
            ],
        ) as notify, patch("builtins.print"):
            summary = run_email_automation_scan(conn, now=self.now)
        self.assertEqual(notify.call_count, 2)
        self.assertEqual(summary["balance_failed"], 1)
        self.assertEqual(summary["balance_sent"], 1)
        self.assertFalse(summary["ok"])

    def test_draw_failure_does_not_interrupt_balance_event(self):
        conn = FakeConnection(
            open_draws=[{
                "id": 133,
                "status": "open",
                "draw_type": "principal",
                "product_name": "Principal",
            }],
            snapshots={133: {"remaining_numbers": 15}},
            balance_rows=[self._row()],
        )
        with patch.dict(os.environ, self.base_env, clear=False), patch(
            "email_automation_scan.notify_email_automation_event",
            side_effect=[
                {"ok": False, "reason": "backend_http_error"},
                {"ok": True, "data": {"sent": 1}},
            ],
        ) as notify, patch("builtins.print"):
            summary = run_email_automation_scan(conn, now=self.now)
        self.assertEqual(notify.call_count, 2)
        self.assertEqual(summary["balance_sent"], 1)
        self.assertEqual(summary["failed"], 1)
        self.assertFalse(summary["ok"])

    def test_balance_source_failure_does_not_interrupt_draw_event(self):
        class FailingBalanceCursor(FakeCursor):
            def execute(self, sql, params=()):
                if "user_coupon_balance_expiry" in sql:
                    raise RuntimeError("view unavailable")
                super().execute(sql, params)

        class FailingBalanceConnection(FakeConnection):
            def cursor(self):
                return FailingBalanceCursor(self)

        conn = FailingBalanceConnection(
            open_draws=[{
                "id": 133,
                "status": "open",
                "draw_type": "principal",
                "product_name": "Principal",
            }],
            snapshots={133: {"remaining_numbers": 15}},
        )
        with patch.dict(os.environ, self.base_env, clear=False), patch(
            "email_automation_scan.notify_email_automation_event",
            return_value={"ok": True, "data": {"sent": 1}},
        ) as notify, patch("builtins.print"):
            summary = run_email_automation_scan(conn, now=self.now)
        self.assertEqual(notify.call_count, 1)
        self.assertEqual(summary["balance_failed"], 1)
        self.assertEqual(summary["sent"], 1)
        self.assertFalse(summary["ok"])

    def test_deduplicated_event_is_not_failure(self):
        result = {"ok": True, "status": 409, "deduped": True}
        _conn, summary, _notify, _log = self._run([self._row()], result=result)
        self.assertEqual(summary["balance_deduped"], 1)
        self.assertEqual(summary["balance_failed"], 0)
        self.assertTrue(summary["ok"])

    def test_backend_recipient_failure_counts_as_balance_failure(self):
        result = {
            "ok": True,
            "data": {"sent": 0, "failed": 1, "deduped": 0},
        }
        _conn, summary, _notify, _log = self._run([self._row()], result=result)
        self.assertEqual(summary["balance_sent"], 0)
        self.assertEqual(summary["balance_failed"], 1)
        self.assertFalse(summary["ok"])

    def test_balance_counters_are_event_based(self):
        rows = [
            self._row(user_id=123, days_to_expire=30),
            self._row(user_id=124, days_to_expire=20),
            self._row(user_id=125, days_to_expire=29),
        ]
        conn = FakeConnection(balance_rows=rows)
        with patch.dict(os.environ, self.base_env, clear=False), patch(
            "email_automation_scan.notify_email_automation_event",
            side_effect=[
                {"ok": True, "data": {"sent": 1}},
                {"ok": True, "status": 409, "deduped": True},
            ],
        ), patch("builtins.print"):
            summary = run_email_automation_scan(conn, now=self.now)
        self.assertEqual(summary["balance_checked"], 3)
        self.assertEqual(summary["balance_with_valid_expiry"], 3)
        self.assertEqual(summary["balance_eligible"], 2)
        self.assertEqual(summary["balance_events"], 2)
        self.assertEqual(summary["balance_sent"], 1)
        self.assertEqual(summary["balance_deduped"], 1)
        self.assertEqual(summary["balance_failed"], 0)
        self.assertEqual(summary["balance_skipped"], 1)
        self.assertEqual(summary["events"], 2)

    def test_dry_run_builds_events_without_publishing(self):
        _conn, summary, notify, log = self._run([self._row()], env={
            "EMAIL_AUTOMATION_DRY_RUN": "true",
        })
        notify.assert_not_called()
        self.assertTrue(summary["dry_run"])
        self.assertEqual(summary["balance_events"], 1)
        self.assertEqual(summary["balance_sent"], 0)
        self.assertTrue(summary["ok"])
        self.assertIn("would_publish", str(log.call_args_list))

    def test_balance_candidates_are_loaded_in_one_canonical_view_query(self):
        conn, _summary, _notify, _log = self._run([self._row()])
        queries = [
            sql for sql, _params in conn.executions
            if "user_coupon_balance_expiry" in sql
        ]
        self.assertEqual(len(queries), 1)
        self.assertIn("WHERE balance_cents > 0", queries[0])
        for field in (
            "user_id",
            "balance_reference_at",
            "expires_at",
            "expires_on",
            "days_to_expire",
            "expiry_source",
        ):
            self.assertIn(field, queries[0])

    def test_draw_snapshots_and_type_label_remain_in_payload(self):
        draw = {
            "id": 151,
            "status": "open",
            "draw_type": "secundario",
            "product_name": "Vale-compras",
            "draw_description": "Descrição pública",
            "banner_title": "Banner",
        }
        conn = FakeConnection(
            open_draws=[draw],
            snapshots={151: {"remaining_numbers": 15}},
        )
        with patch.dict(os.environ, self.base_env, clear=False), patch(
            "email_automation_scan.notify_email_automation_event",
            return_value={"ok": True},
        ) as notify, patch("builtins.print"):
            run_email_automation_scan(conn, now=self.now)
        event = notify.call_args.kwargs
        metadata = event["metadata"]
        self.assertEqual(metadata["draw_type"], "secundario")
        self.assertEqual(metadata["draw_type_label"], "Sorteio secundário")
        self.assertEqual(metadata["draw_name"], "Vale-compras")
        self.assertEqual(metadata["draw_description"], "Descrição pública")
        self.assertEqual(metadata["banner_title"], "Banner")
        self.assertEqual(metadata["remaining_numbers"], 15)
        self.assertEqual(metadata["status"], "open")
        self.assertEqual(
            event["reference_key"],
            "additional_draw:151:email_remaining:15",
        )

    def test_database_connection_is_closed_before_http_publication(self):
        class ClosableFakeConnection(FakeConnection):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.closed = False
                self.rollback_count = 0

            def rollback(self):
                self.rollback_count += 1

            def close(self):
                self.closed = True

        conn = ClosableFakeConnection(balance_rows=[self._row()])

        def assert_closed_before_publish(**_kwargs):
            self.assertTrue(conn.closed)
            return {"ok": True, "data": {"sent": 1}}

        with patch.dict(os.environ, self.base_env, clear=False), patch(
            "email_automation_scan.notify_email_automation_event",
            side_effect=assert_closed_before_publish,
        ), patch("builtins.print"):
            summary = run_email_automation_scan(
                conn,
                close_connection_before_publish=True,
                now=self.now,
            )

        self.assertTrue(summary["ok"])
        self.assertTrue(conn.closed)
        self.assertGreaterEqual(conn.rollback_count, 1)

    def test_declared_balance_event_mapping_is_exact(self):
        self.assertEqual(EMAIL_BALANCE_EXPIRING_EVENTS, {
            30: "EMAIL_BALANCE_EXPIRING_30_DAYS",
            20: "EMAIL_BALANCE_EXPIRING_20_DAYS",
            10: "EMAIL_BALANCE_EXPIRING_10_DAYS",
            7: "EMAIL_BALANCE_EXPIRING_7_DAYS",
            3: "EMAIL_BALANCE_EXPIRING_3_DAYS",
        })


class EmailAutomationEventRetryTest(unittest.TestCase):
    class FakeResponse:
        def __init__(self, status_code, payload=None):
            self.status_code = status_code
            self.ok = 200 <= status_code < 300
            self.payload = payload or {}

        def json(self):
            return self.payload

    env = {
        "BACKEND_INTERNAL_API_BASE": "https://backend.example",
        "PUSH_INTERNAL_EVENTS_TOKEN": "token",
        "EMAIL_AUTOMATION_BACKEND_CONNECT_TIMEOUT_SECONDS": "10",
        "EMAIL_AUTOMATION_BACKEND_READ_TIMEOUT_SECONDS": "240",
    }

    def _notify(self):
        return notify_email_automation_event(
            event_key="NEW_DRAW_PUBLISHED",
            reference_type="draw",
            reference_key="draw:133:published_email",
            metadata={"draw_id": 133},
        )

    def test_positive_int_env_uses_default_for_missing_or_invalid_values(self):
        invalid_values = ("", "invalid", "0", "-1")
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                _positive_int_env("MISSING_TIMEOUT", 37),
                37,
            )
        for raw in invalid_values:
            with self.subTest(raw=raw), patch.dict(
                os.environ,
                {"TEST_TIMEOUT": raw},
                clear=True,
            ):
                self.assertEqual(_positive_int_env("TEST_TIMEOUT", 37), 37)

    def test_positive_int_env_accepts_positive_integer(self):
        with patch.dict(
            os.environ,
            {"TEST_TIMEOUT": "91"},
            clear=True,
        ):
            self.assertEqual(_positive_int_env("TEST_TIMEOUT", 37), 91)

    @patch.dict(os.environ, {
        **env,
        "EMAIL_AUTOMATION_BACKEND_CONNECT_TIMEOUT_SECONDS": "17",
        "EMAIL_AUTOMATION_BACKEND_READ_TIMEOUT_SECONDS": "321",
    }, clear=False)
    @patch("email_automation_events.requests.post")
    def test_request_uses_configured_connect_and_read_timeouts(self, post):
        post.return_value = self.FakeResponse(200, {"ok": True})

        result = self._notify()

        self.assertTrue(result["ok"])
        self.assertEqual(post.call_args.kwargs["timeout"], (17, 321))

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.requests.post")
    def test_backend_payload_contains_draw_name_and_draw_context(self, post):
        post.return_value = self.FakeResponse(200, {"ok": True})
        metadata = {
            "draw_id": 145,
            "draw_type": "adicional",
            "draw_name": "Sorteio adicional de créditos",
            "draw_description": "Créditos para usar na loja",
            "banner_title": "Créditos New Store",
            "draw_type_label": "Sorteio adicional",
            "status": "open",
            "remaining_numbers": 15,
            "threshold": 15,
            "product_name": "Sorteio adicional de créditos",
        }

        result = notify_email_automation_event(
            event_key="EMAIL_DRAW_REMAINING_15",
            reference_type="additional_draw",
            reference_key="additional_draw:145:email_remaining:15",
            metadata=metadata,
        )

        self.assertTrue(result["ok"])
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["event_key"], "EMAIL_DRAW_REMAINING_15")
        self.assertEqual(
            payload["reference_key"],
            "additional_draw:145:email_remaining:15",
        )
        self.assertEqual(payload["draw_id"], 145)
        self.assertEqual(payload["draw_type"], "adicional")
        self.assertEqual(payload["draw_name"], "Sorteio adicional de créditos")
        self.assertEqual(
            payload["draw_description"],
            "Créditos para usar na loja",
        )
        self.assertEqual(payload["banner_title"], "Créditos New Store")
        self.assertEqual(payload["draw_type_label"], "Sorteio adicional")
        self.assertEqual(payload["status"], "open")
        self.assertEqual(payload["remaining_numbers"], 15)
        self.assertEqual(payload["metadata"], metadata)
        self.assertEqual(
            post.call_args.kwargs["headers"]["Idempotency-Key"],
            "additional_draw:145:email_remaining:15",
        )

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.requests.post")
    def test_backend_balance_payload_preserves_canonical_metadata(self, post):
        post.return_value = self.FakeResponse(200, {"ok": True})
        metadata = {
            "user_id": 123,
            "balance_cents": 15000,
            "balance_reference_at": "2026-03-05T03:00:00Z",
            "expires_at": "2026-09-05T03:00:00Z",
            "expires_on": "2026-09-05",
            "days_to_expire": 30,
            "expiry_source": "approved_payment",
        }
        reference_key = (
            "user_balance:123:expires:2026-09-05:email:30_days"
        )

        notify_email_automation_event(
            event_key="EMAIL_BALANCE_EXPIRING_30_DAYS",
            reference_type="user_balance",
            reference_key=reference_key,
            scan_id="email-scan:20260806T130000Z:abc123",
            occurred_at="2026-08-06T13:00:00Z",
            metadata=metadata,
        )

        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["metadata"], metadata)
        self.assertNotIn("name", payload["metadata"])
        self.assertNotIn("email", payload["metadata"])
        self.assertEqual(
            post.call_args.kwargs["headers"],
            {
                "X-Internal-Token": "token",
                "Idempotency-Key": reference_key,
            },
        )

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.requests.post")
    def test_legacy_payload_without_draw_name_still_works(self, post):
        post.return_value = self.FakeResponse(200, {"ok": True})

        result = self._notify()

        self.assertTrue(result["ok"])
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["metadata"], {"draw_id": 133})
        self.assertEqual(payload["draw_id"], 133)
        self.assertNotIn("draw_name", payload)

    @patch.dict(os.environ, {
        "BACKEND_INTERNAL_API_BASE": "https://backend.example",
        "PUSH_INTERNAL_EVENTS_TOKEN": "token",
    }, clear=True)
    @patch("email_automation_events.requests.post")
    def test_request_uses_default_timeouts_when_environment_is_absent(self, post):
        post.return_value = self.FakeResponse(200, {"ok": True})

        result = self._notify()

        self.assertTrue(result["ok"])
        self.assertEqual(post.call_args.kwargs["timeout"], (
            DEFAULT_BACKEND_CONNECT_TIMEOUT_SECONDS,
            DEFAULT_BACKEND_READ_TIMEOUT_SECONDS,
        ))

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.time.sleep", return_value=None)
    @patch("email_automation_events.requests.post")
    def test_read_timeout_is_retried_and_delivery_is_unknown(self, post, sleep):
        post.side_effect = requests.ReadTimeout("response timeout")

        result = self._notify()

        self.assertEqual(result, {
            "ok": False,
            "status": None,
            "reason": "backend_read_timeout",
            "delivery_unknown": True,
            "attempts": 3,
        })
        self.assertEqual(post.call_count, 3)
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list],
            [2, 5],
        )

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.time.sleep", return_value=None)
    @patch("email_automation_events.requests.post")
    def test_connect_timeout_exhausts_safe_retries(self, post, sleep):
        post.side_effect = requests.ConnectTimeout("connect timeout")

        result = self._notify()

        self.assertEqual(result, {
            "ok": False,
            "status": None,
            "reason": "backend_connect_timeout",
            "attempts": 3,
        })
        self.assertEqual(post.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.requests.post")
    def test_http_200_backend_configuration_error_is_failure(self, post):
        backend_data = {
            "ok": False,
            "status": "configuration_error",
            "reason": "manual_email_smtp_not_configured",
            "failed": 0,
            "skipped": 2,
        }
        post.return_value = self.FakeResponse(200, backend_data)

        result = self._notify()

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["reason"], "backend_http_error")
        self.assertEqual(
            result["backend_reason"],
            "manual_email_smtp_not_configured",
        )
        self.assertEqual(result["data"], backend_data)
        self.assertEqual(post.call_count, 1)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.time.sleep", return_value=None)
    @patch("email_automation_events.requests.post")
    def test_connect_timeout_retries_and_can_recover(self, post, _sleep):
        post.side_effect = [
            requests.ConnectTimeout("connect timeout"),
            self.FakeResponse(200, {"sent": 1}),
        ]

        result = self._notify()

        self.assertTrue(result["ok"])
        self.assertEqual(post.call_count, 2)
        first = post.call_args_list[0].kwargs
        second = post.call_args_list[1].kwargs
        self.assertEqual(first["json"], second["json"])
        self.assertEqual(
            first["headers"]["Idempotency-Key"],
            second["headers"]["Idempotency-Key"],
        )

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.time.sleep", return_value=None)
    @patch("email_automation_events.requests.post")
    def test_backend_unavailable_exhausts_connection_retries(self, post, _sleep):
        post.side_effect = requests.ConnectionError("offline")

        result = self._notify()

        self.assertFalse(result["ok"])
        self.assertIsNone(result["status"])
        self.assertEqual(result["reason"], "backend_connection_error")
        self.assertEqual(post.call_count, 3)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.time.sleep", return_value=None)
    @patch("email_automation_events.requests.post")
    def test_retryable_non_2xx_exhausts_retries(self, post, _sleep):
        post.return_value = self.FakeResponse(503, {"error": "unavailable"})

        result = self._notify()

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 503)
        self.assertEqual(result["reason"], "backend_http_error")
        self.assertEqual(result["backend_reason"], "unavailable")
        self.assertEqual(result["data"], {"error": "unavailable"})
        self.assertEqual(post.call_count, 3)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.time.sleep", return_value=None)
    @patch("email_automation_events.requests.post")
    def test_http_502_retries_and_can_recover(self, post, sleep):
        post.side_effect = [
            self.FakeResponse(502, {"error": "bad_gateway"}),
            self.FakeResponse(200, {"ok": True}),
        ]

        result = self._notify()

        self.assertTrue(result["ok"])
        self.assertEqual(post.call_count, 2)
        sleep.assert_called_once_with(2)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.time.sleep", return_value=None)
    @patch("email_automation_events.requests.post")
    def test_http_500_retries_three_times_and_preserves_backend_error(self, post, _sleep):
        backend_data = {"error": "email_event_failed"}
        post.return_value = self.FakeResponse(500, backend_data)

        result = self._notify()

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 500)
        self.assertEqual(result["reason"], "backend_http_error")
        self.assertEqual(result["backend_reason"], "email_event_failed")
        self.assertEqual(result["data"], backend_data)
        self.assertEqual(post.call_count, 3)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.requests.post")
    def test_non_retryable_non_2xx_does_not_block_future_scans(self, post):
        post.return_value = self.FakeResponse(401, {"error": "unauthorized"})

        result = self._notify()

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 401)
        self.assertEqual(result["reason"], "backend_http_error")
        self.assertEqual(result["backend_reason"], "unauthorized")
        self.assertEqual(post.call_count, 1)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.requests.post")
    def test_http_400_is_not_retried_and_preserves_backend_error(self, post):
        post.return_value = self.FakeResponse(
            400,
            {"error": "email_draw_id_invalid"},
        )

        result = self._notify()

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 400)
        self.assertEqual(result["reason"], "backend_http_error")
        self.assertEqual(result["backend_reason"], "email_draw_id_invalid")
        self.assertEqual(result["data"], {"error": "email_draw_id_invalid"})
        self.assertEqual(post.call_count, 1)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.requests.post")
    def test_backend_dedupe_conflict_is_successful_and_not_retried(self, post):
        post.return_value = self.FakeResponse(409, {"status": "skipped"})

        result = self._notify()

        self.assertTrue(result["ok"])
        self.assertTrue(result["deduped"])
        self.assertEqual(result["status"], 409)
        self.assertEqual(post.call_count, 1)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.requests.post")
    def test_invalid_success_response_is_classified(self, post):
        response = self.FakeResponse(200)

        def invalid_json():
            raise ValueError("invalid json")

        response.json = invalid_json
        post.return_value = response

        result = self._notify()

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "backend_invalid_response")
        self.assertEqual(post.call_count, 1)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.requests.post")
    def test_logs_do_not_contain_token_or_email(self, post):
        post.return_value = self.FakeResponse(200, {"ok": True})
        with patch("builtins.print") as log:
            notify_email_automation_event(
                event_key="EMAIL_BALANCE_EXPIRING_30_DAYS",
                reference_type="user_balance",
                reference_key=(
                    "user_balance:123:expires:2026-09-05:email:30_days"
                ),
                metadata={
                    "user_id": 123,
                    "balance_cents": 15000,
                    "expires_on": "2026-09-05",
                    "days_to_expire": 30,
                },
            )

        rendered_logs = str(log.call_args_list)
        self.assertNotIn("token", rendered_logs)
        self.assertNotIn("cliente@example.com", rendered_logs)


if __name__ == "__main__":
    unittest.main()
