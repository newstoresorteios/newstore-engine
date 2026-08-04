import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import requests

from email_automation_events import notify_email_automation_event
from email_automation_scan import (
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
        elif "status = 'open'" in sql:
            self.rows = self.conn.open_draws
        elif "closed_at IS NOT NULL" in sql:
            self.rows = self.conn.closed_draws
        elif "FROM public.numbers" in sql:
            draw_id = int(params[0])
            self.rows = [self.conn.snapshots[draw_id]]
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
    ):
        self.published_draws = published_draws or []
        self.open_draws = open_draws or []
        self.snapshots = snapshots or {}
        self.closed_draws = closed_draws or []
        self.executions = []

    def cursor(self):
        return FakeCursor(self)


class EmailAutomationScanTest(unittest.TestCase):
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
        self.assertEqual(log.call_count, 2)
        log.assert_any_call("[email-automation] event_failed", {
            "event_key": "NEW_DRAW_PUBLISHED",
            "reference_key": "draw:301:published_email",
            "status": None,
            "reason": "backend_request_failed",
        })
        self.assertEqual(summary["events"], 3)
        self.assertEqual(summary["failed"], 2)
        self.assertEqual(summary["sent"], 1)
        self.assertFalse(summary["ok"])


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
    }

    def _notify(self):
        return notify_email_automation_event(
            event_key="NEW_DRAW_PUBLISHED",
            reference_type="draw",
            reference_key="draw:133:published_email",
            metadata={"draw_id": 133},
        )

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
        self.assertEqual(result["reason"], "manual_email_smtp_not_configured")
        self.assertEqual(result["data"], backend_data)
        self.assertEqual(post.call_count, 1)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.time.sleep", return_value=None)
    @patch("email_automation_events.requests.post")
    def test_timeout_retries_and_can_recover(self, post, _sleep):
        post.side_effect = [
            requests.Timeout("timeout"),
            self.FakeResponse(200, {"sent": 1}),
        ]

        result = self._notify()

        self.assertTrue(result["ok"])
        self.assertEqual(post.call_count, 2)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.time.sleep", return_value=None)
    @patch("email_automation_events.requests.post")
    def test_backend_unavailable_exhausts_connection_retries(self, post, _sleep):
        post.side_effect = requests.ConnectionError("offline")

        result = self._notify()

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "backend_request_failed")
        self.assertEqual(post.call_count, 3)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.time.sleep", return_value=None)
    @patch("email_automation_events.requests.post")
    def test_retryable_non_2xx_exhausts_retries(self, post, _sleep):
        post.return_value = self.FakeResponse(503, {"error": "unavailable"})

        result = self._notify()

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 503)
        self.assertEqual(result["reason"], "unavailable")
        self.assertEqual(result["data"], {"error": "unavailable"})
        self.assertEqual(post.call_count, 3)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.time.sleep", return_value=None)
    @patch("email_automation_events.requests.post")
    def test_http_500_retries_three_times_and_preserves_backend_error(self, post, _sleep):
        backend_data = {"error": "email_event_failed"}
        post.return_value = self.FakeResponse(500, backend_data)

        result = self._notify()

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 500)
        self.assertEqual(result["reason"], "email_event_failed")
        self.assertEqual(result["data"], backend_data)
        self.assertEqual(post.call_count, 3)

    @patch.dict(os.environ, env, clear=False)
    @patch("email_automation_events.requests.post")
    def test_non_retryable_non_2xx_does_not_block_future_scans(self, post):
        post.return_value = self.FakeResponse(401, {"error": "unauthorized"})

        result = self._notify()

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 401)
        self.assertEqual(result["reason"], "unauthorized")
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
        self.assertEqual(result["reason"], "email_draw_id_invalid")
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


if __name__ == "__main__":
    unittest.main()
