import os
import unittest
from collections import defaultdict
from datetime import datetime, timezone
from unittest.mock import patch

from push_automation_scan import (
    _empty_event_key_stats,
    _get_sold_snapshot,
    _select_remaining_threshold,
    emit_additional_remaining_numbers_events,
    emit_additional_winner_defined_events,
    emit_remaining_numbers_events,
    run_push_automation_scan,
)
from push_automation_events import notify_push_automation_event


def _empty_summary(ok=True):
    return {
        "ok": ok,
        "remaining_numbers_checked": 0,
        "winner_checked": 0,
        "balance_checked": 0,
        "events_candidates": 0,
        "events_attempted": 0,
        "events_blocked": 0,
        "events_skipped": 0,
        "events_sent_to_backend": 0,
    }


def run_manual_scan():
    if os.getenv("PUSH_AUTOMATION_SCAN_ENABLED", "false").lower() != "true":
        print("[push-automation] manual scan skipped: scan disabled")
        summary = _empty_summary()
        print("[push-automation] scan:finished", summary)
        return 0

    from main import db

    conn = db()
    try:
        summary = run_push_automation_scan(conn)
        conn.rollback()
        print("[push-automation] scan:finished", {
            "ok": summary.get("ok") is True,
            "scan_id": summary.get("scan_id"),
            "remaining_numbers_checked": summary.get("remaining_numbers_checked", 0),
            "winner_checked": summary.get("winner_checked", 0),
            "balance_checked": summary.get("balance_checked", 0),
            "events_candidates": summary.get("events_candidates", 0),
            "events_attempted": summary.get("events_attempted", 0),
            "events_blocked": summary.get("events_blocked", 0),
            "events_skipped": summary.get("events_skipped", 0),
            "events_sent_to_backend": summary.get("events_sent_to_backend", 0),
            "by_event_key": summary.get("by_event_key", {}),
        })
        return 0
    except Exception as exc:
        print("[push-automation] manual scan failed:", {
            "message": str(exc) or "manual_scan_failed",
        })
        try:
            conn.rollback()
        except Exception:
            pass
        print("[push-automation] scan:finished", _empty_summary(ok=False))
        return 1
    finally:
        conn.close()


class RemainingThresholdSelectionTest(unittest.TestCase):
    def test_remaining_above_75_does_not_emit(self):
        self.assertIsNone(_select_remaining_threshold(76))

    def test_remaining_equal_75_emits_75(self):
        self.assertEqual(
            _select_remaining_threshold(75),
            (75, "DRAW_REMAINING_NUMBERS_75"),
        )

    def test_remaining_below_75_recovers_75(self):
        self.assertEqual(
            _select_remaining_threshold(72),
            (75, "DRAW_REMAINING_NUMBERS_75"),
        )

    def test_remaining_above_50_does_not_emit_50(self):
        self.assertEqual(
            _select_remaining_threshold(51),
            (75, "DRAW_REMAINING_NUMBERS_75"),
        )

    def test_remaining_below_50_emits_50(self):
        self.assertEqual(
            _select_remaining_threshold(48),
            (50, "DRAW_REMAINING_NUMBERS_50"),
        )

    def test_remaining_above_20_does_not_emit_20(self):
        self.assertEqual(
            _select_remaining_threshold(21),
            (50, "DRAW_REMAINING_NUMBERS_50"),
        )

    def test_remaining_below_20_emits_20(self):
        self.assertEqual(
            _select_remaining_threshold(18),
            (20, "DRAW_REMAINING_NUMBERS_20"),
        )

    def test_remaining_above_10_does_not_emit_10(self):
        self.assertEqual(
            _select_remaining_threshold(11),
            (20, "DRAW_REMAINING_NUMBERS_20"),
        )

    def test_remaining_below_10_emits_only_10(self):
        self.assertEqual(
            _select_remaining_threshold(8),
            (10, "DRAW_REMAINING_NUMBERS_10"),
        )

    def test_reference_key_remains_stable_for_50(self):
        draw_id = 123
        threshold, _event_key = _select_remaining_threshold(48)
        first = f"draw:{draw_id}:remaining:{threshold}"
        second = f"draw:{draw_id}:remaining:{threshold}"
        self.assertEqual(first, "draw:123:remaining:50")
        self.assertEqual(first, second)


class FakeCursor:
    def __init__(self, rows=None, one=None):
        self.rows = rows or []
        self.one = one
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        return False

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchall(self):
        return self.rows

    def fetchone(self):
        if self.one is not None:
            return self.one
        return self.rows[0] if self.rows else {}


class FakeConn:
    def __init__(self, rows=None, one=None):
        self.cursor_instance = FakeCursor(rows=rows, one=one)

    def cursor(self):
        return self.cursor_instance


def _test_ctx():
    return {
        "scan_id": "push-scan:test",
        "config": {
            "allow_large_batch": True,
            "max_events_per_scan": 20,
            "max_events_per_key_per_scan": 20,
            "require_occurred_at": False,
            "no_backfill": False,
            "remaining_lookback_hours": 24,
            "remaining_max_events_per_scan": 20,
            "winner_lookback_hours": 24,
            "winner_max_events_per_scan": 20,
        },
        "events_by_key": defaultdict(_empty_event_key_stats),
    }


def _draw(draw_id, sold, draw_type="adicional"):
    now = datetime(2026, 7, 14, tzinfo=timezone.utc)
    return {
        "id": draw_id,
        "draw_type": draw_type,
        "product_name": f"Adicional {draw_id}",
        "product_link": "https://example.invalid/ignored",
        "updated_at": now,
        "_sold": sold,
    }


class AdditionalRemainingNumbersTest(unittest.TestCase):
    draw_columns = {
        "id": "integer",
        "status": "text",
        "draw_type": "text",
        "product_name": "text",
        "product_link": "text",
        "updated_at": "timestamp with time zone",
    }

    def _capture_candidates(self, draws):
        captured = {}

        def fake_sold(_conn, draw_id):
            draw = next(item for item in draws if int(item["id"]) == int(draw_id))
            return {"sold": draw["_sold"], "activity_at": draw["updated_at"]}

        def fake_process(_ctx, candidates, checked, group, *_args, **_kwargs):
            captured["candidates"] = candidates
            captured["checked"] = checked
            captured["group"] = group
            return {
                "checked": checked,
                "events_candidates": len(candidates),
                "events_attempted": 0,
                "events_blocked": 0,
                "events_skipped": 0,
                "events_sent_to_backend": 0,
            }

        with patch("push_automation_scan._table_columns", return_value=self.draw_columns), \
             patch("push_automation_scan._get_sold_snapshot", side_effect=fake_sold), \
             patch("push_automation_scan._process_candidates", side_effect=fake_process):
            emit_additional_remaining_numbers_events(FakeConn(rows=draws), _test_ctx())

        return captured

    def test_additional_with_72_remaining_emits_75(self):
        captured = self._capture_candidates([_draw(145, sold=28)])
        candidate = captured["candidates"][0]
        self.assertEqual(candidate["event_key"], "DRAW_REMAINING_NUMBERS_75")
        self.assertEqual(candidate["reference_type"], "additional_draw")
        self.assertEqual(candidate["reference_key"], "additional_draw:145:remaining:75")
        self.assertEqual(candidate["metadata"]["remaining_numbers"], 72)
        self.assertEqual(candidate["metadata"]["draw_url"], "/")
        self.assertTrue(candidate["metadata"]["is_additional_draw"])

    def test_additional_with_48_remaining_emits_50(self):
        captured = self._capture_candidates([_draw(146, sold=52)])
        self.assertEqual(captured["candidates"][0]["event_key"], "DRAW_REMAINING_NUMBERS_50")
        self.assertEqual(captured["candidates"][0]["reference_key"], "additional_draw:146:remaining:50")

    def test_additional_with_18_remaining_emits_20(self):
        captured = self._capture_candidates([_draw(147, sold=82)])
        self.assertEqual(captured["candidates"][0]["event_key"], "DRAW_REMAINING_NUMBERS_20")

    def test_additional_with_8_remaining_emits_only_10(self):
        captured = self._capture_candidates([_draw(148, sold=92)])
        self.assertEqual(len(captured["candidates"]), 1)
        self.assertEqual(captured["candidates"][0]["event_key"], "DRAW_REMAINING_NUMBERS_10")

    def test_two_additional_draws_have_distinct_reference_keys(self):
        captured = self._capture_candidates([_draw(149, sold=52), _draw(150, sold=82, draw_type="secundario")])
        reference_keys = {candidate["reference_key"] for candidate in captured["candidates"]}
        self.assertEqual(reference_keys, {
            "additional_draw:149:remaining:50",
            "additional_draw:150:remaining:20",
        })
        self.assertEqual(captured["checked"], 2)

    def test_additional_without_sales_does_not_emit_threshold(self):
        captured = self._capture_candidates([_draw(151, sold=0)])
        self.assertEqual(captured["candidates"], [])

    def test_principal_and_additional_keep_separate_reference_types(self):
        principal_draw = {"id": 1, "updated_at": datetime(2026, 7, 14, tzinfo=timezone.utc)}
        principal_captured = {}
        additional_captured = self._capture_candidates([_draw(152, sold=52)])

        def fake_process(_ctx, candidates, checked, group, *_args, **_kwargs):
            principal_captured["candidates"] = candidates
            return {
                "checked": checked,
                "events_candidates": len(candidates),
                "events_attempted": 0,
                "events_blocked": 0,
                "events_skipped": 0,
                "events_sent_to_backend": 0,
            }

        with patch("push_automation_scan._table_columns", return_value={
            "id": "integer",
            "status": "text",
            "draw_type": "text",
            "updated_at": "timestamp with time zone",
        }), \
             patch("push_automation_scan._get_total_slots_from_config", return_value=100), \
             patch("push_automation_scan._get_sold_snapshot", return_value={
                 "sold": 52,
                 "activity_at": principal_draw["updated_at"],
             }), \
             patch("push_automation_scan._process_candidates", side_effect=fake_process):
            emit_remaining_numbers_events(FakeConn(rows=[principal_draw]), _test_ctx())

        self.assertEqual(principal_captured["candidates"][0]["reference_type"], "draw")
        self.assertEqual(principal_captured["candidates"][0]["reference_key"], "draw:1:remaining:50")
        self.assertEqual(additional_captured["candidates"][0]["reference_type"], "additional_draw")
        self.assertEqual(additional_captured["candidates"][0]["reference_key"], "additional_draw:152:remaining:50")


class AdditionalWinnerDefinedTest(unittest.TestCase):
    draw_columns = {
        "id": "integer",
        "status": "text",
        "draw_type": "text",
        "winner_number": "integer",
        "winner_user_id": "integer",
        "realized_at": "timestamp with time zone",
        "updated_at": "timestamp with time zone",
        "product_name": "text",
    }

    def test_additional_winner_closed_status_emits_winner_defined(self):
        now = datetime(2026, 7, 14, tzinfo=timezone.utc)
        rows = [{
            "id": 160,
            "draw_type": "adicional",
            "winner_number": 33,
            "winner_user_id": 123,
            "realized_at": now,
            "product_name": "Adicional Premiado",
        }]
        captured = {}

        def fake_process(_ctx, candidates, checked, group, *_args, **_kwargs):
            captured["candidates"] = candidates
            captured["checked"] = checked
            captured["group"] = group
            return {
                "checked": checked,
                "events_candidates": len(candidates),
                "events_attempted": 0,
                "events_blocked": 0,
                "events_skipped": 0,
                "events_sent_to_backend": 0,
            }

        fake_conn = FakeConn(rows=rows)
        fake_conn.cursor_instance.one = {"candidates_count": 1}
        with patch("push_automation_scan._table_columns", return_value=self.draw_columns), \
             patch("push_automation_scan._process_candidates", side_effect=fake_process):
            emit_additional_winner_defined_events(fake_conn, _test_ctx())

        candidate = captured["candidates"][0]
        self.assertEqual(candidate["event_key"], "WINNER_DEFINED")
        self.assertEqual(candidate["reference_type"], "additional_draw")
        self.assertEqual(candidate["reference_key"], "additional_draw:160:winner_defined")
        self.assertEqual(candidate["metadata"]["winner_number"], 33)
        self.assertEqual(candidate["metadata"]["winner_user_id"], 123)
        self.assertEqual(candidate["metadata"]["draw_name"], "Adicional Premiado")

    def test_additional_winner_reference_key_is_stable_for_dedupe(self):
        draw_id = 160
        first = f"additional_draw:{draw_id}:winner_defined"
        second = f"additional_draw:{draw_id}:winner_defined"
        self.assertEqual(first, second)


class SoldSnapshotSqlTest(unittest.TestCase):
    def test_paid_and_approved_payments_count_as_sold(self):
        fake_conn = FakeConn(one={"sold": 1})
        with patch("push_automation_scan._table_columns", side_effect=[
            {"number": "integer", "status": "text", "payment_id": "integer"},
            {"id": "integer", "status": "text"},
        ]):
            result = _get_sold_snapshot(fake_conn, 145)

        sql = fake_conn.cursor_instance.queries[0][0]
        self.assertEqual(result["sold"], 1)
        self.assertIn("r.status = 'paid'", sql)
        self.assertIn("p.status IN ('approved','paid')", sql)


class PushAutomationEventRetryTest(unittest.TestCase):
    def _response(self, status_code, body=None):
        class FakeResponse:
            def __init__(self, code, payload):
                self.status_code = code
                self.ok = 200 <= code < 300
                self._payload = payload or {}

            def json(self):
                return self._payload

        return FakeResponse(status_code, body)

    @patch.dict(os.environ, {
        "PUSH_AUTOMATION_EVENTS_ENABLED": "true",
        "BACKEND_INTERNAL_API_BASE": "https://backend.example",
        "PUSH_INTERNAL_EVENTS_TOKEN": "token",
    }, clear=False)
    @patch("push_automation_events.time.sleep", return_value=None)
    @patch("push_automation_events.requests.post")
    def test_retry_503_then_200(self, post, _sleep):
        post.side_effect = [
            self._response(503, {"error": "unavailable"}),
            self._response(200, {"ok": True, "status": "dry_run"}),
        ]

        result = notify_push_automation_event(
            event_key="DRAW_REMAINING_NUMBERS_50",
            reference_type="draw",
            reference_key="draw:1:remaining:50",
            metadata={"draw_id": 1},
            scan_id="scan:test",
            occurred_at="2026-07-14T00:00:00Z",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(post.call_count, 2)

    @patch.dict(os.environ, {
        "PUSH_AUTOMATION_EVENTS_ENABLED": "true",
        "BACKEND_INTERNAL_API_BASE": "https://backend.example",
        "PUSH_INTERNAL_EVENTS_TOKEN": "token",
    }, clear=False)
    @patch("push_automation_events.requests.post")
    def test_no_retry_on_401(self, post):
        post.return_value = self._response(401, {"error": "unauthorized"})

        result = notify_push_automation_event(
            event_key="DRAW_REMAINING_NUMBERS_50",
            reference_type="draw",
            reference_key="draw:1:remaining:50",
            metadata={"draw_id": 1},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 401)
        self.assertEqual(post.call_count, 1)


if __name__ == "__main__":
    raise SystemExit(run_manual_scan())
