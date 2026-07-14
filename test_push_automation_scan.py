import os
import unittest
from unittest.mock import patch

from push_automation_scan import run_push_automation_scan, _select_remaining_threshold
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
