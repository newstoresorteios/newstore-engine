import io
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from unittest.mock import patch

import main
import push_automation_scan as scan
from test_push_winner_temporal import SimConn, REAL_DRAWS_COLUMNS, principal


class ResultPushRecoveryTests(unittest.TestCase):
    def test_failed_older_events_do_not_starve_an_unattempted_result(self):
        now = datetime.now(timezone.utc)
        candidates = [{"event_key": "WINNER_DEFINED", "reference_key": f"draw:{i}:winner_defined",
                       "reference_type": "draw", "occurred_at": now} for i in (147, 148, 149)]
        attempted = []
        with patch.object(scan, "_event_last_attempts", return_value={
                "draw:147:winner_defined": now, "draw:148:winner_defined": now}, create=True), \
             patch.object(scan, "notify_push_automation_event", side_effect=lambda **e: attempted.append(e["reference_key"]) or {"ok": False}), \
             redirect_stdout(io.StringIO()):
            scan._process_candidates(scan._new_scan_context(scan._scan_config()), candidates, 3, "winner_defined")
        self.assertIn("draw:149:winner_defined", attempted)
        self.assertLessEqual(len(attempted), 2)

    def test_http_200_with_backend_failure_is_not_success(self):
        context = scan._new_scan_context(scan._scan_config())
        candidate = {"event_key": "WINNER_DEFINED", "reference_key": "draw:149:winner_defined",
                     "reference_type": "draw", "occurred_at": datetime.now(timezone.utc)}
        with patch.object(scan, "notify_push_automation_event", return_value={"ok": True, "data": {"ok": False}}), \
             redirect_stdout(io.StringIO()):
            scan._process_candidates(context, [candidate], 1, "winner_defined")
        self.assertTrue(context.get("publish_failed"))

    def test_additional_and_principal_have_independent_pending_batches(self):
        now = datetime.now(timezone.utc)
        rows = [principal(149, now, now), principal(151, now, now, draw_type="adicional")]
        emitted = []
        config = scan._scan_config()
        config["allow_large_batch"] = True
        context = scan._new_scan_context(config)
        with patch.object(scan, "_table_columns", return_value=REAL_DRAWS_COLUMNS), \
             patch.object(scan, "notify_push_automation_event", side_effect=lambda **kw: emitted.append(kw) or {"ok": True}), \
             redirect_stdout(io.StringIO()):
            scan.emit_winner_defined_events(SimConn(rows), context)
            scan.emit_additional_winner_defined_events(SimConn(rows), context)
        self.assertEqual({event["reference_key"] for event in emitted}, {"draw:149:winner_defined", "additional_draw:151:winner_defined"})

    def test_result_event_contains_realization_time_and_execution_id(self):
        realized = datetime.now(timezone.utc)
        for draw_type, prefix in [("principal", "draw"), ("adicional", "additional_draw")]:
            with self.subTest(draw_type=draw_type):
                event = main._winner_defined_event(
                    {"id": 149, "draw_type": draw_type, "realized_at": realized}, 0, 11, "Ana"
                )
                self.assertEqual(event.get("occurred_at"), realized.isoformat())
                self.assertTrue(event.get("scan_id"))
                self.assertEqual(event["reference_key"], f"{prefix}:149:winner_defined")

    def test_three_results_are_processed_in_two_bounded_batches(self):
        now = datetime.now(timezone.utc)
        rows = [principal(i, now, now) for i in (147, 148, 149)]
        delivered = set()
        emitted = []
        def notify(**event):
            emitted.append(event["reference_key"])
            delivered.add(event["reference_key"])
            return {"ok": True, "data": {"status": "sent", "sent": 1}}
        def completed(_conn, event_key):
            return delivered.copy()
        with patch.object(scan, "_table_columns", return_value=REAL_DRAWS_COLUMNS), \
             patch.object(scan, "_completed_event_references", completed, create=True), \
             patch.object(scan, "notify_push_automation_event", side_effect=notify), \
             redirect_stdout(io.StringIO()):
            config = scan._scan_config()
            config["require_occurred_at"] = True
            scan.emit_winner_defined_events(SimConn(rows), scan._new_scan_context(config))
            self.assertEqual(len(emitted), 2)
            scan.emit_winner_defined_events(SimConn(rows), scan._new_scan_context(config))
        self.assertEqual(len(emitted), 3)
        self.assertEqual(len(set(emitted)), 3)

    def test_processing_rejects_missing_realization_instead_of_fabricating_timestamp(self):
        with self.assertRaisesRegex(RuntimeError, "realized_at"):
            main._winner_defined_event({"id": 149}, 7, 11, "Ana")


if __name__ == "__main__":
    unittest.main()
