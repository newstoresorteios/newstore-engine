import unittest
from unittest.mock import patch

from email_automation_scan import (
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
        if "status = 'open'" in sql:
            self.rows = self.conn.open_draws
        elif "status = 'closed'" in sql:
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
    def __init__(self, open_draws=None, snapshots=None, closed_draws=None):
        self.open_draws = open_draws or []
        self.snapshots = snapshots or {}
        self.closed_draws = closed_draws or []

    def cursor(self):
        return FakeCursor(self)


class EmailAutomationScanTest(unittest.TestCase):
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
            {"id": 133, "draw_type": "principal", "product_name": "Principal", "closed_at": None},
            {"id": 150, "draw_type": "adicional", "product_name": "Adicional", "closed_at": None},
        ])
        with patch("email_automation_scan.notify_email_automation_event", return_value={"ok": True}) as notify:
            run_email_automation_scan(conn)
        keys = [call.kwargs["reference_key"] for call in notify.call_args_list]
        self.assertEqual(keys, ["draw:133:closed_email", "additional_draw:150:closed_email"])


if __name__ == "__main__":
    unittest.main()
