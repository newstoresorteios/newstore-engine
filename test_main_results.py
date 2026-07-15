import unittest
from contextlib import ExitStack
from datetime import date, datetime, timezone
from unittest.mock import Mock, call, patch

import main


class FakeConnection:
    def __init__(self):
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.closed = True


class RecordingCursor:
    def __init__(self, rows=None, rowcount=1):
        self.rows = rows or []
        self.rowcount = rowcount
        self.sql = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return self.rows


class SequenceCursor(RecordingCursor):
    def __init__(self, fetchone_rows):
        super().__init__()
        self.fetchone_rows = list(fetchone_rows)
        self.executions = []

    def execute(self, sql, params=None):
        super().execute(sql, params)
        self.executions.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self.fetchone_rows.pop(0) if self.fetchone_rows else None


class CursorConnection(FakeConnection):
    def __init__(self, cursor):
        super().__init__()
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def draw(draw_id, draw_type="principal", status="closed", closed_day=14, product_name=None):
    return {
        "id": draw_id,
        "status": status,
        "draw_type": draw_type,
        "opened_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        "closed_at": datetime(2026, 7, closed_day, 20, tzinfo=timezone.utc),
        "product_name": product_name,
    }


def lotomania(number=33, result_day=15):
    return {
        "winner_number": number,
        "contest_number": 2891,
        "result_date": date(2026, 7, result_day),
    }


class ResultProcessingTests(unittest.TestCase):
    def run_scenario(self, draws, winners=None, participants=None, update_counts=None, result=None):
        conn = FakeConnection()
        winners = winners or {}
        participants = participants or {}
        update_counts = update_counts or {}
        winner_calls = []
        update_calls = []

        def winner_lookup(_conn, draw_id, number):
            winner_calls.append((draw_id, number))
            return winners.get(draw_id, (None, None, None))

        def update_draw(_conn, draw_id, number, user_id, winner_name):
            update_calls.append((draw_id, number, user_id, winner_name))
            result = update_counts.get(draw_id, 1)
            if isinstance(result, Exception):
                raise result
            return result

        communication_commit_counts = []

        def record_event_commit(**_kwargs):
            communication_commit_counts.append(conn.commit_count)
            return {"ok": True}

        event_mock = Mock(side_effect=record_event_commit)
        open_mock = Mock()
        lotomania_mock = Mock(return_value=result or lotomania())
        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "COMMIT", True))
            stack.enter_context(patch.object(main, "db", return_value=conn))
            stack.enter_context(patch.object(main, "get_pending_draws", return_value=draws))
            stack.enter_context(patch.object(main, "get_last_lotomania_result", lotomania_mock))
            stack.enter_context(patch.object(main, "winner_for_number", side_effect=winner_lookup))
            stack.enter_context(patch.object(main, "set_draw_sorteado_any_status", side_effect=update_draw))
            stack.enter_context(patch.object(main, "get_participants", side_effect=lambda _conn, draw_id: participants.get(draw_id, [])))
            stack.enter_context(patch.object(main, "get_draw_label", side_effect=lambda _conn, draw_id: f"Sorteio #{draw_id}"))
            stack.enter_context(patch.object(main, "notify_push_automation_event", event_mock))
            stack.enter_context(patch.object(main, "send_winner_email"))
            stack.enter_context(patch.object(main, "send_draw_closed_admin"))
            stack.enter_context(patch.object(main, "send_loser_email"))
            stack.enter_context(patch.object(main, "_run_push_automation_scan_safely"))
            stack.enter_context(patch.object(main, "open_new_draw", open_mock))
            result_code = main.run()

        return {
            "result_code": result_code,
            "conn": conn,
            "winner_calls": winner_calls,
            "update_calls": update_calls,
            "events": event_mock,
            "open_new_draw": open_mock,
            "lotomania": lotomania_mock,
            "communication_commit_counts": communication_commit_counts,
        }

    def test_pending_query_selects_all_supported_closed_draws_without_limit(self):
        cursor = RecordingCursor(rows=[])
        conn = CursorConnection(cursor)
        self.assertEqual(main.get_pending_draws(conn), [])
        normalized_sql = " ".join(cursor.sql.split())
        self.assertIn("d.status = 'closed'", normalized_sql)
        self.assertIn("d.realized_at IS NULL", normalized_sql)
        self.assertIn("COALESCE(d.draw_type, 'principal') IN", normalized_sql)
        self.assertIn("'principal', 'adicional', 'secundario'", normalized_sql)
        self.assertIn("ORDER BY d.closed_at ASC NULLS LAST, d.id ASC", normalized_sql)
        self.assertNotIn("LIMIT 1", normalized_sql.upper())

    def test_safe_update_revalidates_closed_and_unrealized(self):
        cursor = RecordingCursor(rowcount=1)
        conn = CursorConnection(cursor)
        updated = main.set_draw_sorteado_any_status(conn, 10, 33, 7, "Cliente")
        normalized_sql = " ".join(cursor.sql.split())
        self.assertEqual(updated, 1)
        self.assertIn("WHERE id = %s AND status = 'closed' AND realized_at IS NULL", normalized_sql)
        self.assertEqual(cursor.params, (33, 7, "Cliente", 10))

    def test_winner_lookup_is_scoped_to_current_draw_id(self):
        cursor = SequenceCursor([
            {"n": 33, "status": "sold", "reservation_id": "reservation-133"},
            {"user_id": 7, "name": "Cliente A", "email": "a@example.com"},
        ])
        conn = CursorConnection(cursor)
        winner = main.winner_for_number(conn, 133, 33)
        self.assertEqual(winner, (7, "Cliente A", "a@example.com"))
        self.assertIn("WHERE draw_id = %s AND n = %s", cursor.executions[0][0])
        self.assertEqual(cursor.executions[0][1], (133, 33))
        self.assertIn("WHERE r.id = %s AND r.draw_id = %s", cursor.executions[1][0])
        self.assertEqual(cursor.executions[1][1], ("reservation-133", 133))

    def test_principal_closed_is_processed_without_opening_new_draw(self):
        outcome = self.run_scenario(
            [draw(133)],
            winners={133: (7, "Cliente A", "a@example.com")},
        )
        self.assertEqual(outcome["result_code"], 0)
        self.assertEqual(outcome["winner_calls"], [(133, 33)])
        self.assertEqual(outcome["conn"].commit_count, 1)
        outcome["open_new_draw"].assert_not_called()
        event = outcome["events"].call_args.kwargs
        self.assertEqual(event["event_key"], "WINNER_DEFINED")
        self.assertEqual(event["reference_key"], "draw:133:winner_defined")
        self.assertFalse(event["metadata"]["is_additional_draw"])

    def test_additional_closed_is_processed_while_principal_open_is_untouched(self):
        principal_open = draw(133, status="open")
        additional = draw(134, draw_type="adicional", product_name="Adicional Moto")
        outcome = self.run_scenario(
            [additional],
            winners={134: (8, "Cliente B", "b@example.com")},
        )
        self.assertEqual(outcome["winner_calls"], [(134, 33)])
        self.assertNotIn(principal_open["id"], [item[0] for item in outcome["update_calls"]])
        event = outcome["events"].call_args.kwargs
        self.assertEqual(event["reference_key"], "additional_draw:134:winner_defined")
        self.assertEqual(event["reference_type"], "additional_draw")
        self.assertEqual(event["metadata"]["draw_type"], "adicional")
        self.assertEqual(event["metadata"]["product_name"], "Adicional Moto")
        self.assertTrue(event["metadata"]["is_additional_draw"])

    def test_principal_closed_is_processed_while_additional_open_is_untouched(self):
        additional_open = draw(134, draw_type="adicional", status="open")
        outcome = self.run_scenario([draw(133)])
        self.assertEqual(outcome["winner_calls"], [(133, 33)])
        self.assertNotIn(additional_open["id"], [item[0] for item in outcome["update_calls"]])

    def test_principal_and_additional_use_same_result_but_resolve_buyers_by_draw(self):
        outcome = self.run_scenario(
            [draw(133), draw(134, draw_type="adicional")],
            winners={
                133: (7, "Cliente A", "a@example.com"),
                134: (8, "Cliente B", "b@example.com"),
            },
        )
        self.assertEqual(outcome["winner_calls"], [(133, 33), (134, 33)])
        self.assertEqual(outcome["update_calls"][0][2], 7)
        self.assertEqual(outcome["update_calls"][1][2], 8)
        self.assertEqual(outcome["conn"].commit_count, 2)
        self.assertEqual(outcome["events"].call_count, 2)
        self.assertEqual(outcome["communication_commit_counts"], [1, 2])
        outcome["lotomania"].assert_called_once_with()

    def test_two_additional_draws_are_both_processed(self):
        outcome = self.run_scenario([
            draw(134, draw_type="adicional"),
            draw(135, draw_type="secundario"),
        ])
        self.assertEqual(outcome["winner_calls"], [(134, 33), (135, 33)])
        self.assertEqual(outcome["conn"].commit_count, 2)
        references = [item.kwargs["reference_key"] for item in outcome["events"].call_args_list]
        self.assertEqual(references, [
            "additional_draw:134:winner_defined",
            "additional_draw:135:winner_defined",
        ])

    def test_number_without_buyer_is_saved_without_choosing_another_number(self):
        outcome = self.run_scenario([draw(133)], winners={133: (None, None, None)})
        self.assertEqual(outcome["winner_calls"], [(133, 33)])
        self.assertEqual(outcome["update_calls"], [(133, 33, None, None)])
        event_metadata = outcome["events"].call_args.kwargs["metadata"]
        self.assertEqual(event_metadata["winner_number"], 33)
        self.assertIsNone(event_metadata["winner_user_id"])

    def test_api_unavailable_changes_no_draw(self):
        conn = FakeConnection()
        update_mock = Mock()
        with patch.object(main, "db", return_value=conn), \
             patch.object(main, "get_pending_draws", return_value=[draw(133)]), \
             patch.object(main, "get_last_lotomania_result", side_effect=RuntimeError("offline")), \
             patch.object(main, "set_draw_sorteado_any_status", update_mock), \
             patch.object(main, "_run_push_automation_scan_safely"):
            self.assertEqual(main.run(), 1)
        update_mock.assert_not_called()
        self.assertEqual(conn.commit_count, 0)

    def test_incomplete_payload_is_rejected_without_real_api(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"numero": 2891, "listaDezenas": []}
        with patch.object(main.requests, "get", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "Sem dezenas"):
                main.get_last_lotomania_result()

        response.json.return_value = {"listaDezenas": ["100"]}
        with patch.object(main.requests, "get", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "fora do intervalo"):
                main.get_last_lotomania_result()

    def test_result_before_close_skips_only_that_draw(self):
        outcome = self.run_scenario(
            [draw(133, closed_day=16), draw(134, draw_type="adicional", closed_day=14)],
            result=lotomania(result_day=15),
        )
        self.assertEqual(outcome["winner_calls"], [(134, 33)])
        self.assertEqual(outcome["update_calls"], [(134, 33, None, None)])
        self.assertEqual(outcome["conn"].commit_count, 1)

    def test_repeated_execution_does_not_duplicate_event_or_email(self):
        first_conn = FakeConnection()
        second_conn = FakeConnection()
        event_mock = Mock(return_value={"ok": True})
        with patch.object(main, "COMMIT", True), \
             patch.object(main, "db", side_effect=[first_conn, second_conn]), \
             patch.object(main, "get_pending_draws", side_effect=[[draw(133)], []]), \
             patch.object(main, "get_last_lotomania_result", return_value=lotomania()), \
             patch.object(main, "winner_for_number", return_value=(7, "Cliente A", "a@example.com")), \
             patch.object(main, "get_participants", return_value=[]), \
             patch.object(main, "get_draw_label", return_value="Principal"), \
             patch.object(main, "set_draw_sorteado_any_status", return_value=1), \
             patch.object(main, "notify_push_automation_event", event_mock), \
             patch.object(main, "send_winner_email") as winner_email, \
             patch.object(main, "send_draw_closed_admin"), \
             patch.object(main, "_run_push_automation_scan_safely"):
            self.assertEqual(main.run(), 0)
            self.assertEqual(main.run(), 0)
        self.assertEqual(event_mock.call_count, 1)
        self.assertEqual(winner_email.call_count, 1)

    def test_rowcount_zero_sends_no_communication(self):
        outcome = self.run_scenario([draw(133)], update_counts={133: 0})
        self.assertEqual(outcome["conn"].commit_count, 0)
        self.assertGreaterEqual(outcome["conn"].rollback_count, 1)
        outcome["events"].assert_not_called()

    def test_failure_in_one_draw_rolls_back_only_it_and_continues(self):
        outcome = self.run_scenario(
            [draw(133), draw(134, draw_type="adicional")],
            update_counts={133: RuntimeError("draw update failed")},
        )
        self.assertEqual(outcome["winner_calls"], [(133, 33), (134, 33)])
        self.assertEqual(outcome["conn"].commit_count, 1)
        self.assertGreaterEqual(outcome["conn"].rollback_count, 1)
        self.assertEqual(outcome["events"].call_count, 1)
        self.assertEqual(
            outcome["events"].call_args.kwargs["reference_key"],
            "additional_draw:134:winner_defined",
        )

    def test_email_failure_after_commit_does_not_undo_or_stop_other_draw(self):
        conn = FakeConnection()
        event_mock = Mock(return_value={"ok": True})
        with patch.object(main, "COMMIT", True), \
             patch.object(main, "db", return_value=conn), \
             patch.object(main, "get_pending_draws", return_value=[draw(133), draw(134, draw_type="adicional")]), \
             patch.object(main, "get_last_lotomania_result", return_value=lotomania()), \
             patch.object(main, "winner_for_number", return_value=(7, "Cliente", "winner@example.com")), \
             patch.object(main, "get_participants", return_value=[]), \
             patch.object(main, "get_draw_label", return_value="Sorteio"), \
             patch.object(main, "set_draw_sorteado_any_status", return_value=1), \
             patch.object(main, "notify_push_automation_event", event_mock), \
             patch.object(main, "send_winner_email", side_effect=RuntimeError("smtp offline")), \
             patch.object(main, "send_draw_closed_admin"), \
             patch.object(main, "_run_push_automation_scan_safely"):
            self.assertEqual(main.run(), 0)
        self.assertEqual(conn.commit_count, 2)
        self.assertEqual(event_mock.call_count, 2)

    def test_lotomania_payload_validation_and_date_parsing(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "numero": "2891",
            "dataApuracao": "15/07/2026",
            "listaDezenas": ["01", "07", "33"],
        }
        with patch.object(main.requests, "get", return_value=response):
            result = main.get_last_lotomania_result()
        self.assertEqual(result, lotomania())
        response.raise_for_status.assert_called_once_with()

    def test_result_path_has_no_automatic_new_draw_publication(self):
        outcome = self.run_scenario([draw(133)])
        outcome["open_new_draw"].assert_not_called()
        self.assertNotIn("NEW_DRAW_PUBLISHED", main._process_pending_draw.__code__.co_names)


if __name__ == "__main__":
    unittest.main()
