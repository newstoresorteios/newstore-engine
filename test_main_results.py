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


def draw(
    draw_id,
    draw_type="principal",
    status="closed",
    closed_day=14,
    product_name=None,
    closed_at=None,
):
    return {
        "id": draw_id,
        "status": status,
        "draw_type": draw_type,
        "opened_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        "closed_at": closed_at or datetime(2026, 7, closed_day, 20, tzinfo=timezone.utc),
        "product_name": product_name,
    }


def lotomania(number=33, result_day=15, contest_number=2891, previous_contest_number=2890):
    return {
        "winner_number": number,
        "contest_number": contest_number,
        "previous_contest_number": previous_contest_number,
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
        draws_by_id = {int(item["id"]): item for item in draws}

        def resolve_result(_draw, latest_result, **_kwargs):
            return latest_result

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "COMMIT", True))
            stack.enter_context(patch.object(main, "db", return_value=conn))
            stack.enter_context(patch.object(main, "get_pending_draws", return_value=draws))
            stack.enter_context(patch.object(main, "get_last_lotomania_result", lotomania_mock))
            stack.enter_context(patch.object(
                main,
                "resolve_first_eligible_lotomania_result",
                side_effect=resolve_result,
            ))
            stack.enter_context(patch.object(
                main,
                "lock_pending_draw_for_result",
                side_effect=lambda _conn, draw_id: draws_by_id.get(draw_id),
            ))
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

    def test_result_lock_revalidates_closed_and_unrealized_before_winner_lookup(self):
        current_draw = {
            "id": 10,
            "status": "closed",
            "realized_at": None,
            "closed_at": datetime(2026, 7, 14, 20, tzinfo=timezone.utc),
        }
        cursor = SequenceCursor([current_draw])
        conn = CursorConnection(cursor)
        self.assertEqual(main.lock_pending_draw_for_result(conn, 10), current_draw)
        normalized_sql, params = cursor.executions[0]
        self.assertIn("SELECT id, status, realized_at, closed_at FROM draws", normalized_sql)
        self.assertIn("status = 'closed'", normalized_sql)
        self.assertIn("realized_at IS NULL", normalized_sql)
        self.assertIn("FOR UPDATE", normalized_sql)
        self.assertEqual(params, (10,))

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
            with self.assertRaisesRegex(RuntimeError, "dezenasSorteadasOrdemSorteio"):
                main.get_last_lotomania_result()

        response.json.return_value = {
            "numero": 2891,
            "numeroConcursoAnterior": 2890,
            "dataApuracao": "15/07/2026",
            "dezenasSorteadasOrdemSorteio": ["100"],
        }
        with patch.object(main.requests, "get", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "fora do intervalo"):
                main.get_last_lotomania_result()

    def test_missing_real_draw_order_does_not_finalize_draw(self):
        conn = FakeConnection()
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "numero": 2891,
            "numeroConcursoAnterior": 2890,
            "dataApuracao": "15/07/2026",
            "listaDezenas": ["02", "19", "45", "73"],
        }
        update_mock = Mock()
        winner_mock = Mock()
        with patch.object(main, "COMMIT", True), \
             patch.object(main, "db", return_value=conn), \
             patch.object(main, "get_pending_draws", return_value=[draw(133)]), \
             patch.object(main.requests, "get", return_value=response), \
             patch.object(main, "winner_for_number", winner_mock), \
             patch.object(main, "set_draw_sorteado_any_status", update_mock), \
             patch.object(main, "_run_push_automation_scan_safely"):
            self.assertEqual(main.run(), 1)
        winner_mock.assert_not_called()
        update_mock.assert_not_called()
        self.assertEqual(conn.commit_count, 0)

    def test_same_day_contest_is_eligible_when_draw_closed_before_draw_time(self):
        closed_draw = draw(
            133,
            closed_at=datetime(2026, 7, 15, 20, 30, tzinfo=main.BRASILIA_TZ),
        )
        same_day = lotomania(
            number=19,
            result_day=15,
            contest_number=2891,
            previous_contest_number=2890,
        )
        previous = lotomania(
            number=73,
            result_day=13,
            contest_number=2890,
            previous_contest_number=2889,
        )
        fetcher = Mock(return_value=previous)
        resolved = main.resolve_first_eligible_lotomania_result(
            closed_draw,
            same_day,
            result_fetcher=fetcher,
        )
        self.assertEqual(resolved["contest_number"], 2891)
        self.assertEqual(resolved["winner_number"], 19)
        fetcher.assert_called_once_with(2890)

    def test_same_day_contest_is_rejected_when_draw_closed_after_draw_time(self):
        closed_draw = draw(
            133,
            closed_at=datetime(2026, 7, 15, 22, 0, tzinfo=main.BRASILIA_TZ),
        )
        same_day = lotomania(
            number=19,
            result_day=15,
            contest_number=2891,
            previous_contest_number=2890,
        )
        self.assertIsNone(
            main.resolve_first_eligible_lotomania_result(
                closed_draw,
                same_day,
                result_fetcher=Mock(),
            )
        )

        next_contest = lotomania(
            number=45,
            result_day=17,
            contest_number=2892,
            previous_contest_number=2891,
        )
        resolved = main.resolve_first_eligible_lotomania_result(
            closed_draw,
            next_contest,
            result_fetcher=Mock(return_value=same_day),
        )
        self.assertEqual(resolved["contest_number"], 2892)
        self.assertEqual(resolved["winner_number"], 45)

    def test_delayed_engine_uses_first_eligible_contest_not_latest(self):
        closed_draw = draw(
            133,
            closed_at=datetime(2026, 7, 12, 12, 0, tzinfo=main.BRASILIA_TZ),
        )
        latest = lotomania(
            number=73,
            result_day=15,
            contest_number=2892,
            previous_contest_number=2891,
        )
        first_eligible = lotomania(
            number=19,
            result_day=13,
            contest_number=2891,
            previous_contest_number=2890,
        )
        before_close = lotomania(
            number=2,
            result_day=10,
            contest_number=2890,
            previous_contest_number=2889,
        )
        results = {
            2891: first_eligible,
            2890: before_close,
        }
        fetcher = Mock(side_effect=lambda contest_number: results[contest_number])
        resolved = main.resolve_first_eligible_lotomania_result(
            closed_draw,
            latest,
            result_fetcher=fetcher,
        )
        self.assertEqual(resolved["contest_number"], 2891)
        self.assertEqual(resolved["winner_number"], 19)
        self.assertNotEqual(resolved["contest_number"], 2892)
        self.assertEqual(fetcher.call_args_list, [call(2891), call(2890)])

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
             patch.object(main, "resolve_first_eligible_lotomania_result", side_effect=lambda _draw, latest, **_kwargs: latest), \
             patch.object(main, "lock_pending_draw_for_result", return_value=draw(133)), \
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

    def test_second_processing_after_realization_stops_at_lock(self):
        conn = FakeConnection()
        pending_draw = draw(133)
        winner_mock = Mock(return_value=(7, "Cliente A", "a@example.com"))
        update_mock = Mock(return_value=1)
        communications_mock = Mock()
        with patch.object(main, "COMMIT", True), \
             patch.object(main, "lock_pending_draw_for_result", side_effect=[pending_draw, None]), \
             patch.object(main, "winner_for_number", winner_mock), \
             patch.object(main, "set_draw_sorteado_any_status", update_mock), \
             patch.object(main, "get_draw_label", return_value="Principal"), \
             patch.object(main, "get_participants", return_value=[]), \
             patch.object(main, "_send_result_communications", communications_mock):
            self.assertTrue(main._process_pending_draw(conn, pending_draw, lotomania()))
            self.assertFalse(main._process_pending_draw(conn, pending_draw, lotomania()))
        winner_mock.assert_called_once_with(conn, 133, 33)
        update_mock.assert_called_once()
        communications_mock.assert_called_once()
        self.assertEqual(conn.commit_count, 1)

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
             patch.object(main, "resolve_first_eligible_lotomania_result", side_effect=lambda _draw, latest, **_kwargs: latest), \
             patch.object(main, "lock_pending_draw_for_result", side_effect=[draw(133), draw(134, draw_type="adicional")]), \
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

    def test_winner_smtp_failure_is_reported_and_other_emails_continue(self):
        pending_draw = draw(133)
        loser = {"id": 8, "name": "Cliente B", "email": "b@example.com"}
        with patch.object(main, "notify_push_automation_event", return_value={"ok": True}), \
             patch.object(main, "send_winner_email", side_effect=RuntimeError("smtp offline")), \
             patch.object(main, "send_draw_closed_admin", return_value=True) as admin_email, \
             patch.object(main, "send_loser_email", return_value=True) as loser_email:
            summary = main._send_result_communications(
                pending_draw,
                "Principal",
                33,
                7,
                "Cliente A",
                "a@example.com",
                [loser],
            )
        self.assertEqual(summary["winner_email"], "failed")
        self.assertEqual(summary["admin_email"], "sent")
        self.assertEqual(summary["loser_emails"], {"sent": 1, "failed": 0})
        admin_email.assert_called_once()
        loser_email.assert_called_once()

    def test_one_loser_smtp_failure_does_not_stop_remaining_recipients(self):
        pending_draw = draw(133)
        losers = [
            {"id": 8, "name": "Cliente B", "email": "b@example.com"},
            {"id": 9, "name": "Cliente C", "email": "c@example.com"},
            {"id": 10, "name": "Cliente D", "email": "d@example.com"},
        ]
        loser_email = Mock(side_effect=[True, RuntimeError("smtp offline"), True])
        with patch.object(main, "notify_push_automation_event", return_value={"ok": True}), \
             patch.object(main, "send_winner_email", return_value=True), \
             patch.object(main, "send_draw_closed_admin", return_value=True), \
             patch.object(main, "send_loser_email", loser_email):
            summary = main._send_result_communications(
                pending_draw,
                "Principal",
                33,
                7,
                "Cliente A",
                "a@example.com",
                losers,
            )
        self.assertEqual(summary["winner_email"], "sent")
        self.assertEqual(summary["admin_email"], "sent")
        self.assertEqual(summary["loser_emails"], {"sent": 2, "failed": 1})
        self.assertEqual(loser_email.call_count, 3)

    def test_lotomania_uses_real_draw_order_instead_of_numeric_order(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "numero": "2891",
            "numeroConcursoAnterior": 2890,
            "dataApuracao": "15/07/2026",
            "dezenasSorteadasOrdemSorteio": ["45", "02", "73", "19"],
            "listaDezenas": ["02", "19", "45", "73"],
        }
        with patch.object(main.requests, "get", return_value=response):
            result = main.get_last_lotomania_result()
        self.assertEqual(result["winner_number"], 19)
        self.assertNotEqual(result["winner_number"], 73)
        self.assertEqual(result["contest_number"], 2891)
        self.assertEqual(result["previous_contest_number"], 2890)
        self.assertEqual(result["result_date"], date(2026, 7, 15))
        response.raise_for_status.assert_called_once_with()

    def test_specific_contest_query_uses_existing_caixa_endpoint(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "numero": 2890,
            "numeroConcursoAnterior": 2889,
            "dataApuracao": "13/07/2026",
            "dezenasSorteadasOrdemSorteio": ["02", "45", "19"],
            "listaDezenas": ["02", "19", "45"],
        }
        with patch.object(main.requests, "get", return_value=response) as request_mock:
            result = main.get_lotomania_result(2890)
        self.assertEqual(result["contest_number"], 2890)
        self.assertEqual(result["winner_number"], 19)
        request_mock.assert_called_once_with(
            f"{main.LOT_ENDPOINT.rstrip('/')}/2890",
            timeout=20,
            headers={"Accept": "application/json"},
        )

    def test_result_path_has_no_automatic_new_draw_publication(self):
        outcome = self.run_scenario([draw(133)])
        outcome["open_new_draw"].assert_not_called()
        self.assertNotIn("NEW_DRAW_PUBLISHED", main._process_pending_draw.__code__.co_names)


if __name__ == "__main__":
    unittest.main()
