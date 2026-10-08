import io
import os
import unittest
from contextlib import contextmanager
from contextlib import redirect_stdout
from datetime import date, datetime, timezone
from unittest.mock import Mock, patch

import main
import test_email_automation_scan as scan_base
from email_automation_events import (
    notify_email_automation_event,
    result_email_event_keys,
    result_email_reference_key,
)
from email_automation_scan import (
    EMAIL_RESULT_EXPIRY_SWEEP_HOURS,
    EMAIL_RESULT_LOOKBACK_HOURS,
    run_email_automation_scan,
)

REALIZED = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


def result_row(draw_id, draw_type="principal", winner_user_id=11, winner_number=7):
    return {
        "id": draw_id,
        "status": "sorteado",
        "draw_type": draw_type,
        "product_name": f"Premio {draw_id}",
        "draw_name": f"Premio {draw_id}",
        "winner_number": winner_number,
        "winner_user_id": winner_user_id,
        "realized_at": REALIZED,
    }


class ResultConnection(scan_base.FakeConnection):
    """FakeConnection do scanner + linhas da consulta de resultados."""

    def __init__(self, result_draws=None, **kwargs):
        super().__init__(**kwargs)
        self.result_draws = result_draws or []

    def cursor(self):
        conn = self
        base_cursor = scan_base.FakeCursor(self)
        original = base_cursor.execute

        def execute(sql, params=()):
            if "realized_at IS NOT NULL" in sql and "status = 'sorteado'" in sql:
                conn.executions.append((" ".join(sql.split()), params))
                base_cursor.rows = conn.result_draws
                return
            original(sql, params)

        base_cursor.execute = execute
        return base_cursor


RESULT_ENV = {"EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM": "2026-10-09T00:00:00Z"}


def scan(conn, backend=None, env=None):
    notify = Mock(side_effect=backend or (lambda **_kw: {"ok": True, "status": 200, "data": {"ok": True, "sent": 1, "failed": 0, "skipped": 0}}))
    full_env = {**RESULT_ENV, **(env or {})}
    with patch.dict("os.environ", full_env, clear=True), \
         patch("email_automation_scan.notify_email_automation_event", notify), \
         redirect_stdout(io.StringIO()):
        summary = run_email_automation_scan(conn)
    return summary, notify


def keys(notify):
    return sorted((c.kwargs["event_key"], c.kwargs["reference_type"], c.kwargs["reference_key"]) for c in notify.call_args_list)


class ScannerResultEventsTest(unittest.TestCase):
    def test_principal_with_winner_publishes_winner_participant_and_admin(self):
        summary, notify = scan(ResultConnection([result_row(150)]))
        self.assertEqual(summary["result_checked"], 1)
        self.assertEqual(keys(notify), [
            ("EMAIL_RESULT_ADMIN", "draw", "draw:150:result_admin_email"),
            ("EMAIL_RESULT_PARTICIPANT", "draw", "draw:150:result_participant_email"),
            ("EMAIL_RESULT_WINNER", "draw", "draw:150:result_winner_email"),
        ])
        self.assertTrue(summary["ok"])

    def test_additional_and_secundario_use_additional_draw_references(self):
        _summary, notify = scan(ResultConnection([result_row(151, "adicional"), result_row(152, "secundario")]))
        references = {k[2] for k in keys(notify)}
        self.assertEqual(references, {
            f"additional_draw:{i}:{s}" for i in (151, 152)
            for s in ("result_winner_email", "result_participant_email", "result_admin_email")
        })
        self.assertTrue(all(k[1] == "additional_draw" for k in keys(notify)))

    def test_without_buyer_only_the_admin_is_notified(self):
        _summary, notify = scan(ResultConnection([result_row(153, winner_user_id=None)]))
        self.assertEqual(keys(notify), [("EMAIL_RESULT_ADMIN", "draw", "draw:153:result_admin_email")])

    def test_same_number_in_different_draws_has_independent_keys(self):
        _summary, notify = scan(ResultConnection([result_row(150, winner_number=7), result_row(151, "adicional", winner_number=7)]))
        references = [k[2] for k in keys(notify)]
        self.assertEqual(len(references), len(set(references)))
        self.assertEqual(len(references), 6)

    def test_reexecution_produces_identical_reference_keys_for_backend_dedupe(self):
        conn = ResultConnection([result_row(150)])
        _s1, first = scan(conn)
        _s2, second = scan(conn)
        self.assertEqual(keys(first), keys(second))

    def test_old_draws_are_outside_the_query_window_and_publish_nothing(self):
        conn = ResultConnection([])  # a consulta so devolve realized_at dentro da janela
        summary, notify = scan(conn)
        notify.assert_not_called()
        self.assertEqual(summary["result_checked"], 0)
        query = next((sql, p) for sql, p in conn.executions if "realized_at IS NOT NULL" in sql and "status = 'sorteado'" in sql)
        self.assertIn("realized_at >= NOW()", query[0])
        self.assertEqual(query[1][0], EMAIL_RESULT_LOOKBACK_HOURS + EMAIL_RESULT_EXPIRY_SWEEP_HOURS)

    def test_result_lookback_is_configurable(self):
        conn = ResultConnection([])
        scan(conn, env={"EMAIL_AUTOMATION_RESULT_LOOKBACK_HOURS": "48"})
        query = next((sql, p) for sql, p in conn.executions if "realized_at IS NOT NULL" in sql and "status = 'sorteado'" in sql)
        self.assertEqual(query[1][0], 48 + EMAIL_RESULT_EXPIRY_SWEEP_HOURS)

    def test_backend_disabled_or_skipped_is_not_a_failure(self):
        backend = lambda **_kw: {"ok": True, "status": 200, "data": {"ok": True, "status": "disabled", "sent": 0, "failed": 0, "skipped": 0}}
        summary, _notify = scan(ResultConnection([result_row(150)]), backend=backend)
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["failed"], 0)

    def test_failed_delivery_is_reported_but_other_events_continue(self):
        def backend(**kwargs):
            if kwargs["event_key"] == "EMAIL_RESULT_PARTICIPANT":
                return {"ok": True, "status": 200, "data": {"ok": True, "status": "partial_failure", "sent": 1, "failed": 2, "skipped": 0}}
            return {"ok": True, "status": 200, "data": {"ok": True, "sent": 1, "failed": 0, "skipped": 0}}
        summary, notify = scan(ResultConnection([result_row(150)]), backend=backend)
        self.assertEqual(notify.call_count, 3)
        self.assertFalse(summary["ok"])
        self.assertEqual(summary["failed"], 2)

    def test_dry_run_publishes_nothing(self):
        summary, notify = scan(ResultConnection([result_row(150)]), env={"EMAIL_AUTOMATION_DRY_RUN": "true"})
        notify.assert_not_called()
        self.assertTrue(summary["ok"])


class ResultEffectiveFromGuardTest(unittest.TestCase):
    """Segunda trava (engine): sorteios historicos nunca geram evento de resultado."""

    def run_scan(self, env, rows=None):
        conn = ResultConnection(rows if rows is not None else [result_row(150)])
        notify = Mock(return_value={"ok": True})
        with patch.dict("os.environ", env, clear=True):
            with patch("email_automation_scan.notify_email_automation_event", notify):
                with redirect_stdout(io.StringIO()):
                    summary = run_email_automation_scan(conn)
        queried = any(
            "realized_at IS NOT NULL" in sql and "status = 'sorteado'" in sql
            for sql, _ in conn.executions
        )
        return summary, notify, conn, queried

    def test_without_the_variable_results_are_disabled_and_not_even_queried(self):
        summary, notify, _conn, queried = self.run_scan({})
        self.assertTrue(summary["result_disabled"])
        self.assertEqual(summary["result_checked"], 0)
        self.assertFalse(queried)
        published = [c.kwargs["event_key"] for c in notify.call_args_list]
        self.assertFalse([key for key in published if key.startswith("EMAIL_RESULT")])

    def test_invalid_value_is_treated_as_disabled(self):
        for value in ("", "   ", "ontem", "2026-13-45"):
            with self.subTest(value=value):
                summary, _notify, _conn, queried = self.run_scan(
                    {"EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM": value}
                )
                self.assertTrue(summary["result_disabled"])
                self.assertFalse(queried)

    def test_cutoff_is_sent_to_the_query_so_older_draws_are_excluded_by_sql(self):
        env = {"EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM": "2026-10-09T03:00:00-03:00"}
        _summary, _notify, conn, queried = self.run_scan(env)
        self.assertTrue(queried)
        sql, params = next(
            (q, p) for q, p in conn.executions
            if "realized_at IS NOT NULL" in q and "status = 'sorteado'" in q
        )
        self.assertIn("d.realized_at >= %s", sql)
        self.assertEqual(params[0], EMAIL_RESULT_LOOKBACK_HOURS + EMAIL_RESULT_EXPIRY_SWEEP_HOURS)
        self.assertEqual(params[1], datetime(2026, 10, 9, 6, 0, tzinfo=timezone.utc))

    def test_other_event_families_are_unaffected_when_results_are_disabled(self):
        closed = {
            "id": 9, "status": "closed", "draw_type": "principal", "product_name": "X",
            "draw_name": "X", "closed_at": datetime(2026, 10, 8, tzinfo=timezone.utc),
        }
        conn = ResultConnection([result_row(150)], closed_draws=[closed])
        notify = Mock(return_value={"ok": True})
        with patch.dict("os.environ", {}, clear=True):
            with patch("email_automation_scan.notify_email_automation_event", notify):
                with redirect_stdout(io.StringIO()):
                    summary = run_email_automation_scan(conn)
        self.assertEqual(summary["closed_checked"], 1)
        self.assertEqual([c.kwargs["event_key"] for c in notify.call_args_list], ["DRAW_CLOSED"])


class SharedReferenceKeysTest(unittest.TestCase):
    def test_keys_are_shared_between_scanner_and_main(self):
        self.assertEqual(result_email_reference_key("draw", 9, "EMAIL_RESULT_WINNER"), "draw:9:result_winner_email")
        self.assertEqual(result_email_reference_key("additional_draw", 9, "EMAIL_RESULT_ADMIN"), "additional_draw:9:result_admin_email")
        self.assertEqual(result_email_event_keys(True), ["EMAIL_RESULT_WINNER", "EMAIL_RESULT_PARTICIPANT", "EMAIL_RESULT_ADMIN"])
        self.assertEqual(result_email_event_keys(False), ["EMAIL_RESULT_ADMIN"])

    def test_publisher_forwards_contest_metadata_inside_metadata(self):
        response = Mock(status_code=200, ok=True)
        response.json.return_value = {"ok": True}
        env = {"BACKEND_INTERNAL_API_BASE": "https://backend.test", "PUSH_INTERNAL_EVENTS_TOKEN": "token"}
        with patch.dict("os.environ", env, clear=True), patch("email_automation_events.requests.post", return_value=response) as post:
            notify_email_automation_event(
                event_key="EMAIL_RESULT_ADMIN",
                reference_type="draw",
                reference_key="draw:1:result_admin_email",
                metadata={"draw_id": 1, "contest_number": 2990, "result_date": "2026-10-09"},
            )
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["metadata"]["contest_number"], 2990)
        self.assertEqual(payload["metadata"]["result_date"], "2026-10-09")
        self.assertEqual(post.call_args.kwargs["headers"]["Idempotency-Key"], "draw:1:result_admin_email")


@contextmanager
def backend_owner(active, cutoff="2026-01-01T00:00:00Z"):
    """Liga/desliga o corte (a UNICA chave de ativacao) durante o bloco."""
    saved = os.environ.get("EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM")
    if active:
        os.environ["EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM"] = cutoff
    else:
        os.environ.pop("EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM", None)
    try:
        yield
    finally:
        if saved is None:
            os.environ.pop("EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM", None)
        else:
            os.environ["EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM"] = saved


def draw_dict(draw_id=150, draw_type="principal"):
    return {"id": draw_id, "draw_type": draw_type, "status": "closed", "product_name": "Premio"}


def communications(flag, winner_user_id=11, notify_side_effect=None, loser_list=None, draw=None):
    smtp = {name: Mock(return_value=True) for name in ("send_winner_email", "send_draw_closed_admin", "send_loser_email")}
    notify_email = Mock(side_effect=notify_side_effect or (lambda **_kw: {"ok": True}))
    push = Mock(return_value={"ok": True})
    with backend_owner(flag), \
         patch.object(main, "notify_email_automation_event", notify_email), \
         patch.object(main, "notify_push_automation_event", push), \
         patch.multiple(main, **smtp), redirect_stdout(io.StringIO()):
        summary = main._send_result_communications(
            draw or draw_dict(),
            "Premio",
            7,
            winner_user_id,
            "Vencedora" if winner_user_id else None,
            "v@example.test" if winner_user_id else None,
            loser_list if loser_list is not None else [{"id": 2, "name": "Ana", "email": "ana@example.test"}],
            contest_number=2990,
            result_date=date(2026, 10, 9),
        )
    return summary, smtp, notify_email, push


class MainSingleOwnerTest(unittest.TestCase):
    def test_via_backend_engine_sends_no_smtp_and_publishes_three_events_with_contest(self):
        summary, smtp, notify_email, push = communications(True)
        for mock in smtp.values():
            mock.assert_not_called()
        self.assertEqual(summary["mode"], "backend")
        events = sorted((c.kwargs["event_key"], c.kwargs["reference_key"]) for c in notify_email.call_args_list)
        self.assertEqual(events, [
            ("EMAIL_RESULT_ADMIN", "draw:150:result_admin_email"),
            ("EMAIL_RESULT_PARTICIPANT", "draw:150:result_participant_email"),
            ("EMAIL_RESULT_WINNER", "draw:150:result_winner_email"),
        ])
        for call in notify_email.call_args_list:
            self.assertEqual(call.kwargs["metadata"]["contest_number"], 2990)
            self.assertEqual(call.kwargs["metadata"]["result_date"], "2026-10-09")
            self.assertEqual(call.kwargs["reference_type"], "draw")
        push.assert_called_once()  # evento de push (WINNER_DEFINED) inalterado

    def test_via_backend_additional_draw_uses_additional_references(self):
        _s, _smtp, notify_email, _push = communications(True, draw=draw_dict(151, "adicional"))
        self.assertTrue(all(c.kwargs["reference_key"].startswith("additional_draw:151:") for c in notify_email.call_args_list))

    def test_via_backend_without_buyer_only_admin_event_and_no_smtp(self):
        _s, smtp, notify_email, _push = communications(True, winner_user_id=None)
        self.assertEqual([c.kwargs["event_key"] for c in notify_email.call_args_list], ["EMAIL_RESULT_ADMIN"])
        for mock in smtp.values():
            mock.assert_not_called()

    def test_publish_failure_never_undoes_the_result_and_leaves_recovery_to_the_scanner(self):
        def boom(**_kw):
            raise RuntimeError("backend down")
        summary, smtp, _notify, _push = communications(True, notify_side_effect=boom)
        self.assertEqual(set(summary["events"].values()), {"failed"})
        for mock in smtp.values():
            mock.assert_not_called()

    def test_legacy_path_keeps_sending_smtp_when_flag_is_off(self):
        summary, smtp, notify_email, _push = communications(False)
        smtp["send_winner_email"].assert_called_once()
        smtp["send_draw_closed_admin"].assert_called_once()
        smtp["send_loser_email"].assert_called_once()
        notify_email.assert_not_called()
        self.assertEqual(summary["loser_emails"]["sent"], 1)

    def test_legacy_path_without_buyer_does_not_tell_participants_they_lost(self):
        _s, smtp, _notify, _push = communications(False, winner_user_id=None)
        smtp["send_winner_email"].assert_not_called()
        smtp["send_loser_email"].assert_not_called()
        smtp["send_draw_closed_admin"].assert_called_once()


class MissingBackendConfigTest(unittest.TestCase):
    """Workflow sem BACKEND_INTERNAL_API_BASE / PUSH_INTERNAL_EVENTS_TOKEN (caso atual do lotomania-result)."""

    def test_publisher_is_blocked_without_config_and_does_not_call_the_network(self):
        for env in ({}, {"BACKEND_INTERNAL_API_BASE": "https://backend.test"}, {"PUSH_INTERNAL_EVENTS_TOKEN": "t"}):
            with self.subTest(env=sorted(env)), patch.dict("os.environ", env, clear=True),                  patch("email_automation_events.requests.post") as post:
                response = notify_email_automation_event(
                    event_key="EMAIL_RESULT_ADMIN",
                    reference_type="draw",
                    reference_key="draw:1:result_admin_email",
                    metadata={"draw_id": 1},
                )
            post.assert_not_called()
            self.assertEqual(response["ok"], False)
            self.assertEqual(response["reason"], "backend_config_missing")

    def test_blocked_publication_keeps_the_result_sends_no_smtp_and_leaves_recovery_to_the_scanner(self):
        blocked = lambda **_kw: {"ok": False, "blocked": True, "reason": "backend_config_missing"}
        summary, smtp, _notify, push = communications(True, notify_side_effect=blocked)
        self.assertEqual(set(summary["events"].values()), {"failed"})
        for mock in smtp.values():
            mock.assert_not_called()
        push.assert_called_once()


class ProcessPendingDrawBackendModeTest(unittest.TestCase):
    def process(self, flag):
        conn = Mock()
        draw = {"id": 150, "draw_type": "principal", "status": "closed",
                "closed_at": datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc), "product_name": "Premio"}
        result = {"winner_number": 7, "contest_number": 2990, "previous_contest_number": 2989,
                  "result_date": date(2026, 10, 9)}
        comms = Mock()
        participants = Mock(return_value=[{"id": 2, "name": "Ana", "email": "ana@example.test"}])
        with patch.object(main, "COMMIT", True), \
             backend_owner(flag), \
             patch.object(main, "lock_pending_draw_for_result", return_value=draw), \
             patch.object(main, "get_draw_number_grid", return_value={"total": 100, "min": 0, "max": 99}), \
             patch.object(main, "winner_for_number", return_value=(11, "Vencedora", "v@example.test")), \
             patch.object(main, "set_draw_sorteado_any_status", return_value=1), \
             patch.object(main, "get_draw_label", return_value="Premio"), \
             patch.object(main, "get_participants", participants), \
             patch.object(main, "_send_result_communications", comms), \
             redirect_stdout(io.StringIO()):
            self.assertTrue(main._process_pending_draw(conn, draw, result))
        return comms, participants, conn

    def test_backend_mode_passes_contest_and_skips_loading_participants(self):
        comms, participants, conn = self.process(True)
        participants.assert_not_called()
        self.assertEqual(comms.call_args.kwargs["contest_number"], 2990)
        self.assertEqual(comms.call_args.kwargs["result_date"], date(2026, 10, 9))
        conn.commit.assert_called_once()

    def test_legacy_mode_still_loads_participants_without_the_winner(self):
        comms, participants, _conn = self.process(False)
        participants.assert_called_once()
        self.assertEqual(comms.call_args.args[6], [{"id": 2, "name": "Ana", "email": "ana@example.test"}])


if __name__ == "__main__":
    unittest.main()
