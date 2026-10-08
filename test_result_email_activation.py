"""Janelas de recuperacao, varredura de expiracao, alerta de falha definitiva, dono unico do envio
e preservacao do D+1/Caixa. Tudo com mocks: nao envia e-mail, nao toca no banco."""
import hashlib
import inspect
import io
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import main
import test_result_email_events as base
from email_automation_scan import (
    EMAIL_RESULT_EXPIRY_SWEEP_HOURS,
    EMAIL_RESULT_LOOKBACK_HOURS,
    run_email_automation_scan,
)

REALIZED = base.REALIZED
CUTOFF_ENV = {"EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM": "2026-10-09T00:00:00Z"}


def row_realized(draw_id, hours_ago, now, **kwargs):
    row = base.result_row(draw_id, **kwargs)
    row["realized_at"] = now - timedelta(hours=hours_ago)
    return row


def scan_at(now, rows, backend=None, env=None, dry_run=False):
    conn = base.ResultConnection(rows)
    notify = Mock(side_effect=backend or (lambda **_kw: {"ok": True, "status": 200, "data": {"ok": True, "sent": 1, "failed": 0, "skipped": 0}}))
    full_env = {**CUTOFF_ENV, **(env or {})}
    if dry_run:
        full_env["EMAIL_AUTOMATION_DRY_RUN"] = "true"
    out = io.StringIO()
    with patch.dict("os.environ", full_env, clear=True):
        with patch("email_automation_scan.notify_email_automation_event", notify):
            with redirect_stdout(out):
                summary = run_email_automation_scan(conn, now=now)
    return summary, notify, conn, out.getvalue()


def published(notify):
    return sorted((c.kwargs["event_key"], c.kwargs["reference_key"], c.kwargs["metadata"].get("expiry_sweep")) for c in notify.call_args_list)


NOW = REALIZED + timedelta(hours=1)


class RecoveryWindowTest(unittest.TestCase):
    def test_windows_are_168h_republication_plus_72h_critical_sweep(self):
        self.assertEqual(EMAIL_RESULT_LOOKBACK_HOURS, 168)
        self.assertEqual(EMAIL_RESULT_EXPIRY_SWEEP_HOURS, 72)

    def test_query_window_is_lookback_plus_sweep_and_keeps_the_activation_cutoff(self):
        _s, _n, conn, _log = scan_at(NOW, [])
        sql, params = next((q, p) for q, p in conn.executions if "realized_at IS NOT NULL" in q and "status = 'sorteado'" in q)
        self.assertEqual(params[0], 168 + 72)
        self.assertEqual(params[1], datetime(2026, 10, 9, tzinfo=timezone.utc))
        self.assertIn("d.realized_at >= %s", sql)

    def test_inside_168h_all_events_are_republished_for_recovery(self):
        now = NOW + timedelta(hours=100)
        _s, notify, _c, _l = scan_at(now, [row_realized(150, 167, now)])
        self.assertEqual(
            [(e, sweep) for e, _k, sweep in published(notify)],
            [("EMAIL_RESULT_ADMIN", False), ("EMAIL_RESULT_PARTICIPANT", False), ("EMAIL_RESULT_WINNER", False)],
        )

    def test_after_168h_only_critical_events_are_republished_as_expiry_sweep(self):
        _s, notify, _c, _l = scan_at(NOW, [row_realized(150, 200, NOW)])
        self.assertEqual(
            [(e, sweep) for e, _k, sweep in published(notify)],
            [("EMAIL_RESULT_ADMIN", True), ("EMAIL_RESULT_WINNER", True)],
        )

    def test_after_168h_without_buyer_only_the_admin_event(self):
        _s, notify, _c, _l = scan_at(NOW, [row_realized(150, 200, NOW, winner_user_id=None)])
        self.assertEqual([(e, s) for e, _k, s in published(notify)], [("EMAIL_RESULT_ADMIN", True)])

    def test_sweep_window_is_configurable(self):
        _s, _n, conn, _l = scan_at(NOW, [], env={"EMAIL_AUTOMATION_RESULT_EXPIRY_SWEEP_HOURS": "24", "EMAIL_AUTOMATION_RESULT_LOOKBACK_HOURS": "100"})
        _sql, params = next((q, p) for q, p in conn.executions if "realized_at IS NOT NULL" in q and "status = 'sorteado'" in q)
        self.assertEqual(params[0], 124)

    def test_reference_keys_do_not_change_between_republications(self):
        _s1, first, _c1, _l1 = scan_at(NOW, [row_realized(150, 10, NOW)])
        _s2, second, _c2, _l2 = scan_at(NOW + timedelta(hours=170), [row_realized(150, 10 + 170, NOW + timedelta(hours=170))])
        first_keys = {k for _e, k, _s in published(first)}
        second_keys = {k for _e, k, _s in published(second)}
        self.assertTrue(second_keys <= first_keys)

    def test_without_activation_cutoff_nothing_is_published_even_for_recent_results(self):
        out = io.StringIO()
        conn = base.ResultConnection([row_realized(150, 1, NOW)])
        notify = Mock(return_value={"ok": True})
        with patch.dict("os.environ", {}, clear=True):
            with patch("email_automation_scan.notify_email_automation_event", notify):
                with redirect_stdout(out):
                    summary = run_email_automation_scan(conn, now=NOW)
        self.assertTrue(summary["result_disabled"])
        notify.assert_not_called()


class CriticalFailureAlertTest(unittest.TestCase):
    def critical(self, **kwargs):
        def backend(**call):
            if call["event_key"] == "EMAIL_RESULT_WINNER":
                return {"ok": True, "status": 200, "data": {"ok": True, "status": "critical_failure", "sent": 0, "failed": 1, "skipped": 0, "critical_alerts": 1}}
            return {"ok": True, "status": 200, "data": {"ok": True, "status": "processed", "sent": 1, "failed": 0, "skipped": 0}}
        return scan_at(NOW, [row_realized(150, 10, NOW)], backend=backend, **kwargs)

    def test_definitive_failure_makes_the_job_fail_and_other_events_still_go_out(self):
        summary, notify, _c, log = self.critical()
        self.assertFalse(summary["ok"])
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(notify.call_count, 3)  # administracao e participantes nao ficam bloqueados

    def test_alert_log_identifies_draw_and_event_without_personal_data(self):
        _summary, _notify, _c, log = self.critical()
        line = next(entry for entry in log.splitlines() if "critical_result_notification_failed" in entry)
        self.assertIn("'draw_id': 150", line)
        self.assertIn("EMAIL_RESULT_WINNER", line)
        self.assertIn("draw:150:result_winner_email", line)
        self.assertNotIn("@", line)

    def test_a_later_silent_response_does_not_fail_the_job_again(self):
        def silent(**_call):
            return {"ok": True, "status": 200, "data": {"ok": True, "status": "nothing_to_send", "sent": 0, "failed": 0, "skipped": 1, "exhausted": 1}}
        summary, _notify, _c, log = scan_at(NOW, [row_realized(150, 10, NOW)], backend=silent)
        self.assertTrue(summary["ok"])
        self.assertNotIn("critical_result_notification_failed", log)


class SingleOwnerTest(unittest.TestCase):
    """Exatamente um responsavel por resultado: SMTP legado XOR backend."""

    STATES = {
        "sem corte": ({}, "legacy"),
        "corte vazio": ({"EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM": ""}, "legacy"),
        "corte invalido": ({"EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM": "amanha"}, "legacy"),
        "corte no futuro": ({"EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM": "2999-01-01T00:00:00Z"}, "legacy"),
        "corte alcancado": ({"EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM": "2026-01-01T00:00:00Z"}, "backend"),
    }

    def run_comms(self, env):
        smtp = {name: Mock(return_value=True) for name in ("send_winner_email", "send_draw_closed_admin", "send_loser_email")}
        notify_email = Mock(return_value={"ok": True})
        with patch.dict("os.environ", env, clear=True):
            with patch.object(main, "notify_email_automation_event", notify_email):
                with patch.object(main, "notify_push_automation_event", Mock(return_value={"ok": True})):
                    with patch.multiple(main, **smtp):
                        with redirect_stdout(io.StringIO()):
                            main._send_result_communications(
                                base.draw_dict(), "Premio", 7, 11, "Vencedora", "v@example.test",
                                [{"id": 2, "name": "Ana", "email": "ana@example.test"}],
                                contest_number=2990, result_date=None,
                            )
        smtp_used = any(mock.called for mock in smtp.values())
        return smtp_used, notify_email.called

    def test_never_both_and_never_neither(self):
        for name, (env, expected) in self.STATES.items():
            with self.subTest(state=name):
                smtp_used, published_events = self.run_comms(env)
                self.assertNotEqual(smtp_used, published_events)  # XOR
                self.assertEqual("legacy" if smtp_used else "backend", expected)

    def test_scanner_and_main_use_the_same_activation_switch(self):
        for name, (env, expected) in self.STATES.items():
            with self.subTest(state=name):
                with patch.dict("os.environ", env, clear=True):
                    owned_by_backend = main._result_emails_owned_by_backend()
                    scanner_enabled = base.run_email_automation_scan.__globals__["_result_effective_from"]() is not None
                self.assertEqual(owned_by_backend, expected == "backend")
                # o scanner publica quando ha corte valido (inclusive no futuro: o backend e o scanner descartam
                # sorteios anteriores ao corte); o main so abre mao do SMTP depois do corte
                if expected == "backend":
                    self.assertTrue(scanner_enabled)

    def test_future_cutoff_keeps_smtp_for_draws_realized_before_it(self):
        with patch.dict("os.environ", {"EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM": "2026-10-20T00:00:00Z"}, clear=True):
            self.assertFalse(main._result_emails_owned_by_backend(datetime(2026, 10, 19, 23, 59, tzinfo=timezone.utc)))
            self.assertTrue(main._result_emails_owned_by_backend(datetime(2026, 10, 20, 0, 0, tzinfo=timezone.utc)))

    def test_default_production_state_is_legacy_and_publishes_nothing(self):
        smtp_used, published_events = self.run_comms({})
        self.assertTrue(smtp_used)
        self.assertFalse(published_events)

    def test_workflows_wire_the_same_variable_for_both_owners(self):
        for path in (".github/workflows/lotomania-result.yml", ".github/workflows/email-automation-scan.yml"):
            with self.subTest(path=path):
                text = open(path, encoding="utf-8").read()
                self.assertIn("EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM: ${{ vars.EMAIL_RESULT_AUTOMATION_EFFECTIVE_FROM }}", text)
                self.assertNotIn("RESULT_EMAILS_VIA_BACKEND", text)


class OtherEmailFamiliesUnaffectedTest(unittest.TestCase):
    def test_closed_email_still_published_alongside_result_events(self):
        closed = {"id": 9, "status": "closed", "draw_type": "principal", "product_name": "X", "draw_name": "X",
                  "closed_at": NOW - timedelta(hours=2)}
        conn = base.ResultConnection([row_realized(150, 3, NOW)], closed_draws=[closed])
        notify = Mock(return_value={"ok": True})
        with patch.dict("os.environ", CUTOFF_ENV, clear=True):
            with patch("email_automation_scan.notify_email_automation_event", notify):
                with redirect_stdout(io.StringIO()):
                    summary = run_email_automation_scan(conn, now=NOW)
        events = sorted(c.kwargs["event_key"] for c in notify.call_args_list)
        self.assertEqual(events, ["DRAW_CLOSED", "EMAIL_RESULT_ADMIN", "EMAIL_RESULT_PARTICIPANT", "EMAIL_RESULT_WINNER"])
        closed_call = next(c for c in notify.call_args_list if c.kwargs["event_key"] == "DRAW_CLOSED")
        self.assertEqual(closed_call.kwargs["reference_key"], "draw:9:closed_email")
        self.assertEqual(summary["closed_checked"], 1)


class Dplus1AndCaixaPreservedTest(unittest.TestCase):
    """As funcoes da Caixa, da regra D+1 e da definicao do vencedor sao identicas ao publicado (6c4e604).
    Se uma mudanca deliberada for necessaria, atualize estes hashes junto com a revisao da regra."""

    EXPECTED = {
        "resolve_first_eligible_lotomania_result": "8dae6c8fd129c912",
        "_lotomania_result_at": "4e4d08ab5bd73de3",
        "_result_before_draw_close": "8a7d5da0600c75b4",
        "_parse_lotomania_payload": "7026fe4ebf783a7a",
        "get_lotomania_result": "38867dfe6aa23696",
        "get_last_lotomania_result": "f05f1dcea2ad266c",
        "get_pending_draws": "789dfc58df4f1b41",
        "lock_pending_draw_for_result": "4b2893070c26f979",
        "winner_for_number": "e274b4b2216e66bf",
        "set_draw_sorteado_any_status": "7733189981009374",
        "_as_brasilia_datetime": "822e64c3e8a31edc",
    }

    def test_source_of_critical_functions_is_unchanged(self):
        for name, expected in self.EXPECTED.items():
            with self.subTest(function=name):
                source = inspect.getsource(getattr(main, name)).replace("\r\n", "\n")
                self.assertEqual(hashlib.sha256(source.encode()).hexdigest()[:16], expected)

    def test_reference_time_and_last_dezena_rule_are_unchanged(self):
        self.assertEqual(main.LOTOMANIA_DRAW_TIME_BRT.hour, 21)
        self.assertEqual(main._parse_lotomania_payload({
            "numero": 10, "numeroConcursoAnterior": 9, "dataApuracao": "01/10/2026",
            "dezenasSorteadasOrdemSorteio": [f"{n:02d}" for n in (60, 5, 33, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 21)],
        })["winner_number"], 21)


if __name__ == "__main__":
    unittest.main()
