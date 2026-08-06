import os
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import run_email_automation_scan


class RunEmailAutomationScanTest(unittest.TestCase):
    postgres_url = "postgres://reader:secret@database.example/postgres"

    def _connection(self):
        conn = Mock()
        conn.closed = False
        return conn

    @patch.dict(os.environ, {}, clear=True)
    def test_disabled_scan_returns_zero(self):
        self.assertEqual(run_email_automation_scan.main(), 0)

    @patch("run_email_automation_scan.run_email_automation_scan")
    @patch("run_email_automation_scan.psycopg2.connect")
    def test_successful_scan_returns_zero(self, connect, scan):
        connect.return_value = self._connection()
        scan.return_value = {"ok": True}
        env = {
            "EMAIL_AUTOMATION_SCAN_ENABLED": "true",
            "EMAIL_AUTOMATION_DRY_RUN": "false",
            "POSTGRES_URL": self.postgres_url,
            "BACKEND_INTERNAL_API_BASE": "https://backend.example",
            "PUSH_INTERNAL_EVENTS_TOKEN": "internal-secret",
        }

        with patch.dict(os.environ, env, clear=True), patch("builtins.print") as log:
            exit_code = run_email_automation_scan.main()

        self.assertEqual(exit_code, 0)
        scan.assert_called_once_with(
            connect.return_value,
            close_connection_before_publish=True,
        )
        rendered_logs = str(log.call_args_list)
        self.assertNotIn(self.postgres_url, rendered_logs)
        self.assertNotIn("internal-secret", rendered_logs)

    @patch("run_email_automation_scan.run_email_automation_scan")
    @patch("run_email_automation_scan.psycopg2.connect")
    def test_definitive_failure_returns_one(self, connect, scan):
        connect.return_value = self._connection()
        scan.return_value = {"ok": False, "failed": 1}
        env = {
            "EMAIL_AUTOMATION_SCAN_ENABLED": "true",
            "EMAIL_AUTOMATION_DRY_RUN": "false",
            "POSTGRES_URL": self.postgres_url,
            "BACKEND_INTERNAL_API_BASE": "https://backend.example",
            "PUSH_INTERNAL_EVENTS_TOKEN": "internal-secret",
        }

        with patch.dict(os.environ, env, clear=True), patch("builtins.print"):
            exit_code = run_email_automation_scan.main()

        self.assertEqual(exit_code, 1)

    @patch("run_email_automation_scan.run_email_automation_scan")
    @patch("run_email_automation_scan.psycopg2.connect")
    def test_dry_run_does_not_require_backend_credentials(self, connect, scan):
        connect.return_value = self._connection()
        scan.return_value = {"ok": True, "dry_run": True}
        env = {
            "EMAIL_AUTOMATION_SCAN_ENABLED": "true",
            "EMAIL_AUTOMATION_DRY_RUN": "true",
            "POSTGRES_URL": self.postgres_url,
        }

        with patch.dict(os.environ, env, clear=True), patch("builtins.print"):
            exit_code = run_email_automation_scan.main()

        self.assertEqual(exit_code, 0)
        scan.assert_called_once()

    @patch.dict(os.environ, {
        "EMAIL_AUTOMATION_SCAN_ENABLED": "true",
        "EMAIL_AUTOMATION_DRY_RUN": "false",
        "POSTGRES_URL": "postgres://reader:secret@database.example/postgres",
    }, clear=True)
    def test_real_mode_requires_backend_credentials(self):
        with patch("builtins.print"):
            self.assertEqual(run_email_automation_scan.main(), 1)

    def test_existing_workflow_keeps_schedule_and_has_required_limits(self):
        workflow = Path(
            ".github/workflows/email-automation-scan.yml"
        ).read_text(encoding="utf-8")
        self.assertIn('cron: "*/10 * * * *"', workflow)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("timeout-minutes: 10", workflow)
        self.assertIn("EMAIL_BALANCE_AUTOMATION_EFFECTIVE_FROM", workflow)
        self.assertIn("EMAIL_BALANCE_EXPIRED_BACKFILL_ENABLED", workflow)
        self.assertIn("EMAIL_AUTOMATION_DRY_RUN", workflow)


if __name__ == "__main__":
    unittest.main()
