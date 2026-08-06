#!/usr/bin/env python3
import os
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg2
from psycopg2.extras import RealDictCursor

from email_automation_scan import run_email_automation_scan


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() == "true"


def _clean_pg_url(value: str) -> str:
    parts = urlsplit(value)
    allowed = {"sslmode", "ssl", "sslrootcert", "connect_timeout", "target_session_attrs", "application_name", "options"}
    query = {key: val for key, val in parse_qsl(parts.query or "", keep_blank_values=True) if key in allowed}
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def main() -> int:
    if not _env_true("EMAIL_AUTOMATION_SCAN_ENABLED"):
        print("[email-automation] skipped", {"reason": "disabled"})
        return 0
    dry_run = _env_true("EMAIL_AUTOMATION_DRY_RUN")
    postgres_url = _clean_pg_url(os.getenv("POSTGRES_URL", "").strip())
    backend_configured = (
        bool(os.getenv("BACKEND_INTERNAL_API_BASE", "").strip())
        and bool(os.getenv("PUSH_INTERNAL_EVENTS_TOKEN", "").strip())
    )
    if not postgres_url or (not dry_run and not backend_configured):
        print("[email-automation] failed", {"reason": "missing_required_environment"})
        return 1
    print("[email-automation] database_connecting", {"dry_run": dry_run})
    conn = None
    try:
        conn = psycopg2.connect(postgres_url, cursor_factory=RealDictCursor, sslmode="require")
        summary = run_email_automation_scan(
            conn,
            close_connection_before_publish=True,
        )
        print("[email-automation] completed", summary)
        return 0 if summary.get("ok") else 1
    except Exception as exc:
        print("[email-automation] failed", {
            "reason": "scan_failed",
            "error_type": exc.__class__.__name__,
        })
        return 1
    finally:
        if conn is not None and not getattr(conn, "closed", False):
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
