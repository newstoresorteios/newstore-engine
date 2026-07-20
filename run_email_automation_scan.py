#!/usr/bin/env python3
import os
import re
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


def _mask_pg_url(value: str) -> str:
    return re.sub(r"://([^:]+):[^@]+@", r"://\1:***@", value or "")


def main() -> int:
    if not _env_true("EMAIL_AUTOMATION_SCAN_ENABLED"):
        print("[email-automation] skipped", {"reason": "disabled"})
        return 0
    postgres_url = _clean_pg_url(os.getenv("POSTGRES_URL", "").strip())
    if not postgres_url or not os.getenv("BACKEND_INTERNAL_API_BASE", "").strip() or not os.getenv("PUSH_INTERNAL_EVENTS_TOKEN", "").strip():
        print("[email-automation] failed", {"reason": "missing_required_environment"})
        return 1
    print("[email-automation] database_connecting", {"postgres_url": _mask_pg_url(postgres_url)})
    conn = None
    try:
        conn = psycopg2.connect(postgres_url, cursor_factory=RealDictCursor, sslmode="require")
        summary = run_email_automation_scan(conn)
        conn.rollback()
        print("[email-automation] completed", summary)
        return 0 if summary.get("ok") else 1
    except Exception as exc:
        print("[email-automation] failed", {"error": str(exc) or "scan_failed"})
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
