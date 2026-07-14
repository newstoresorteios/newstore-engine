#!/usr/bin/env python3
import os
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

from push_automation_scan import run_push_automation_scan


REQUIRED_ENV = (
    "POSTGRES_URL",
    "BACKEND_INTERNAL_API_BASE",
    "PUSH_INTERNAL_EVENTS_TOKEN",
)


def _env_true(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() == "true"


def _mask_pg_url(value: str) -> str:
    return re.sub(r"://([^:]+):[^@]+@", r"://\1:***@", value or "")


def _clean_pg_url(value: str) -> str:
    if not value:
        return value
    parts = urlsplit(value)
    allowed = {
        "sslmode",
        "ssl",
        "sslrootcert",
        "connect_timeout",
        "target_session_attrs",
        "application_name",
        "options",
    }
    query = {
        key: val
        for key, val in parse_qsl(parts.query or "", keep_blank_values=True)
        if key in allowed
    }
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _load_env() -> None:
    if load_dotenv is not None:
        load_dotenv()


def _validate_environment() -> None:
    missing = [name for name in REQUIRED_ENV if not os.getenv(name, "").strip()]
    if missing:
        raise RuntimeError(f"missing_required_environment:{','.join(missing)}")
    if not _env_true("PUSH_AUTOMATION_SCAN_ENABLED"):
        raise RuntimeError("missing_required_environment:PUSH_AUTOMATION_SCAN_ENABLED")


def _connect():
    postgres_url = _clean_pg_url(os.getenv("POSTGRES_URL", "").strip())
    print("[push-scan] database_connecting", {"postgres_url": _mask_pg_url(postgres_url)})
    return psycopg2.connect(postgres_url, cursor_factory=RealDictCursor, sslmode="require")


def main() -> int:
    print("[push-scan] start")
    conn = None
    try:
        _load_env()
        _validate_environment()
        conn = _connect()
        print("[push-scan] database_connected")
        summary = run_push_automation_scan(conn)
        conn.rollback()
        print("[push-scan] completed", {
            "ok": summary.get("ok") is True,
            "scan_id": summary.get("scan_id"),
            "events_candidates": summary.get("events_candidates", 0),
            "events_attempted": summary.get("events_attempted", 0),
            "events_blocked": summary.get("events_blocked", 0),
            "events_skipped": summary.get("events_skipped", 0),
            "events_sent_to_backend": summary.get("events_sent_to_backend", 0),
            "by_event_key": summary.get("by_event_key", {}),
        })
        return 0
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        print("[push-scan] failed", {
            "error": getattr(exc, "args", [str(exc)])[0] or "scan_failed",
        })
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
