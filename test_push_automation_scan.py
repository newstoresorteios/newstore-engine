import os

from main import db
from push_automation_scan import run_push_automation_scan


def run_manual_scan():
    if os.getenv("PUSH_AUTOMATION_SCAN_ENABLED", "false").lower() != "true":
        print("[push-automation] manual scan skipped: scan disabled")
        return 0

    conn = db()
    try:
        run_push_automation_scan(conn)
        conn.rollback()
        return 0
    except Exception as exc:
        print("[push-automation] manual scan failed:", repr(exc))
        try:
            conn.rollback()
        except Exception:
            pass
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(run_manual_scan())
