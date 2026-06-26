import os

from main import db
from push_automation_scan import run_push_automation_scan


def _empty_summary(ok=True):
    return {
        "ok": ok,
        "remaining_numbers_checked": 0,
        "winner_checked": 0,
        "balance_checked": 0,
        "events_candidates": 0,
        "events_attempted": 0,
        "events_blocked": 0,
        "events_skipped": 0,
        "events_sent_to_backend": 0,
    }


def run_manual_scan():
    if os.getenv("PUSH_AUTOMATION_SCAN_ENABLED", "false").lower() != "true":
        print("[push-automation] manual scan skipped: scan disabled")
        summary = _empty_summary()
        print("[push-automation] scan:finished", summary)
        return 0

    conn = db()
    try:
        summary = run_push_automation_scan(conn)
        conn.rollback()
        print("[push-automation] scan:finished", {
            "ok": summary.get("ok") is True,
            "scan_id": summary.get("scan_id"),
            "remaining_numbers_checked": summary.get("remaining_numbers_checked", 0),
            "winner_checked": summary.get("winner_checked", 0),
            "balance_checked": summary.get("balance_checked", 0),
            "events_candidates": summary.get("events_candidates", 0),
            "events_attempted": summary.get("events_attempted", 0),
            "events_blocked": summary.get("events_blocked", 0),
            "events_skipped": summary.get("events_skipped", 0),
            "events_sent_to_backend": summary.get("events_sent_to_backend", 0),
            "by_event_key": summary.get("by_event_key", {}),
        })
        return 0
    except Exception as exc:
        print("[push-automation] manual scan failed:", {
            "message": str(exc) or "manual_scan_failed",
        })
        try:
            conn.rollback()
        except Exception:
            pass
        print("[push-automation] scan:finished", _empty_summary(ok=False))
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(run_manual_scan())
