import os
import time

import requests


PUSH_EVENTS_PATH = "/api/internal/push/events"
NEW_DRAW_PUBLISHED = "NEW_DRAW_PUBLISHED"
MAX_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (1, 3)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEDUPE_STATUS_CODES = {409}


def _enabled() -> bool:
    return os.getenv("PUSH_AUTOMATION_EVENTS_ENABLED", "").lower() == "true"


def _allowed_event_key(event_key: str) -> bool:
    configured = os.getenv("PUSH_AUTOMATION_EVENT_KEYS", "").strip()
    if not configured:
        return True

    allowed = {
        key.strip()
        for key in configured.replace(";", ",").split(",")
        if key.strip()
    }
    return event_key in allowed


def notify_push_automation_event(
    event_key: str,
    reference_type: str,
    reference_key: str,
    metadata: dict | None = None,
    recipient_user_ids: list[int] | None = None,
    source: str = "engine",
    scan_id: str | None = None,
    occurred_at: str | None = None,
):
    if not _enabled():
        print("[push-automation] skipped: disabled")
        return {"ok": True, "skipped": True, "reason": "disabled"}

    if not _allowed_event_key(event_key):
        print("[push-automation] skipped: event key not allowed", {
            "event_key": event_key,
        })
        return {"ok": True, "skipped": True, "reason": "event_key_not_allowed"}

    backend_base_url = os.getenv("BACKEND_INTERNAL_API_BASE", "").strip()
    internal_token = os.getenv("PUSH_INTERNAL_EVENTS_TOKEN", "").strip()

    if not backend_base_url or not internal_token:
        print("[push-automation] skipped: backend config missing", {
            "has_base_url": bool(backend_base_url),
            "has_token": bool(internal_token),
        })
        return {"ok": False, "blocked": True, "reason": "backend_config_missing"}

    payload = {
        "event_key": event_key,
        "source": source,
        "reference_type": reference_type,
        "reference_key": reference_key,
        "metadata": metadata.copy() if isinstance(metadata, dict) else {},
    }
    if scan_id:
        payload["scan_id"] = scan_id
    if occurred_at:
        payload["occurred_at"] = occurred_at
    if recipient_user_ids is not None:
        payload["recipient_user_ids"] = recipient_user_ids

    print("[push-automation] notify:start", {
        "event_key": event_key,
        "reference_type": reference_type,
        "reference_key": reference_key,
    })

    url = f"{backend_base_url.rstrip('/')}{PUSH_EVENTS_PATH}"

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print("[push-events] attempt", {
                "event_key": event_key,
                "reference_key": reference_key,
                "attempt": attempt,
            })
            response = requests.post(
                url,
                json=payload,
                headers={"x-internal-token": internal_token},
                timeout=15,
            )

            try:
                data = response.json()
            except ValueError:
                data = None

            if response.status_code in DEDUPE_STATUS_CODES:
                print("[push-events] sent", {
                    "event_key": event_key,
                    "reference_key": reference_key,
                    "status": response.status_code,
                    "deduped": True,
                })
                return {"ok": True, "status": response.status_code, "deduped": True, "data": data}

            if response.ok:
                print("[push-events] sent", {
                    "event_key": event_key,
                    "reference_key": reference_key,
                    "status": response.status_code,
                    "backend_status": data.get("status") if isinstance(data, dict) else None,
                })
                return {"ok": True, "status": response.status_code, "data": data}

            message = (
                data.get("error") or data.get("message")
                if isinstance(data, dict)
                else "backend_request_failed"
            )
            should_retry = response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_ATTEMPTS
            if should_retry:
                delay = RETRY_DELAYS_SECONDS[attempt - 1]
                print("[push-events] retry", {
                    "event_key": event_key,
                    "reference_key": reference_key,
                    "status": response.status_code,
                    "attempt": attempt,
                    "delay_seconds": delay,
                })
                time.sleep(delay)
                continue

            print("[push-events] failed", {
                "event_key": event_key,
                "reference_key": reference_key,
                "status": response.status_code,
                "message": message,
            })
            return {
                "ok": False,
                "status": response.status_code,
                "reason": "backend_request_failed",
            }
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt < MAX_ATTEMPTS:
                delay = RETRY_DELAYS_SECONDS[attempt - 1]
                print("[push-events] retry", {
                    "event_key": event_key,
                    "reference_key": reference_key,
                    "status": None,
                    "attempt": attempt,
                    "delay_seconds": delay,
                    "message": exc.__class__.__name__,
                })
                time.sleep(delay)
                continue
            print("[push-events] failed", {
                "event_key": event_key,
                "reference_key": reference_key,
                "message": exc.__class__.__name__,
            })
            return {"ok": False, "reason": "backend_request_failed"}
        except Exception as exc:
            print("[push-events] failed", {
                "event_key": event_key,
                "reference_key": reference_key,
                "message": str(exc) or "backend_request_failed",
            })
            return {"ok": False, "reason": "backend_request_failed"}

    return {"ok": False, "reason": "backend_request_failed"}


def notify_new_draw_published(draw_id: int, metadata: dict | None = None):
    safe_metadata = metadata.copy() if isinstance(metadata, dict) else {}
    safe_metadata["draw_id"] = draw_id

    return notify_push_automation_event(
        event_key=NEW_DRAW_PUBLISHED,
        reference_type="draw",
        reference_key=f"draw:{draw_id}",
        metadata=safe_metadata,
        source="engine",
    )
