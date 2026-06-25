import os

import requests


PUSH_EVENTS_PATH = "/api/internal/push/events"
NEW_DRAW_PUBLISHED = "NEW_DRAW_PUBLISHED"


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


def notify_new_draw_published(draw_id: int, metadata: dict | None = None):
    event_key = NEW_DRAW_PUBLISHED

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

    safe_metadata = metadata.copy() if isinstance(metadata, dict) else {}
    safe_metadata["draw_id"] = draw_id

    payload = {
        "event_key": event_key,
        "source": "engine",
        "reference_type": "draw",
        "reference_key": f"draw:{draw_id}",
        "metadata": safe_metadata,
    }

    print("[push-automation] notify:start", {
        "event_key": event_key,
        "reference_type": "draw",
        "reference_key": payload["reference_key"],
    })

    try:
        response = requests.post(
            f"{backend_base_url.rstrip('/')}{PUSH_EVENTS_PATH}",
            json=payload,
            headers={"x-internal-token": internal_token},
            timeout=15,
        )

        try:
            data = response.json()
        except ValueError:
            data = None

        if not response.ok:
            print("[push-automation] notify:failed", {
                "event_key": event_key,
                "reference_key": payload["reference_key"],
                "status": response.status_code,
                "message": (
                    data.get("error") or data.get("message")
                    if isinstance(data, dict)
                    else "backend_request_failed"
                ),
            })
            return {
                "ok": False,
                "status": response.status_code,
                "reason": "backend_request_failed",
            }

        print("[push-automation] notify:done", {
            "event_key": event_key,
            "reference_key": payload["reference_key"],
            "status": data.get("status") if isinstance(data, dict) else None,
        })
        return {"ok": True, "status": response.status_code, "data": data}
    except Exception as exc:
        print("[push-automation] notify:failed", {
            "event_key": event_key,
            "reference_key": payload["reference_key"],
            "message": str(exc) or "backend_request_failed",
        })
        return {"ok": False, "reason": "backend_request_failed"}
