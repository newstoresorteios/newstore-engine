import os
import time

import requests


EMAIL_EVENTS_PATH = "/api/internal/email/events"
MAX_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (1, 3)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def notify_email_automation_event(
    event_key: str,
    reference_type: str,
    reference_key: str,
    metadata: dict | None = None,
    scan_id: str | None = None,
    occurred_at: str | None = None,
):
    backend_base_url = os.getenv("BACKEND_INTERNAL_API_BASE", "").strip()
    internal_token = os.getenv("PUSH_INTERNAL_EVENTS_TOKEN", "").strip()
    if not backend_base_url or not internal_token:
        return {"ok": False, "blocked": True, "reason": "backend_config_missing"}

    payload = {
        "event_key": event_key,
        "reference_type": reference_type,
        "reference_key": reference_key,
        "metadata": metadata.copy() if isinstance(metadata, dict) else {},
    }
    if scan_id:
        payload["scan_id"] = scan_id
    if occurred_at:
        payload["occurred_at"] = occurred_at

    url = f"{backend_base_url.rstrip('/')}{EMAIL_EVENTS_PATH}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.post(
                url,
                json=payload,
                headers={"x-internal-token": internal_token},
                timeout=15,
            )
            data = response.json() if response.content else None
            if response.ok:
                return {"ok": True, "status": response.status_code, "data": data}
            if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_DELAYS_SECONDS[attempt - 1])
                continue
            return {"ok": False, "status": response.status_code, "reason": "backend_request_failed"}
        except (requests.Timeout, requests.ConnectionError):
            if attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_DELAYS_SECONDS[attempt - 1])
                continue
            return {"ok": False, "reason": "backend_request_failed"}
        except Exception:
            return {"ok": False, "reason": "backend_request_failed"}
    return {"ok": False, "reason": "backend_request_failed"}
