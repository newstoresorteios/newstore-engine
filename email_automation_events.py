import os
import time

import requests


EMAIL_EVENTS_PATH = "/api/internal/email/events"
MAX_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (1, 3)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEDUPE_STATUS_CODES = {409}
DEFAULT_BACKEND_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_BACKEND_READ_TIMEOUT_SECONDS = 240


def _positive_int_env(name, default):
    raw = os.getenv(name)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


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

    event_metadata = metadata.copy() if isinstance(metadata, dict) else {}
    payload = {
        "event_key": event_key,
        "reference_type": reference_type,
        "reference_key": reference_key,
        "metadata": event_metadata,
    }
    for field in ("draw_id", "draw_type", "draw_name", "remaining_numbers"):
        if field in event_metadata:
            payload[field] = event_metadata[field]
    if scan_id:
        payload["scan_id"] = scan_id
    if occurred_at:
        payload["occurred_at"] = occurred_at

    url = f"{backend_base_url.rstrip('/')}{EMAIL_EVENTS_PATH}"
    connect_timeout = _positive_int_env(
        "EMAIL_AUTOMATION_BACKEND_CONNECT_TIMEOUT_SECONDS",
        DEFAULT_BACKEND_CONNECT_TIMEOUT_SECONDS,
    )
    read_timeout = _positive_int_env(
        "EMAIL_AUTOMATION_BACKEND_READ_TIMEOUT_SECONDS",
        DEFAULT_BACKEND_READ_TIMEOUT_SECONDS,
    )
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.post(
                url,
                json=payload,
                headers={"x-internal-token": internal_token},
                timeout=(connect_timeout, read_timeout),
            )
            try:
                data = response.json()
            except ValueError:
                data = None
            if response.status_code in DEDUPE_STATUS_CODES:
                return {
                    "ok": True,
                    "status": response.status_code,
                    "deduped": True,
                    "data": data,
                }
            if response.ok:
                if isinstance(data, dict) and data.get("ok") is False:
                    return {
                        "ok": False,
                        "status": response.status_code,
                        "reason": (
                            data.get("reason")
                            or data.get("error")
                            or "backend_event_failed"
                        ),
                        "data": data,
                    }
                return {"ok": True, "status": response.status_code, "data": data}
            if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_DELAYS_SECONDS[attempt - 1])
                continue
            if isinstance(data, dict):
                reason = (
                    data.get("reason")
                    or data.get("error")
                    or "backend_request_failed"
                )
            else:
                reason = "backend_request_failed"
            return {
                "ok": False,
                "status": response.status_code,
                "reason": reason,
                "data": data,
            }
        except requests.ReadTimeout:
            return {
                "ok": False,
                "status": None,
                "reason": "backend_response_timeout",
                "delivery_unknown": True,
            }
        except requests.ConnectTimeout:
            if attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_DELAYS_SECONDS[attempt - 1])
                continue
            return {
                "ok": False,
                "status": None,
                "reason": "backend_connection_timeout",
            }
        except requests.ConnectionError:
            if attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_DELAYS_SECONDS[attempt - 1])
                continue
            return {
                "ok": False,
                "status": None,
                "reason": "backend_connection_failed",
            }
        except Exception:
            return {"ok": False, "reason": "backend_request_failed"}
    return {"ok": False, "reason": "backend_request_failed"}
