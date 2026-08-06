import os
import time

import requests


EMAIL_EVENTS_PATH = "/api/internal/email/events"
MAX_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (2, 5, 10)
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
DEDUPE_STATUS_CODES = {409}
DEFAULT_BACKEND_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_BACKEND_READ_TIMEOUT_SECONDS = 45


def _positive_int_env(name, default):
    raw = os.getenv(name)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _log_context(event_key, reference_key, metadata):
    safe_metadata = metadata if isinstance(metadata, dict) else {}
    context = {
        "event_key": event_key,
        "reference_key": reference_key,
    }
    for field in (
        "draw_id",
        "draw_type",
        "draw_name",
        "user_id",
        "expires_on",
        "days_to_expire",
        "balance_cents",
    ):
        if safe_metadata.get(field) is not None:
            context[field] = safe_metadata[field]
    return context


def _log_attempt(context, attempt, started_at, status, reason=None):
    details = {
        **context,
        "attempt": attempt,
        "status": status,
        "elapsed_ms": max(0, int((time.monotonic() - started_at) * 1000)),
    }
    if reason:
        details["reason"] = reason
    print("[email-automation] publish_attempt", details)


def _retry_delay(attempt):
    return RETRY_DELAYS_SECONDS[attempt - 1]


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
    for field in (
        "draw_id",
        "draw_type",
        "draw_name",
        "draw_description",
        "product_name",
        "banner_title",
        "draw_type_label",
        "remaining_numbers",
        "status",
    ):
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
    context = _log_context(event_key, reference_key, event_metadata)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        started_at = time.monotonic()
        try:
            response = requests.post(
                url,
                json=payload,
                headers={
                    "X-Internal-Token": internal_token,
                    "Idempotency-Key": reference_key,
                },
                timeout=(connect_timeout, read_timeout),
            )
            try:
                data = response.json()
                response_is_valid = isinstance(data, dict)
            except ValueError:
                data = None
                response_is_valid = False

            if response.status_code in DEDUPE_STATUS_CODES:
                _log_attempt(context, attempt, started_at, response.status_code)
                return {
                    "ok": True,
                    "status": response.status_code,
                    "deduped": True,
                    "data": data,
                    "attempts": attempt,
                }

            if response.ok:
                if not response_is_valid:
                    reason = "backend_invalid_response"
                    _log_attempt(
                        context,
                        attempt,
                        started_at,
                        response.status_code,
                        reason,
                    )
                    return {
                        "ok": False,
                        "status": response.status_code,
                        "reason": reason,
                        "data": data,
                        "attempts": attempt,
                    }
                if data.get("ok") is False:
                    backend_reason = (
                        data.get("reason")
                        or data.get("error")
                        or "backend_event_failed"
                    )
                    reason = "backend_http_error"
                    _log_attempt(
                        context,
                        attempt,
                        started_at,
                        response.status_code,
                        reason,
                    )
                    return {
                        "ok": False,
                        "status": response.status_code,
                        "reason": reason,
                        "backend_reason": backend_reason,
                        "data": data,
                        "attempts": attempt,
                    }
                _log_attempt(context, attempt, started_at, response.status_code)
                return {
                    "ok": True,
                    "status": response.status_code,
                    "data": data,
                    "attempts": attempt,
                }

            reason = "backend_http_error"
            if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_ATTEMPTS:
                _log_attempt(
                    context,
                    attempt,
                    started_at,
                    response.status_code,
                    reason,
                )
                time.sleep(_retry_delay(attempt))
                continue
            _log_attempt(
                context,
                attempt,
                started_at,
                response.status_code,
                reason,
            )
            backend_reason = None
            if isinstance(data, dict):
                backend_reason = data.get("reason") or data.get("error")
            return {
                "ok": False,
                "status": response.status_code,
                "reason": reason,
                "backend_reason": backend_reason,
                "data": data,
                "attempts": attempt,
            }
        except requests.ConnectTimeout:
            reason = "backend_connect_timeout"
            _log_attempt(context, attempt, started_at, None, reason)
            if attempt < MAX_ATTEMPTS:
                time.sleep(_retry_delay(attempt))
                continue
            return {
                "ok": False,
                "status": None,
                "reason": reason,
                "attempts": attempt,
            }
        except requests.ReadTimeout:
            reason = "backend_read_timeout"
            _log_attempt(context, attempt, started_at, None, reason)
            if attempt < MAX_ATTEMPTS:
                time.sleep(_retry_delay(attempt))
                continue
            return {
                "ok": False,
                "status": None,
                "reason": reason,
                "delivery_unknown": True,
                "attempts": attempt,
            }
        except requests.ConnectionError:
            reason = "backend_connection_error"
            _log_attempt(context, attempt, started_at, None, reason)
            if attempt < MAX_ATTEMPTS:
                time.sleep(_retry_delay(attempt))
                continue
            return {
                "ok": False,
                "status": None,
                "reason": reason,
                "attempts": attempt,
            }
        except Exception:
            reason = "backend_connection_error"
            _log_attempt(context, attempt, started_at, None, reason)
            return {
                "ok": False,
                "status": None,
                "reason": reason,
                "attempts": attempt,
            }

    return {"ok": False, "reason": "backend_connection_error"}
