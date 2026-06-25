"use strict";

const PUSH_EVENTS_PATH = "/api/internal/push/events";

function notificationLogData(payload) {
  return {
    event_key: payload.event_key || null,
    source: payload.source || null,
    metadata_keys:
      payload.metadata && typeof payload.metadata === "object"
        ? Object.keys(payload.metadata).sort()
        : [],
  };
}

function blocked(reason) {
  return {
    ok: false,
    blocked: true,
    reason,
  };
}

function buildPushAutomationPayload(payload) {
  const eventPayload = payload && typeof payload === "object" ? payload : {};

  return {
    event_key: eventPayload.event_key || "NEW_DRAW_PUBLISHED",
    source: eventPayload.source || "engine-dry-run",
    metadata:
      eventPayload.metadata && typeof eventPayload.metadata === "object"
        ? eventPayload.metadata
        : {},
  };
}

async function notifyPushAutomationEvent(payload) {
  const eventPayload = buildPushAutomationPayload(payload);

  if (process.env.PUSH_AUTOMATION_EVENTS_ENABLED !== "true") {
    return {
      ok: true,
      skipped: true,
      reason: "push_automation_events_disabled",
    };
  }

  if (process.env.PUSH_AUTOMATION_DRY_RUN !== "true") {
    return blocked("push_automation_dry_run_required");
  }

  const backendBaseUrl = String(
    process.env.BACKEND_INTERNAL_API_BASE || "",
  ).trim();
  const internalToken = String(
    process.env.PUSH_INTERNAL_EVENTS_TOKEN || "",
  ).trim();

  if (!backendBaseUrl || !internalToken) {
    return blocked("push_internal_backend_config_missing");
  }

  const logData = notificationLogData(eventPayload);
  console.log("[push-automation] notify:start", logData);

  try {
    if (typeof globalThis.fetch !== "function") {
      throw new Error("fetch_not_available");
    }

    const response = await globalThis.fetch(
      `${backendBaseUrl.replace(/\/+$/, "")}${PUSH_EVENTS_PATH}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-internal-token": internalToken,
        },
        body: JSON.stringify(eventPayload),
      },
    );

    let data = null;
    try {
      data = await response.json();
    } catch (_) {
      data = null;
    }

    if (!response.ok) {
      console.warn("[push-automation] notify:failed", {
        event_key: eventPayload.event_key || null,
        status: response.status,
        message: data?.error || data?.message || "backend_request_failed",
      });

      return {
        ok: false,
        status: response.status,
        reason: "backend_request_failed",
      };
    }

    console.log("[push-automation] notify:done", {
      event_key: eventPayload.event_key || null,
      ok: response.ok,
      status: data?.status || null,
    });

    return {
      ok: true,
      status: response.status,
      data,
    };
  } catch (error) {
    console.warn("[push-automation] notify:failed", {
      event_key: eventPayload.event_key || null,
      status: null,
      message: error?.message || "backend_request_failed",
    });

    return {
      ok: false,
      reason: "backend_request_failed",
    };
  }
}

async function notifyBackendEvent(payload) {
  return notifyPushAutomationEvent(payload);
}

module.exports = {
  notifyPushAutomationEvent,
  notifyBackendEvent,
};
