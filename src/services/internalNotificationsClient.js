"use strict";

const INTERNAL_EVENTS_PATH = "/api/internal/notifications/events";

function hasOwn(object, key) {
  return Object.prototype.hasOwnProperty.call(object, key);
}

function notificationLogData(payload) {
  return {
    event_key: payload.event_key || null,
    dedupe_key: payload.dedupe_key || null,
    category: payload.category || null,
    entity_type: payload.entity_type || null,
    entity_id: payload.entity_id || null,
    has_user_id: Boolean(payload.user_id),
    has_subscription_id: Boolean(payload.subscription_id),
    audience_type: payload.audience?.type || null,
  };
}

function blocked(reason) {
  return {
    ok: false,
    blocked: true,
    reason,
  };
}

async function notifyBackendEvent(payload) {
  const eventPayload = payload && typeof payload === "object" ? payload : {};

  if (process.env.ENGINE_PUSH_WATCHER_ENABLED !== "true") {
    return {
      ok: true,
      skipped: true,
      reason: "engine_push_watcher_disabled",
    };
  }

  if (process.env.PUSH_WATCHER_DRY_RUN === "true") {
    console.log("[push-watcher] dry-run", notificationLogData(eventPayload));
    return {
      ok: true,
      dry_run: true,
    };
  }

  if (process.env.PUSH_WATCHER_ALLOW_REAL_SEND !== "true") {
    return blocked("push_watcher_real_send_blocked");
  }

  if (hasOwn(eventPayload, "audience")) {
    return blocked("audience_blocked_in_engine_test_mode");
  }

  const channels = Array.isArray(eventPayload.channels)
    ? eventPayload.channels
    : [];

  if (
    channels.some(
      (channel) => String(channel || "").trim().toLowerCase() === "whatsapp",
    )
  ) {
    return blocked("whatsapp_blocked_in_engine_test_mode");
  }

  if (
    String(eventPayload.provider || "").trim().toLowerCase() === "brevo"
  ) {
    return blocked("brevo_blocked_in_engine_test_mode");
  }

  if (process.env.ENGINE_PUSH_TEST_ONLY !== "true") {
    return blocked("engine_push_test_only_required");
  }

  const allowedSubscriptionId = String(
    process.env.PUSH_TEST_SUBSCRIPTION_ID || "",
  ).trim();
  const requestedSubscriptionId = String(
    eventPayload.subscription_id || "",
  ).trim();

  if (
    !allowedSubscriptionId ||
    !requestedSubscriptionId ||
    requestedSubscriptionId !== allowedSubscriptionId
  ) {
    return blocked("subscription_not_allowed_in_engine_test_mode");
  }

  const backendBaseUrl = String(
    process.env.BACKEND_INTERNAL_BASE_URL || "",
  ).trim();
  const internalEngineToken = String(
    process.env.INTERNAL_ENGINE_TOKEN || "",
  ).trim();

  if (!backendBaseUrl || !internalEngineToken) {
    return blocked("backend_internal_config_missing");
  }

  const logData = notificationLogData(eventPayload);
  console.log("[push-watcher] notify:start", logData);

  try {
    if (typeof globalThis.fetch !== "function") {
      throw new Error("fetch_not_available");
    }

    const response = await globalThis.fetch(
      `${backendBaseUrl.replace(/\/+$/, "")}${INTERNAL_EVENTS_PATH}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${internalEngineToken}`,
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
      console.warn("[push-watcher] notify:failed", {
        event_key: eventPayload.event_key || null,
        dedupe_key: eventPayload.dedupe_key || null,
        status: response.status,
        message: data?.error || data?.message || "backend_request_failed",
      });

      return {
        ok: false,
        status: response.status,
        reason: "backend_request_failed",
      };
    }

    console.log("[push-watcher] notify:done", {
      event_key: eventPayload.event_key || null,
      dedupe_key: eventPayload.dedupe_key || null,
      ok: response.ok,
      deduped: data?.deduped || false,
      sent: data?.sent || 0,
      failed: data?.failed || 0,
      skipped: data?.skipped || 0,
    });

    return {
      ok: true,
      status: response.status,
      data,
    };
  } catch (error) {
    console.warn("[push-watcher] notify:failed", {
      event_key: eventPayload.event_key || null,
      dedupe_key: eventPayload.dedupe_key || null,
      status: null,
      message: error?.message || "backend_request_failed",
    });

    return {
      ok: false,
      reason: "backend_request_failed",
    };
  }
}

module.exports = {
  notifyBackendEvent,
};
