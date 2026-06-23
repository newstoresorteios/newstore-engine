"use strict";

const INTERNAL_EVENTS_PATH = "/api/internal/notifications/events";

function hasOwn(object, key) {
  return Object.prototype.hasOwnProperty.call(object, key);
}

function blocked(reason, eventPayload) {
  console.warn("[push-watcher] notify:blocked", {
    event_key: eventPayload?.event_key || null,
    dedupe_key: eventPayload?.dedupe_key || null,
    reason,
  });

  return {
    ok: false,
    blocked: true,
    reason,
  };
}

function resolveEffectiveDryRun(eventPayload) {
  if (process.env.ENGINE_PUSH_WATCHER_DRY_RUN === "true") {
    return true;
  }

  return eventPayload.dry_run === true;
}

async function notifyBackendEvent(payload) {
  const eventPayload = payload && typeof payload === "object" ? { ...payload } : {};
  const effectiveDryRun = resolveEffectiveDryRun(eventPayload);

  if (process.env.ENGINE_PUSH_WATCHER_ENABLED !== "true") {
    console.log("[push-watcher] disabled");
    return {
      ok: true,
      skipped: true,
      reason: "engine_push_watcher_disabled",
    };
  }

  const backendBaseUrl = String(
    process.env.BACKEND_INTERNAL_BASE_URL || "",
  ).trim();
  const internalEngineToken = String(
    process.env.INTERNAL_ENGINE_TOKEN || "",
  ).trim();

  if (!backendBaseUrl || !internalEngineToken) {
    console.error("[push-watcher] backend config missing", {
      has_base_url: Boolean(backendBaseUrl),
      has_token: Boolean(internalEngineToken),
    });
    return blocked("backend_internal_config_missing", eventPayload);
  }

  if (
    hasOwn(eventPayload, "audience") &&
    process.env.ENGINE_PUSH_ALLOW_PRODUCTION_AUDIENCE !== "true"
  ) {
    return blocked("engine_audience_blocked", eventPayload);
  }

  if (hasOwn(eventPayload, "user_id")) {
    if (process.env.ENGINE_PUSH_ALLOW_SINGLE_TEST_SEND !== "true") {
      if (!effectiveDryRun) {
        return blocked("engine_single_send_blocked", eventPayload);
      }
    } else if (!effectiveDryRun) {
      const testUserId = Number(process.env.ENGINE_PUSH_TEST_USER_ID);
      const requestedUserId = Number(eventPayload.user_id);

      if (
        !Number.isFinite(testUserId) ||
        !Number.isFinite(requestedUserId) ||
        requestedUserId !== testUserId
      ) {
        return blocked("engine_single_send_blocked", eventPayload);
      }
    }
  }

  const channels = Array.isArray(eventPayload.channels)
    ? eventPayload.channels
    : [];

  if (
    channels.some(
      (channel) => String(channel || "").trim().toLowerCase() === "whatsapp",
    )
  ) {
    return blocked("whatsapp_blocked_in_engine", eventPayload);
  }

  if (String(eventPayload.provider || "").trim().toLowerCase() === "brevo") {
    return blocked("brevo_blocked_in_engine", eventPayload);
  }

  const requestBody = {
    ...eventPayload,
    dry_run: effectiveDryRun,
  };

  console.log("[push-watcher] notify:start", {
    event_key: requestBody.event_key || null,
    dedupe_key: requestBody.dedupe_key || null,
    category: requestBody.category || null,
    entity_type: requestBody.entity_type || null,
    entity_id: requestBody.entity_id || null,
    has_user_id: Boolean(requestBody.user_id),
    audience_type: requestBody.audience?.type || null,
    dry_run: effectiveDryRun,
  });

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
        body: JSON.stringify(requestBody),
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
        event_key: requestBody.event_key || null,
        dedupe_key: requestBody.dedupe_key || null,
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
      event_key: requestBody.event_key || null,
      dedupe_key: requestBody.dedupe_key || null,
      ok: data?.ok || false,
      deduped: data?.deduped || false,
      sent: data?.sent || 0,
      failed: data?.failed || 0,
      skipped: data?.skipped || 0,
      blocked: data?.blocked || false,
    });

    return {
      ok: true,
      status: response.status,
      data,
    };
  } catch (error) {
    console.warn("[push-watcher] notify:failed", {
      event_key: requestBody.event_key || null,
      dedupe_key: requestBody.dedupe_key || null,
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
