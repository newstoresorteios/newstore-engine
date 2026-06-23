"use strict";

const {
  notifyBackendEvent,
} = require("../services/internalNotificationsClient");

function blocked(reason) {
  console.warn("[push-watcher] test-single:blocked", { reason });
  return {
    ok: false,
    blocked: true,
    reason,
  };
}

async function testPushSingleUser() {
  if (process.env.ENGINE_PUSH_WATCHER_ENABLED !== "true") {
    return blocked("engine_push_watcher_disabled");
  }

  if (process.env.ENGINE_PUSH_ALLOW_SINGLE_TEST_SEND !== "true") {
    return blocked("engine_single_send_blocked");
  }

  const testUserId = String(process.env.ENGINE_PUSH_TEST_USER_ID || "").trim();
  if (!testUserId) {
    return blocked("engine_push_test_user_id_missing");
  }

  if (process.env.ENGINE_PUSH_WATCHER_DRY_RUN === "true") {
    return blocked("engine_push_dry_run_required_false");
  }

  return notifyBackendEvent({
    event_key: "ENGINE_PUSH_TEST",
    category: "operational",
    title: "New Store",
    body: "Teste de Push enviado pelo engine.",
    url: "/me",
    dedupe_key: `engine:test:${testUserId}:${Date.now()}`,
    entity_type: "engine_test",
    entity_id: String(Date.now()),
    user_id: Number(testUserId),
    payload: {
      source: "engine_test",
    },
    dry_run: process.env.ENGINE_PUSH_WATCHER_DRY_RUN === "true",
  });
}

if (require.main === module) {
  testPushSingleUser()
    .then((result) => {
      console.log("[push-watcher] test-single:finished", {
        ok: result?.ok === true,
        blocked: result?.blocked === true,
        reason: result?.reason || null,
        sent: result?.data?.sent || 0,
      });
      process.exit(result?.ok === true ? 0 : 1);
    })
    .catch((err) => {
      console.error("[push-watcher] test-single:fatal", {
        message: err?.message || null,
        stack: err?.stack || null,
      });
      process.exit(1);
    });
}

module.exports = {
  testPushSingleUser,
};
