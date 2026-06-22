"use strict";

const {
  notifyBackendEvent,
} = require("../services/internalNotificationsClient");

async function testPushSingleDeviceDryRun() {
  if (process.env.PUSH_WATCHER_DRY_RUN !== "true") {
    return {
      ok: false,
      blocked: true,
      reason: "push_test_dry_run_required",
    };
  }

  return notifyBackendEvent({
    event_key: "PUSH_TEST_ENGINE_DRY_RUN",
    category: "test",
    title: "New Store",
    body: "Teste dry-run do engine.",
    url: "/me",
    dedupe_key: "dry-run:engine:push:test",
    subscription_id: process.env.PUSH_TEST_SUBSCRIPTION_ID || null,
    test_label: process.env.ENGINE_PUSH_TEST_PHONE_LABEL || "43998640480",
    payload: {
      test_only: true,
      dry_run: true,
    },
  });
}

if (require.main === module) {
  testPushSingleDeviceDryRun()
    .then((result) => {
      console.log("[push-watcher] test-dry-run:finished", {
        ok: result?.ok === true,
        skipped: result?.skipped === true,
        dry_run: result?.dry_run === true,
        blocked: result?.blocked === true,
        reason: result?.reason || null,
      });
      process.exit(0);
    })
    .catch((err) => {
      console.error("[push-watcher] test-dry-run:fatal", {
        message: err?.message || null,
        stack: err?.stack || null,
      });
      process.exit(1);
    });
}

module.exports = {
  testPushSingleDeviceDryRun,
};
