"use strict";

const {
  notifyPushAutomationEvent,
} = require("../services/internalNotificationsClient");

async function testPushAutomationDryRun() {
  return notifyPushAutomationEvent({
    event_key: "NEW_DRAW_PUBLISHED",
    source: "engine-dry-run-script",
    metadata: {
      test: true,
      origin: "manual-script",
    },
  });
}

if (require.main === module) {
  testPushAutomationDryRun()
    .then((result) => {
      console.log("[push-automation] test-dry-run:finished", {
        ok: result?.ok === true,
        skipped: result?.skipped === true,
        blocked: result?.blocked === true,
        reason: result?.reason || null,
        status: result?.status || null,
      });
      process.exit(0);
    })
    .catch((err) => {
      console.error("[push-automation] test-dry-run:fatal", {
        message: err?.message || null,
        stack: err?.stack || null,
      });
      process.exit(1);
    });
}

module.exports = {
  testPushAutomationDryRun,
};
