"use strict";

const {
  notifyPushAutomationEvent,
} = require("../services/internalNotificationsClient");

async function checkPushAutomationDryRun() {
  return notifyPushAutomationEvent({
    event_key: "NEW_DRAW_PUBLISHED",
    source: "engine-dry-run-watcher",
    metadata: {
      test: true,
      origin: "watcher",
    },
  });
}

async function runPushNotificationWatcher() {
  if (process.env.PUSH_AUTOMATION_EVENTS_ENABLED !== "true") {
    console.log("[push-watcher] disabled");
  } else if (process.env.PUSH_AUTOMATION_DRY_RUN === "true") {
    console.log("[push-watcher] dry-run");
  } else {
    console.log("[push-watcher] dry-run required in this phase");
    return [];
  }

  return [await checkPushAutomationDryRun()];
}

if (require.main === module) {
  runPushNotificationWatcher()
    .then(() => process.exit(0))
    .catch((err) => {
      console.error("[push-watcher] fatal", {
        message: err?.message || null,
        stack: err?.stack || null,
      });
      process.exit(1);
    });
}

module.exports = {
  checkPushAutomationDryRun,
  runPushNotificationWatcher,
};
