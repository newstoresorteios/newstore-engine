"use strict";

const {
  notifyBackendEvent,
} = require("../services/internalNotificationsClient");

async function checkDrawOpenedDryRun() {
  return notifyBackendEvent({
    event_key: "DRAW_OPENED",
    category: "marketing",
    title: "New Store",
    body: "Novo aviso disponível na sua conta.",
    url: "/me",
    dedupe_key: "dry-run:draw:opened",
    entity_type: "draw",
    entity_id: "dry-run",
    subscription_id: process.env.PUSH_TEST_SUBSCRIPTION_ID || null,
    payload: {
      dry_run: true,
    },
  });
}

async function checkDrawProgress50DryRun() {
  return notifyBackendEvent({
    event_key: "DRAW_PROGRESS_50",
    category: "marketing",
    title: "New Store",
    body: "Um sorteio que você acompanha recebeu uma atualização.",
    url: "/me",
    dedupe_key: "dry-run:draw:progress:50",
    entity_type: "draw",
    entity_id: "dry-run",
    subscription_id: process.env.PUSH_TEST_SUBSCRIPTION_ID || null,
    payload: {
      dry_run: true,
    },
  });
}

async function checkBalanceExpiringDryRun() {
  return notifyBackendEvent({
    event_key: "BALANCE_EXPIRING",
    category: "operational",
    title: "New Store",
    body: "Seu saldo vence em alguns dias.",
    url: "/me",
    dedupe_key: "dry-run:balance:expiring",
    entity_type: "user_balance",
    entity_id: "dry-run",
    subscription_id: process.env.PUSH_TEST_SUBSCRIPTION_ID || null,
    payload: {
      dry_run: true,
    },
  });
}

async function runPushNotificationWatcher() {
  if (process.env.ENGINE_PUSH_WATCHER_ENABLED !== "true") {
    console.log("[push-watcher] disabled");
  } else if (process.env.PUSH_WATCHER_DRY_RUN === "true") {
    console.log("[push-watcher] dry-run");
  } else {
    console.log("[push-watcher] disabled or dry-run only in this phase");
    return [];
  }

  return Promise.all([
    checkDrawOpenedDryRun(),
    checkDrawProgress50DryRun(),
    checkBalanceExpiringDryRun(),
  ]);
}

if (require.main === module) {
  runPushNotificationWatcher()
    .then(() => {
      console.log("[push-watcher] finished");
      process.exit(0);
    })
    .catch((err) => {
      console.error("[push-watcher] fatal", {
        message: err?.message || null,
        stack: err?.stack || null,
      });
      process.exit(1);
    });
}

module.exports = {
  checkDrawOpenedDryRun,
  checkDrawProgress50DryRun,
  checkBalanceExpiringDryRun,
  runPushNotificationWatcher,
};
