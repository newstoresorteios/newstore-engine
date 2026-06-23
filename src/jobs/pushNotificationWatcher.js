"use strict";

const {
  notifyBackendEvent,
} = require("../services/internalNotificationsClient");
const {
  getOpenDraws,
  getSoldCount,
  getTotalSlots,
  getUsersWithBalanceExpiringOn,
  formatDateKey,
  closePool,
} = require("../services/db");

function isWatcherEnabled() {
  return process.env.ENGINE_PUSH_WATCHER_ENABLED === "true";
}

async function checkDrawOpened() {
  if (!isWatcherEnabled()) {
    return;
  }

  const draws = await getOpenDraws();

  for (const draw of draws) {
    await notifyBackendEvent({
      event_key: "DRAW_OPENED",
      category: "marketing",
      title: "New Store",
      body: "Novo aviso disponível na sua conta.",
      url: `/me/draw/${draw.id}`,
      dedupe_key: `draw:${draw.id}:opened`,
      entity_type: "draw",
      entity_id: String(draw.id),
      audience: {
        type: "all_push_marketing_opt_in",
      },
      payload: {
        draw_id: draw.id,
      },
    });
  }
}

async function checkDrawProgress50() {
  if (!isWatcherEnabled()) {
    return;
  }

  const draws = await getOpenDraws();
  const totalNumbers = await getTotalSlots();

  for (const draw of draws) {
    const soldCount = await getSoldCount(draw.id);
    const soldPercent =
      totalNumbers > 0 ? (soldCount / totalNumbers) * 100 : 0;

    if (soldPercent < 50) {
      continue;
    }

    await notifyBackendEvent({
      event_key: "DRAW_PROGRESS_50",
      category: "marketing",
      title: "New Store",
      body: "Um sorteio que você acompanha recebeu uma atualização.",
      url: `/me/draw/${draw.id}`,
      dedupe_key: `draw:${draw.id}:progress:50`,
      entity_type: "draw",
      entity_id: String(draw.id),
      audience: {
        type: "all_push_marketing_opt_in",
      },
      payload: {
        draw_id: draw.id,
        sold_percent: Math.round(soldPercent),
        sold_count: soldCount,
        total_numbers: totalNumbers,
      },
    });
  }
}

async function checkBalanceExpiring() {
  if (!isWatcherEnabled()) {
    return;
  }

  const users = await getUsersWithBalanceExpiringOn([30, 7, 1]);

  for (const user of users) {
    const days = Number(user.days_until);
    const expiresAtKey = formatDateKey(user.coupon_expires_at);

    await notifyBackendEvent({
      event_key: "BALANCE_EXPIRING",
      category: "operational",
      title: "New Store",
      body: `Seu saldo vence em ${days} dia${days === 1 ? "" : "s"}.`,
      url: "/me",
      dedupe_key: `balance:${user.id}:expiring:${days}:${expiresAtKey}`,
      entity_type: "user_balance",
      entity_id: String(user.id),
      user_id: user.id,
      payload: {
        days,
        expires_at: user.coupon_expires_at,
      },
    });
  }
}

async function runPushNotificationWatcher() {
  console.log("[push-watcher] start", {
    enabled: process.env.ENGINE_PUSH_WATCHER_ENABLED,
    dry_run: process.env.ENGINE_PUSH_WATCHER_DRY_RUN,
    allow_audience: process.env.ENGINE_PUSH_ALLOW_PRODUCTION_AUDIENCE,
    allow_single_test: process.env.ENGINE_PUSH_ALLOW_SINGLE_TEST_SEND,
  });

  try {
    await checkDrawOpened();
    await checkDrawProgress50();
    await checkBalanceExpiring();
  } finally {
    await closePool();
  }

  console.log("[push-watcher] finished");
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
  checkDrawOpened,
  checkDrawProgress50,
  checkBalanceExpiring,
  runPushNotificationWatcher,
};
