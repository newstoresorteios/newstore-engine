"use strict";

const { Pool } = require("pg");

const ALLOWED_PG_QUERY_PARAMS = new Set([
  "sslmode",
  "ssl",
  "sslrootcert",
  "connect_timeout",
  "target_session_attrs",
  "application_name",
  "options",
]);

let pool = null;

function cleanPgUrl(url) {
  if (!url) {
    return url;
  }

  const parsed = new URL(url);
  const filtered = new URLSearchParams();

  for (const [key, value] of parsed.searchParams.entries()) {
    if (ALLOWED_PG_QUERY_PARAMS.has(key)) {
      filtered.set(key, value);
    }
  }

  parsed.search = filtered.toString();
  return parsed.toString();
}

function getPool() {
  if (pool) {
    return pool;
  }

  const postgresUrl = cleanPgUrl(String(process.env.POSTGRES_URL || "").trim());
  if (!postgresUrl) {
    throw new Error("POSTGRES_URL is required for push watcher database queries");
  }

  pool = new Pool({
    connectionString: postgresUrl,
    ssl: { rejectUnauthorized: false },
  });

  return pool;
}

async function getOpenDraws() {
  const client = getPool();
  const result = await client.query(`
    SELECT id, opened_at
      FROM draws
     WHERE status = 'open'
     ORDER BY id ASC
  `);

  return result.rows || [];
}

async function getReservationColumns() {
  const client = getPool();
  const result = await client.query(`
    SELECT column_name, data_type
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'reservations'
  `);

  return Object.fromEntries(
    (result.rows || []).map((row) => [row.column_name, row.data_type]),
  );
}

async function getSoldCount(drawId) {
  const cols = await getReservationColumns();
  const client = getPool();

  if (Object.prototype.hasOwnProperty.call(cols, "number")) {
    const result = await client.query(
      `
        SELECT COUNT(DISTINCT r.number) AS sold
          FROM reservations r
     LEFT JOIN payments p ON p.id = r.payment_id
         WHERE r.draw_id = $1
           AND (r.status = 'paid' OR p.status IN ('approved','paid'))
      `,
      [drawId],
    );
    return Number(result.rows[0]?.sold || 0);
  }

  if (Object.prototype.hasOwnProperty.call(cols, "numbers")) {
    const result = await client.query(
      `
        WITH flat AS (
          SELECT UNNEST(r.numbers) AS num
            FROM reservations r
       LEFT JOIN payments p ON p.id = r.payment_id
           WHERE r.draw_id = $1
             AND (r.status = 'paid' OR p.status IN ('approved','paid'))
        )
        SELECT COUNT(DISTINCT num) AS sold FROM flat
      `,
      [drawId],
    );
    return Number(result.rows[0]?.sold || 0);
  }

  throw new Error("Tabela reservations não possui colunas 'number' nem 'numbers'.");
}

async function getTotalSlots() {
  const client = getPool();
  const kv = {};

  try {
    const appConfig = await client.query("SELECT key, value FROM app_config");
    for (const row of appConfig.rows || []) {
      kv[String(row.key || "").trim().toLowerCase()] = row.value;
    }
  } catch (_) {
    // app_config may not exist in all environments
  }

  try {
    const kvStore = await client.query("SELECT k, v FROM kv_store");
    for (const row of kvStore.rows || []) {
      kv[String(row.k || "").trim().toLowerCase()] = row.v;
    }
  } catch (_) {
    // kv_store may not exist in all environments
  }

  for (const key of [
    "total_numbers",
    "ticket_count",
    "ticket_total",
    "max_number",
    "range_max",
  ]) {
    const value = kv[key];
    if (value == null) {
      continue;
    }

    const parsed = Number.parseInt(String(value), 10);
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
  }

  return 100;
}

async function getUsersWithBalanceExpiringOn(daysList) {
  const client = getPool();
  const result = await client.query(
    `
      SELECT
        id,
        balance_cents,
        coupon_expires_at,
        (coupon_expires_at::date - CURRENT_DATE) AS days_until
      FROM users
     WHERE balance_cents > 0
       AND coupon_expires_at IS NOT NULL
       AND (coupon_expires_at::date - CURRENT_DATE) = ANY($1::int[])
     ORDER BY id ASC
    `,
    [daysList],
  );

  return result.rows || [];
}

function formatDateKey(value) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "unknown";
  }

  return date.toISOString().slice(0, 10);
}

async function closePool() {
  if (!pool) {
    return;
  }

  await pool.end();
  pool = null;
}

module.exports = {
  getOpenDraws,
  getSoldCount,
  getTotalSlots,
  getUsersWithBalanceExpiringOn,
  formatDateKey,
  closePool,
};
