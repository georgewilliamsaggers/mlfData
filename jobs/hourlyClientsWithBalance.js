import "dotenv/config";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getSupabaseAdmin,
  isSupabaseConfigured,
  runQuery,
  supabaseEnvStatus,
} from "../database/hobbiton/index.js";
import { BUSINESS_TIMEZONE } from "./businessTimezone.js";

const TABLE_CLIENTS = "hourly_clients_with_balance";
const TABLE_FUND = "hourly_fund_amount";

/** \`for_hour\` = start of the clock hour in $1 when the cron runs (same as fund job). */
export const HOURLY_CLIENTS_WITH_BALANCE_SQL = `
WITH bounds AS (
  SELECT (date_trunc('hour', now() AT TIME ZONE $1)::timestamp AT TIME ZONE $1) AS for_hour
)
SELECT
  COUNT(*)::text AS clients,
  (SELECT for_hour::text FROM bounds) AS for_hour
FROM partner_schema.integration_clients
WHERE COALESCE(current_balance, 0) > 0
`.trim();

/** Sum of all \`current_balance\`; \`for_hour\` = start of current clock hour in $1 (IANA). */
export const HOURLY_FUND_AMOUNT_SQL = `
WITH bounds AS (
  SELECT (date_trunc('hour', now() AT TIME ZONE $1)::timestamp AT TIME ZONE $1) AS for_hour
)
SELECT
  COALESCE(SUM(COALESCE(c.current_balance, 0)), 0)::float8::text AS fund_value,
  (SELECT for_hour::text FROM bounds) AS for_hour
FROM partner_schema.integration_clients c
`.trim();

function toBigIntCol(value) {
  if (value == null || value === "") return null;
  const n = Number.parseInt(String(value), 10);
  return Number.isNaN(n) ? null : n;
}

function toDouble(value) {
  if (value == null || value === "") return null;
  const n = Number.parseFloat(String(value));
  return Number.isNaN(n) ? null : n;
}

/** Partner Postgres → Supabase \`hourly_fund_amount\` (\`fund_value\` = sum of balances, \`for_hour\` = cron hour in business TZ). */
export async function runHourlyFundAmount(timezone = BUSINESS_TIMEZONE) {
  const result = await runQuery(HOURLY_FUND_AMOUNT_SQL, [timezone]);
  const row = result.rows[0] ?? {};
  const fundValue = toDouble(row.fund_value);
  const forHour = row.for_hour ?? null;

  console.log(
    `[hourly] total fund (sum current_balance): ${fundValue ?? "null"} for_hour=${forHour ?? "null"}`
  );

  if (!isSupabaseConfigured()) {
    return;
  }

  const supabase = getSupabaseAdmin();
  const { error } = await supabase.from(TABLE_FUND).insert({
    fund_value: fundValue,
    for_hour: forHour,
  });

  if (error) {
    console.log(`${TABLE_FUND} insert:`, error.message);
  } else {
    console.log(`saved to public.${TABLE_FUND} fund_value=${fundValue} for_hour=${forHour}`);
  }
}

/** Partner Postgres → Supabase \`hourly_clients_with_balance\` (\`clients\`, \`for_hour\`), then fund row. */
export async function runHourlyClientsWithBalance() {
  const tz = BUSINESS_TIMEZONE;
  const result = await runQuery(HOURLY_CLIENTS_WITH_BALANCE_SQL, [tz]);
  const row = result.rows[0] ?? {};
  const clients = toBigIntCol(row.clients);
  const forHour = row.for_hour ?? null;

  console.log(
    `[hourly] clients with balance > 0: ${clients ?? "null"} for_hour=${forHour ?? "null"}`
  );

  if (!isSupabaseConfigured()) {
    console.log("Supabase skipped — set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env");
    console.log("Env check:", supabaseEnvStatus());
    await runHourlyFundAmount(tz);
    return;
  }

  const supabase = getSupabaseAdmin();
  const { error } = await supabase.from(TABLE_CLIENTS).insert({
    clients,
    for_hour: forHour,
  });

  if (error) {
    console.log(`${TABLE_CLIENTS} insert:`, error.message);
  } else {
    console.log(
      `saved to public.${TABLE_CLIENTS} clients=${clients} for_hour=${forHour}`
    );
  }

  await runHourlyFundAmount(tz);
}

const ranAsScript =
  process.argv[1] &&
  path.resolve(fileURLToPath(import.meta.url)) === path.resolve(process.argv[1]);

if (ranAsScript) {
  runHourlyClientsWithBalance().catch((err) => {
    console.error(err.message);
    process.exitCode = 1;
  });
}
