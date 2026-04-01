import "dotenv/config";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getSupabaseAdmin,
  isSupabaseConfigured,
  runQuery,
  supabaseEnvStatus,
} from "../database/hobbiton/index.js";
import { BUSINESS_TIMEZONE, reportingForDate } from "./businessTimezone.js";

/**
 * Successful txs only; split by type (adjust enum strings in SQL if your DB differs).
 * $1 = IANA timezone: [local midnight yesterday, local midnight today) = day just finished.
 */
export function buildDailyTransSql() {
  return `
WITH bounds AS (
  SELECT
    (date_trunc('day', now() AT TIME ZONE $1)::timestamp AT TIME ZONE $1) - interval '1 day' AS day_start,
    (date_trunc('day', now() AT TIME ZONE $1)::timestamp AT TIME ZONE $1) AS day_end
)
SELECT
  (SELECT day_start FROM bounds)::text AS day_start,
  (SELECT day_end FROM bounds)::text AS day_end,
  COALESCE(SUM(CASE WHEN LOWER(TRIM(type::text)) IN ('deposit') THEN COALESCE(amount::numeric, 0) ELSE 0 END), 0)::float8::text AS total_deposit_value,
  COUNT(*) FILTER (WHERE LOWER(TRIM(type::text)) IN ('deposit'))::text AS total_deposit_count,
  COALESCE(SUM(CASE WHEN LOWER(TRIM(type::text)) IN ('withdraw', 'withdrawal') THEN COALESCE(amount::numeric, 0) ELSE 0 END), 0)::float8::text AS total_withdraw_value,
  COUNT(*) FILTER (WHERE LOWER(TRIM(type::text)) IN ('withdraw', 'withdrawal'))::text AS total_withdraw_count
FROM partner_schema.integration_transactions
WHERE LOWER(TRIM(status::text)) = 'successful'
  AND date >= (SELECT day_start FROM bounds)
  AND date < (SELECT day_end FROM bounds)
`.trim();
}

function toDouble(value) {
  if (value == null || value === "") return null;
  const n = Number.parseFloat(String(value));
  return Number.isNaN(n) ? null : n;
}

/** Partner Postgres (successful txs, previous local calendar day) → Supabase \`transactions\` (+ for_date). */
export async function runDailyTrans(timezone = BUSINESS_TIMEZONE) {
  const forDate = reportingForDate(timezone);
  const sql = buildDailyTransSql();

  const result = await runQuery(sql, [timezone]);
  console.log(JSON.stringify({ timezone, for_date: forDate, rows: result.rows }, null, 2));

  const row = result.rows[0] ?? {};

  if (!isSupabaseConfigured()) {
    console.log("Supabase skipped — set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env");
    console.log("Env check:", supabaseEnvStatus());
    return;
  }

  const supabase = getSupabaseAdmin();
  const insertRow = {
    total_deposit_value: toDouble(row.total_deposit_value),
    total_deposit_count: toDouble(row.total_deposit_count),
    total_withdraw_value: toDouble(row.total_withdraw_value),
    total_withdraw_count: toDouble(row.total_withdraw_count),
    for_date: forDate,
  };

  const { error: insertError } = await supabase.from("transactions").insert(insertRow);
  if (insertError) {
    console.log("transactions insert:", insertError.message);
  } else {
    console.log(`saved to public.transactions for_date=${forDate}`);
  }
}

const ranAsScript =
  process.argv[1] &&
  path.resolve(fileURLToPath(import.meta.url)) === path.resolve(process.argv[1]);

if (ranAsScript) {
  runDailyTrans().catch((err) => {
    console.error(err.message);
    process.exitCode = 1;
  });
}
