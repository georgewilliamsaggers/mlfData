import "dotenv/config";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getPgSchema,
  getSupabaseAdmin,
  isSupabaseConfigured,
  runQuery,
  supabaseEnvStatus,
} from "../database/hobbiton/index.js";
import { BUSINESS_TIMEZONE, reportingForDate } from "./businessTimezone.js";

export function buildClientSnapshotSql() {
  const s = getPgSchema();
  return `
SELECT
  COUNT(*)::text AS total_clients,
  COUNT(*) FILTER (WHERE COALESCE(current_balance, 0) > 0)::text AS clients_with_balance_gt_zero,
  COUNT(*) FILTER (WHERE COALESCE(current_balance, 0) = 0)::text AS clients_with_zero_balance,
  COALESCE(SUM(COALESCE(current_balance, 0)), 0)::float8::text AS total_fund_value
FROM ${s}.integration_clients
`.trim();
}

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

/** Partner Postgres → Supabase `dailyClients` (+ for_date = local day that just ended). */
export async function runDailyClient(
  sql = buildClientSnapshotSql(),
  timezone = BUSINESS_TIMEZONE
) {
  const forDate = reportingForDate(timezone);
  const result = await runQuery(sql);
  console.log(JSON.stringify({ for_date: forDate, rows: result.rows }, null, 2));
  const row = result.rows[0] ?? {};

  if (!isSupabaseConfigured()) {
    console.log("Supabase skipped — set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env");
    console.log("Env check:", supabaseEnvStatus());
    return;
  }

  const supabase = getSupabaseAdmin();
  const insertRow = {
    clients_with_balance: toBigIntCol(row.clients_with_balance_gt_zero),
    clients_without_balance: toBigIntCol(row.clients_with_zero_balance),
    total_clients: toBigIntCol(row.total_clients),
    total_fund_value: toDouble(row.total_fund_value),
    for_date: forDate,
  };

  const { error: insertError } = await supabase.from("dailyClients").insert(insertRow);
  if (insertError) {
    console.log("dailyClients insert:", insertError.message);
  } else {
    console.log(`saved to public.dailyClients for_date=${forDate}`);
  }
}

const ranAsScript =
  process.argv[1] &&
  path.resolve(fileURLToPath(import.meta.url)) === path.resolve(process.argv[1]);

if (ranAsScript) {
  runDailyClient().catch((err) => {
    console.error(err.message);
    process.exitCode = 1;
  });
}
