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

const USAGE_TABLE =
  process.env.SUPABASE_USAGE_TABLE ||
  process.env.SUPABASE_ACTIVE_USERS_TABLE ||
  "usage";

/**
 * Rolling window ending at local midnight (start of “today” in BUSINESS_TIMEZONE):
 * [local midnight today - N days, local midnight today). Successful txs per client, then histogram.
 * Universe = all rows in PGSCHEMA.integration_clients (clients with 0 txs appear in bucket 0).
 *
 * Supabase \`usage\`: active_user_7 / _30 / _60 / _90 (jsonb) + for_date (business “as of” day).
 * for_date = local calendar day that just ended (not created_at). Table: SUPABASE_USAGE_TABLE (default \`usage\`).
 *
 * Each jsonb column: [{ "transactions_made": 0, "count": 123 }, ...]
 */
export function buildActiveUserDistributionSql() {
  const s = getPgSchema();
  return `
WITH client_counts AS (
  SELECT
    c.id,
    COUNT(t.id)::bigint AS txn_count
  FROM ${s}.integration_clients c
  LEFT JOIN ${s}.integration_transactions t
    ON t.client_id = c.id
    AND LOWER(TRIM(t.status::text)) = 'successful'
    AND t.date >= (SELECT (date_trunc('day', now() AT TIME ZONE $1)::timestamp AT TIME ZONE $1) - ($2::integer * interval '1 day'))
    AND t.date < (SELECT (date_trunc('day', now() AT TIME ZONE $1)::timestamp AT TIME ZONE $1))
  GROUP BY c.id
),
hist AS (
  SELECT
    txn_count AS transactions_made,
    COUNT(*)::bigint AS count
  FROM client_counts
  GROUP BY txn_count
)
SELECT COALESCE(
  jsonb_agg(
    jsonb_build_object(
      'transactions_made', transactions_made,
      'count', count
    )
    ORDER BY transactions_made
  ),
  '[]'::jsonb
) AS distribution
FROM hist
`.trim();
}

/** @param {unknown} value */
function normalizeDistribution(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

/** Partner Postgres → Supabase \`usage\`: one row + for_date + four jsonb columns. */
export async function runDailyActiveUsers(timezone = BUSINESS_TIMEZONE) {
  const sql = buildActiveUserDistributionSql();
  const periods = [7, 30, 60, 90];
  const forDate = reportingForDate(timezone);

  /** @type {Record<string, unknown>} */
  const row = { for_date: forDate };
  for (const days of periods) {
    const result = await runQuery(sql, [timezone, days]);
    const r = result.rows[0] ?? {};
    const distribution = normalizeDistribution(r.distribution);
    const col = `active_user_${days}`;
    row[col] = distribution;
    console.log(
      JSON.stringify(
        {
          for_date: forDate,
          timezone,
          periodDays: days,
          column: col,
          buckets: distribution.length,
        },
        null,
        2
      )
    );
  }

  if (!isSupabaseConfigured()) {
    console.log("Supabase skipped — set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env");
    console.log("Env check:", supabaseEnvStatus());
    return;
  }

  const supabase = getSupabaseAdmin();
  const { error: insertError } = await supabase.from(USAGE_TABLE).insert(row);

  if (insertError) {
    console.log(`${USAGE_TABLE} insert:`, insertError.message);
  } else {
    console.log(`saved to public.${USAGE_TABLE} for_date=${forDate}`);
  }
}

const ranAsScript =
  process.argv[1] &&
  path.resolve(fileURLToPath(import.meta.url)) === path.resolve(process.argv[1]);

if (ranAsScript) {
  runDailyActiveUsers().catch((err) => {
    console.error(err.message);
    process.exitCode = 1;
  });
}


