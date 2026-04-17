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

const TABLE = "transaction_value_metrics";

/** 50-wide steps through 1000, then 250-wide through 2500, then 2500+. */
export function getAmountBucketLabels() {
  const labels = [];
  for (let i = 1; i <= 20; i++) {
    const hi = i * 50;
    labels.push(`${hi - 50}-${hi}`);
  }
  for (let hi = 1250; hi <= 2500; hi += 250) {
    labels.push(`${hi - 250}-${hi}`);
  }
  labels.push("2500+");
  return labels;
}

function buildAmountBucketCaseSql(amtCol = "amt") {
  const whens = [];
  for (let i = 1; i <= 20; i++) {
    const hi = i * 50;
    whens.push(`WHEN ${amtCol} < ${hi} THEN '${hi - 50}-${hi}'`);
  }
  for (let hi = 1250; hi <= 2500; hi += 250) {
    whens.push(`WHEN ${amtCol} < ${hi} THEN '${hi - 250}-${hi}'`);
  }
  return `CASE
      ${whens.join("\n      ")}
      ELSE '2500+'
    END`;
}

function buildBucketDefsValuesSql() {
  return getAmountBucketLabels()
    .map((label, idx) => `('${label.replace(/'/g, "''")}'::text, ${idx + 1})`)
    .join(",\n    ");
}

/**
 * Same local “yesterday” window as dailyTrans + stats + value histograms.
 * Buckets: 50 increments 0–1000, then 250 increments to 2500, then 2500+ (all buckets in JSON).
 */
export function buildTransactionValueMetricsSql() {
  const s = getPgSchema();
  const bucketCase = buildAmountBucketCaseSql("amt");
  const bucketValues = buildBucketDefsValuesSql();

  return `
WITH bounds AS (
  SELECT
    (date_trunc('day', now() AT TIME ZONE $1)::timestamp AT TIME ZONE $1) - interval '1 day' AS day_start,
    (date_trunc('day', now() AT TIME ZONE $1)::timestamp AT TIME ZONE $1) AS day_end
),
day_tx AS (
  SELECT
    t.amount::numeric AS amt,
    LOWER(TRIM(t.type::text)) AS typ
  FROM ${s}.integration_transactions t
  WHERE LOWER(TRIM(t.status::text)) = 'successful'
    AND t.amount IS NOT NULL
    AND t.date >= (SELECT day_start FROM bounds)
    AND t.date < (SELECT day_end FROM bounds)
),
deposit_bucketed AS (
  SELECT
    ${bucketCase} AS range_label
  FROM day_tx
  WHERE typ = 'deposit'
),
withdraw_bucketed AS (
  SELECT
    ${bucketCase} AS range_label
  FROM day_tx
  WHERE typ IN ('withdraw', 'withdrawal')
),
bucket_defs AS (
  SELECT * FROM (VALUES
    ${bucketValues}
  ) AS v(range_label, ord)
),
dep_counts AS (
  SELECT range_label, COUNT(*)::bigint AS cnt
  FROM deposit_bucketed
  GROUP BY range_label
),
wit_counts AS (
  SELECT range_label, COUNT(*)::bigint AS cnt
  FROM withdraw_bucketed
  GROUP BY range_label
)
SELECT
  (SELECT AVG(amt) FROM day_tx WHERE typ = 'deposit')::float8::text AS avg_deposit,
  (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amt) FROM day_tx WHERE typ = 'deposit')::float8::text AS median_deposit,
  (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amt) FROM day_tx WHERE typ = 'deposit')::float8::text AS p90_deposit,
  (SELECT AVG(amt) FROM day_tx WHERE typ IN ('withdraw', 'withdrawal'))::float8::text AS avg_withdraw,
  (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amt) FROM day_tx WHERE typ IN ('withdraw', 'withdrawal'))::float8::text AS median_withdraw,
  (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amt) FROM day_tx WHERE typ IN ('withdraw', 'withdrawal'))::float8::text AS p90_withdraw,
  (
    SELECT COALESCE(
      jsonb_agg(
        jsonb_build_object('range', b.range_label, 'count', COALESCE(d.cnt, 0))
        ORDER BY b.ord
      ),
      '[]'::jsonb
    )
    FROM bucket_defs b
    LEFT JOIN dep_counts d ON d.range_label = b.range_label
  ) AS deposit_distribution,
  (
    SELECT COALESCE(
      jsonb_agg(
        jsonb_build_object('range', b.range_label, 'count', COALESCE(w.cnt, 0))
        ORDER BY b.ord
      ),
      '[]'::jsonb
    )
    FROM bucket_defs b
    LEFT JOIN wit_counts w ON w.range_label = b.range_label
  ) AS withdraw_distribution
`.trim();
}

function toDouble(value) {
  if (value == null || value === "") return null;
  const n = Number.parseFloat(String(value));
  return Number.isNaN(n) ? null : n;
}

/** @param {unknown} value */
function normalizeJsonArray(value) {
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

/** Successful txs for local yesterday → Supabase \`transaction_value_metrics\` (upsert on for_date). */
export async function runDailyTransactionValueMetrics(timezone = BUSINESS_TIMEZONE) {
  const forDate = reportingForDate(timezone);
  const sql = buildTransactionValueMetricsSql();
  const result = await runQuery(sql, [timezone]);
  console.log(
    JSON.stringify({ timezone, for_date: forDate, rows: result.rows }, null, 2)
  );

  const row = result.rows[0] ?? {};

  if (!isSupabaseConfigured()) {
    console.log("Supabase skipped — set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env");
    console.log("Env check:", supabaseEnvStatus());
    return;
  }

  const insertRow = {
    for_date: forDate,
    avg_deposit: toDouble(row.avg_deposit),
    median_deposit: toDouble(row.median_deposit),
    p90_deposit: toDouble(row.p90_deposit),
    avg_withdraw: toDouble(row.avg_withdraw),
    median_withdraw: toDouble(row.median_withdraw),
    p90_withdraw: toDouble(row.p90_withdraw),
    deposit_distribution: normalizeJsonArray(row.deposit_distribution),
    withdraw_distribution: normalizeJsonArray(row.withdraw_distribution),
  };

  const supabase = getSupabaseAdmin();
  const { error } = await supabase.from(TABLE).upsert(insertRow, {
    onConflict: "for_date",
  });

  if (error) {
    console.log(`${TABLE} upsert:`, error.message);
  } else {
    console.log(`saved to public.${TABLE} for_date=${forDate}`);
  }
}

const ranAsScript =
  process.argv[1] &&
  path.resolve(fileURLToPath(import.meta.url)) === path.resolve(process.argv[1]);

if (ranAsScript) {
  runDailyTransactionValueMetrics().catch((err) => {
    console.error(err.message);
    process.exitCode = 1;
  });
}