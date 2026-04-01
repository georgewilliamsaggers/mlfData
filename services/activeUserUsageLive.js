import { runQuery } from "../database/hobbiton/index.js";

/**
 * Rolling window **[now − N days, now)** (not aligned to midnight). Same universe and
 * histogram shape as the daily cron job’s usage metrics, but for “as of this instant”.
 */
export function buildActiveUserUsageLiveSql() {
  return `
WITH client_counts AS (
  SELECT
    c.id,
    COUNT(t.id)::bigint AS txn_count
  FROM partner_schema.integration_clients c
  LEFT JOIN partner_schema.integration_transactions t
    ON t.client_id = c.id
    AND LOWER(TRIM(t.status::text)) = 'successful'
    AND t.date >= NOW() - ($1::integer * interval '1 day')
    AND t.date < NOW()
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

const ALLOWED_DAYS = new Set([7, 30, 60, 90]);

/**
 * @param {number} windowDays — must be 7, 30, 60, or 90
 */
export async function getActiveUserUsageLive(windowDays) {
  if (!ALLOWED_DAYS.has(windowDays)) {
    throw new Error("windowDays must be 7, 30, 60, or 90");
  }

  const sql = buildActiveUserUsageLiveSql();
  const result = await runQuery(sql, [windowDays]);
  const row = result.rows[0] ?? {};
  const asOf = new Date().toISOString();

  return {
    windowDays,
    asOf,
    distribution: normalizeDistribution(row.distribution),
  };
}
