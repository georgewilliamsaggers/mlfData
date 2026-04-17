import { runQuery } from "../database/hobbiton/index.js";
import { buildClientSnapshotSql } from "../jobs/dailyClient.js";

/**
 * Live client metrics (no persistence). Used by GET /api/data/clientSnapshot.
 */
export async function getClientSnapshot() {
  const dateKey = new Date().toISOString().slice(0, 10);

  const result = await runQuery(buildClientSnapshotSql());
  const row = result.rows[0] ?? {};

  return {
    snapshotDate: dateKey,
    generatedAt: new Date().toISOString(),
    totalClients: row.total_clients ?? null,
    clientsWithBalanceGtZero: row.clients_with_balance_gt_zero ?? null,
    clientsWithZeroBalance: row.clients_with_zero_balance ?? null,
    totalFundValue: row.total_fund_value ?? null,
  };
}
