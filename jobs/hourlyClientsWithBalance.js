import "dotenv/config";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getSupabaseAdmin,
  isSupabaseConfigured,
  runQuery,
  supabaseEnvStatus,
} from "../database/hobbiton/index.js";

const TABLE = "hourly_clients_with_balance";

export const HOURLY_CLIENTS_WITH_BALANCE_SQL = `
SELECT COUNT(*)::text AS clients
FROM partner_schema.integration_clients
WHERE COALESCE(current_balance, 0) > 0
`.trim();

function toBigIntCol(value) {
  if (value == null || value === "") return null;
  const n = Number.parseInt(String(value), 10);
  return Number.isNaN(n) ? null : n;
}

/** Partner Postgres → Supabase \`hourly_clients_with_balance\` (\`clients\` = count with balance > 0). */
export async function runHourlyClientsWithBalance() {
  const result = await runQuery(HOURLY_CLIENTS_WITH_BALANCE_SQL);
  const row = result.rows[0] ?? {};
  const clients = toBigIntCol(row.clients);

  console.log(
    `[hourly] clients with balance > 0: ${clients ?? "null"} (${new Date().toISOString()})`
  );

  if (!isSupabaseConfigured()) {
    console.log("Supabase skipped — set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env");
    console.log("Env check:", supabaseEnvStatus());
    return;
  }

  const supabase = getSupabaseAdmin();
  const { error } = await supabase.from(TABLE).insert({ clients });

  if (error) {
    console.log(`${TABLE} insert:`, error.message);
  } else {
    console.log(`saved to public.${TABLE} clients=${clients}`);
  }
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
