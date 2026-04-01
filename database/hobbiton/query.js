/**
 * CLI helper — in code use: import { query } from "./index.js"
 *
 * Usage:
 *   node database/hobbiton/query.js "SELECT * FROM integration_accounts LIMIT 5"
 * Or set QUERY in .env for a default query.
 */
import { query } from "./db.js";

const sql = process.argv[2] || process.env.QUERY;

if (!sql?.trim()) {
  console.error(
    'Usage: node database/hobbiton/query.js "SELECT ..."\nOr set QUERY in .env for a default query.'
  );
  process.exit(1);
}

try {
  const start = Date.now();
  const result = await query(sql);
  const ms = Date.now() - start;

  console.error(`Rows: ${result.rowCount}, ${ms}ms`);
} catch (err) {
  console.error(err.message);
  process.exitCode = 1;
}
