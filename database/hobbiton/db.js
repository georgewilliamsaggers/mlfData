/**
 * Partner PostgreSQL: read-only (SELECT / views). This app must not assume INSERT/UPDATE/DELETE.
 */
import "dotenv/config";
import pg from "pg";

const { Pool } = pg;

function getSsl() {
  const mode = (process.env.PGSSL || "").toLowerCase();
  const host = process.env.PGHOST || "";
  const isLocal =
    host === "localhost" ||
    host === "127.0.0.1" ||
    host === "::1";

  if (mode === "disable" || mode === "false") {
    return undefined;
  }
  if (mode === "no-verify" || mode === "unsafe") {
    return { rejectUnauthorized: false };
  }
  if (mode === "true" || mode === "require" || mode === "verify-full") {
    return { rejectUnauthorized: true };
  }
  if (!isLocal && host) {
    return { rejectUnauthorized: true };
  }
  return undefined;
}

function getPoolConfig() {
  return {
    host: process.env.PGHOST,
    port: Number(process.env.PGPORT || 5432),
    database: process.env.PGDATABASE || "postgres",
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: getSsl(),
    connectionTimeoutMillis: Number(process.env.PGCONNECT_TIMEOUT_MS || 20000),
    max: Number(process.env.PGPOOL_MAX || 10),
  };
}

let pool;

function getPool() {
  if (pool) return pool;

  const missing = ["PGHOST", "PGUSER", "PGPASSWORD"].filter((k) => !process.env[k]);
  if (missing.length) {
    throw new Error(
      `Missing env: ${missing.join(", ")}. Add them to .env in the project root (next to package.json).`
    );
  }

  pool = new Pool(getPoolConfig());
  return pool;
}

/**
 * Schema for `integration_*` views (e.g. `mlf_schema` on DigitalOcean, or `partner_schema`).
 * Set `PGSCHEMA` in `.env`. Identifier-safe only.
 */
export function getPgSchema() {
  const schema = process.env.PGSCHEMA || "partner_schema";
  if (!/^[\w]+$/.test(schema)) {
    throw new Error("PGSCHEMA must contain only letters, digits, and underscores");
  }
  return schema;
}

/**
 * Run SQL with `PGSCHEMA` first on `search_path`, and qualify objects as `${getPgSchema()}.*` in queries.
 * (rows, rowCount, fields, command, etc.).
 *
 * @param {string} sqlText
 * @param {unknown[] | undefined} params Optional bound parameters ($1, $2, …)
 * @returns {Promise<import("pg").QueryResult>}
 */
export async function runQuery(sqlText, params) {
  const client = await getPool().connect();
  try {
    await client.query(`SET search_path TO ${getPgSchema()}, public`);
    if (params !== undefined) {
      return await client.query(sqlText, params);
    }
    return await client.query(sqlText);
  } finally {
    client.release();
  }
}

/**
 * Same as runQuery, but also console.logs the row set (pretty-printed JSON).
 *
 * @param {string} sqlText
 * @param {unknown[] | undefined} params Optional $1, $2, … bindings
 * @returns {Promise<import("pg").QueryResult>}
 */
export async function query(sqlText, params) {
  const result = await runQuery(sqlText, params);
  console.log(JSON.stringify(result.rows, null, 2));
  return result;
}
