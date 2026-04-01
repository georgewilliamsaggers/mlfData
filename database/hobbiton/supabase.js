import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

/** Singleton: admin client using the service role key (bypasses RLS; server-only). */
let adminClient;

/**
 * True when URL + service role key are present. Use before calling {@link getSupabaseAdmin}.
 */
function getServiceRoleKey() {
  return (
    process.env.SUPABASE_SERVICE_ROLE_KEY?.trim() ||
    process.env.SUPABASE_SERVICE_KEY?.trim()
  );
}

export function isSupabaseConfigured() {
  return Boolean(process.env.SUPABASE_URL?.trim() && getServiceRoleKey());
}

/** For debugging: which pieces are missing (no secrets logged). */
export function supabaseEnvStatus() {
  return {
    hasUrl: Boolean(process.env.SUPABASE_URL?.trim()),
    hasServiceRoleKey: Boolean(getServiceRoleKey()),
    hint: "Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (Railway Variables or .env)",
  };
}

/**
 * Admin connection to your Supabase project (full access via service role).
 * Use only in server code / jobs — never expose `SUPABASE_SERVICE_ROLE_KEY` to the browser.
 *
 * Dashboard: Project Settings → API → `Project URL` + `service_role` (secret).
 */
export function getSupabaseAdmin() {
  if (!isSupabaseConfigured()) {
    throw new Error(
      "Supabase admin: set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env (Project Settings → API)."
    );
  }
  if (!adminClient) {
    const url = process.env.SUPABASE_URL.trim();
    const serviceRoleKey = getServiceRoleKey();

    adminClient = createClient(url, serviceRoleKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
        detectSessionInUrl: false,
      },
    });
  }
  return adminClient;
}

/** Same client — alias for clarity in code that talks to Postgres via Supabase. */
export function getSupabaseServiceRoleClient() {
  return getSupabaseAdmin();
}

/**
 * Verifies the admin key can talk to Supabase (lists Storage buckets; no tables required).
 * @returns {{ ok: true, bucketCount: number } | { ok: false, error: string }}
 */
export async function verifySupabaseAdminConnection() {
  if (!isSupabaseConfigured()) {
    return { ok: false, error: "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing" };
  }
  try {
    const supabase = getSupabaseAdmin();
    const { data, error } = await supabase.storage.listBuckets();
    if (error) {
      return { ok: false, error: error.message };
    }
    return { ok: true, bucketCount: data?.length ?? 0 };
  } catch (err) {
    return { ok: false, error: err.message ?? String(err) };
  }
}

/**
 * Upload JSON to Supabase Storage. Create the bucket in Dashboard → Storage first.
 * @param {string} objectPath path inside bucket, e.g. client_snapshots/2026-04-01.json
 * @param {unknown} data serializable object
 * @param {string} [bucket] defaults to SUPABASE_STORAGE_BUCKET or "snapshots"
 */
export async function uploadJsonToStorage(objectPath, data, bucket) {
  const bucketName =
    bucket || process.env.SUPABASE_STORAGE_BUCKET || "snapshots";
  const supabase = getSupabaseAdmin();
  const json = JSON.stringify(data, null, 2);
  const buf = Buffer.from(json, "utf8");

  const { error } = await supabase.storage.from(bucketName).upload(objectPath, buf, {
    contentType: "application/json",
    upsert: true,
  });

  if (error) {
    throw error;
  }
}
