import { getSupabaseAdmin, isSupabaseConfigured } from "../database/hobbiton/supabase.js";

const TABLE = "hourly_clients_with_balance";

/** Start of UTC hour for a timestamp (hour the sample is attributed to). */
export function utcHourStartIso(isoOrDate) {
  const d = new Date(isoOrDate);
  if (Number.isNaN(d.getTime())) return null;
  d.setUTCMinutes(0, 0, 0);
  return d.toISOString();
}

/**
 * Last 72 hours of rows from `hourly_clients_with_balance` (by `created_at`).
 * Each point: `hour` / `for_hour` (business hour the sample is for), `clients`, `created_at`.
 */
export async function getHourlyClientsWithBalanceLast72Hours() {
  if (!isSupabaseConfigured()) {
    const err = new Error("Supabase not configured");
    err.statusCode = 503;
    throw err;
  }

  const asOf = new Date();
  const from = new Date(asOf.getTime() - 72 * 60 * 60 * 1000);

  const supabase = getSupabaseAdmin();
  const { data, error } = await supabase
    .from(TABLE)
    .select("id, created_at, clients, for_hour")
    .gte("created_at", from.toISOString())
    .lte("created_at", asOf.toISOString())
    .order("created_at", { ascending: true });

  if (error) {
    const err = new Error(error.message);
    err.statusCode = 500;
    throw err;
  }

  const rows = Array.isArray(data) ? data : [];
  const points = rows.map((row) => {
    const createdAt = row.created_at;
    const forHourRaw = row.for_hour;
    const forHourIso =
      forHourRaw != null
        ? new Date(forHourRaw).toISOString()
        : null;
    const hour = forHourIso ?? utcHourStartIso(createdAt);
    const clients =
      row.clients == null ? null : Number(row.clients);
    return {
      hour,
      for_hour: forHourIso,
      clients: Number.isNaN(clients) ? null : clients,
      created_at: createdAt,
    };
  });

  for (const p of points) {
    console.log(
      `[hourlyClientsWithBalance] for_hour=${p.for_hour ?? p.hour} clients=${p.clients} (created_at=${p.created_at})`
    );
  }

  return {
    windowHours: 72,
    asOf: asOf.toISOString(),
    from: from.toISOString(),
    count: points.length,
    points,
  };
}
