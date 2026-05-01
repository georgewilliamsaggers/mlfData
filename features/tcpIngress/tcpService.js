import {
  getSupabaseAdmin,
  isSupabaseConfigured,
} from "../../database/hobbiton/index.js";

const TABLE = "tcp";

export class TcpIngressValidationError extends Error {
  /** @param {string} message */
  constructor(message) {
    super(message);
    this.name = "TcpIngressValidationError";
  }
}

/**
 * Maps request fields to columns: textData→streamed_data, jsonData→json, deviceId→Device_Id.
 * @param {{ textData?: unknown, jsonData?: unknown, deviceId?: unknown }} body
 */
export async function saveTcpIngress(body) {
  if (!isSupabaseConfigured()) {
    const err = new Error("Supabase is not configured");
    err.code = "SUPABASE_UNAVAILABLE";
    throw err;
  }

  const textData =
    body.textData === undefined || body.textData === null
      ? null
      : String(body.textData);

  /** Postgres column is quoted `"Device_Id"` — PostgREST expects this key casing. */
  const Device_Id =
    body.deviceId === undefined || body.deviceId === null
      ? null
      : String(body.deviceId);

  let jsonColumn = null;
  if (body.jsonData !== undefined && body.jsonData !== null) {
    if (typeof body.jsonData === "string") {
      try {
        jsonColumn = JSON.parse(body.jsonData);
      } catch {
        throw new TcpIngressValidationError(
          "jsonData must be valid JSON when sent as a string"
        );
      }
    } else {
      jsonColumn = body.jsonData;
    }
  }

  const supabase = getSupabaseAdmin();
  const { error } = await supabase.from(TABLE).insert({
    streamed_data: textData,
    json: jsonColumn,
    Device_Id,
  });

  if (error) {
    const err = new Error(error.message);
    err.code = "SUPABASE_INSERT";
    throw err;
  }
}
