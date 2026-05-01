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
 * Table `tcp` only has `streamed_data` and `json`. `deviceId` is stored inside `json`
 * alongside parsed `jsonData` when present.
 *
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

  const deviceId =
    body.deviceId === undefined || body.deviceId === null
      ? null
      : String(body.deviceId);

  let jsonColumn = null;
  if (deviceId !== null || body.jsonData !== undefined) {
    jsonColumn = {};
    if (deviceId !== null) {
      jsonColumn.deviceId = deviceId;
    }
    if (body.jsonData !== undefined && body.jsonData !== null) {
      if (typeof body.jsonData === "string") {
        try {
          jsonColumn.data = JSON.parse(body.jsonData);
        } catch {
          throw new TcpIngressValidationError("jsonData must be valid JSON when sent as a string");
        }
      } else {
        jsonColumn.data = body.jsonData;
      }
    }
    if (Object.keys(jsonColumn).length === 0) {
      jsonColumn = null;
    }
  }

  const supabase = getSupabaseAdmin();
  const { error } = await supabase.from(TABLE).insert({
    streamed_data: textData,
    json: jsonColumn,
  });

  if (error) {
    const err = new Error(error.message);
    err.code = "SUPABASE_INSERT";
    throw err;
  }
}
