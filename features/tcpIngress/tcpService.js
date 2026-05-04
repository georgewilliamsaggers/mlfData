import {
  getSupabaseAdmin,
  isSupabaseConfigured,
} from "../../database/hobbiton/index.js";

export class TcpIngressValidationError extends Error {
  /** @param {string} message */
  constructor(message) {
    super(message);
    this.name = "TcpIngressValidationError";
  }
}

const TABLE = "tcpV2";

/**
 * Inserts request fields directly into `public."tcpV2"`.
 * @param {{
 *   protocol?: unknown,
 *   receivedAt?: unknown,
 *   connectionId?: unknown,
 *   remoteAddress?: unknown,
 *   deviceId?: unknown,
 *   originalMessageHex?: unknown,
 *   decodedMessage?: unknown
 * }} body
 */
export async function saveTcpIngress(body) {
  if (!isSupabaseConfigured()) {
    const err = new Error("Supabase is not configured");
    err.code = "SUPABASE_UNAVAILABLE";
    throw err;
  }

  const supabase = getSupabaseAdmin();
  const { error } = await supabase.from(TABLE).insert({
    protocol: body.protocol ?? null,
    receivedAt: body.receivedAt ?? null,
    connectionId: body.connectionId ?? null,
    remoteAddress: body.remoteAddress ?? null,
    deviceId: body.deviceId ?? null,
    originalMessageHex: body.originalMessageHex ?? null,
    decodedMessage: body.decodedMessage ?? null,
  });

  if (error) {
    const err = new Error(error.message);
    err.code = "SUPABASE_INSERT";
    throw err;
  }
}
