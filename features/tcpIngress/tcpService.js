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
const TYPE_LOCATION_PING = "Location Ping";
const TYPE_TERMINAL_AUTH = "Terminal authentication";
const TYPE_HEARTBEAT = "Heartbeat";
const TYPE_UNKNOWN = "Unknown";
const TYPE_ID_UNKNOWN = 0;
const TYPE_ID_LOCATION_PING = 1;
const TYPE_ID_HEARTBEAT = 2;
const TYPE_ID_TERMINAL_AUTH = 3;

/**
 * Classifies decoded JT808 message into DB `type` and `typeId`.
 * @param {unknown} decodedMessage
 * @returns {{ type: string, typeId: number }}
 */
function classifyTcpType(decodedMessage) {
  if (!decodedMessage || typeof decodedMessage !== "object") {
    return { type: TYPE_UNKNOWN, typeId: TYPE_ID_UNKNOWN };
  }

  const header = decodedMessage.header;
  const messageId =
    header && typeof header === "object" && typeof header.messageId === "string"
      ? header.messageId.trim().toLowerCase()
      : "";

  if (messageId === "0x0200") {
    return { type: TYPE_LOCATION_PING, typeId: TYPE_ID_LOCATION_PING };
  }
  if (messageId === "0x0102") {
    return { type: TYPE_TERMINAL_AUTH, typeId: TYPE_ID_TERMINAL_AUTH };
  }
  if (messageId === "0x0002") {
    return { type: TYPE_HEARTBEAT, typeId: TYPE_ID_HEARTBEAT };
  }

  return { type: TYPE_UNKNOWN, typeId: TYPE_ID_UNKNOWN };
}

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

  const { type, typeId } = classifyTcpType(body.decodedMessage);

  const supabase = getSupabaseAdmin();
  const { error } = await supabase.from(TABLE).insert({
    protocol: body.protocol ?? null,
    receivedAt: body.receivedAt ?? null,
    connectionId: body.connectionId ?? null,
    remoteAddress: body.remoteAddress ?? null,
    deviceId: body.deviceId ?? null,
    originalMessageHex: body.originalMessageHex ?? null,
    decodedMessage: body.decodedMessage ?? null,
    type,
    typeId,
  });

  if (error) {
    const err = new Error(error.message);
    err.code = "SUPABASE_INSERT";
    throw err;
  }
}

/**
 * Fetches tcp ingress rows with optional filters:
 * - created_at >= startDateTime
 * - created_at <= endDateTime
 * - deviceId exact match
 * - typeId exact match
 * @param {{
 *   startDateTime?: string,
 *   endDateTime?: string,
 *   deviceId?: string,
 *   typeId?: string | number
 * }} filters
 */
export async function getTcpIngressRecords(filters = {}) {
  if (!isSupabaseConfigured()) {
    const err = new Error("Supabase is not configured");
    err.code = "SUPABASE_UNAVAILABLE";
    throw err;
  }

  const startDateTime =
    typeof filters.startDateTime === "string" ? filters.startDateTime.trim() : "";
  const endDateTime =
    typeof filters.endDateTime === "string" ? filters.endDateTime.trim() : "";
  const deviceId =
    typeof filters.deviceId === "string" ? filters.deviceId.trim() : "";
  const typeIdRaw =
    typeof filters.typeId === "number"
      ? String(filters.typeId)
      : typeof filters.typeId === "string"
        ? filters.typeId.trim()
        : "";

  if (startDateTime && Number.isNaN(Date.parse(startDateTime))) {
    throw new TcpIngressValidationError("startDateTime must be a valid date-time");
  }
  if (endDateTime && Number.isNaN(Date.parse(endDateTime))) {
    throw new TcpIngressValidationError("endDateTime must be a valid date-time");
  }
  if (typeIdRaw && !/^\d+$/.test(typeIdRaw)) {
    throw new TcpIngressValidationError("typeId must be an integer");
  }
  const typeId = typeIdRaw ? Number.parseInt(typeIdRaw, 10) : null;

  let query = getSupabaseAdmin().from(TABLE).select("*").order("id", { ascending: false });

  if (startDateTime) {
    query = query.gte("created_at", startDateTime);
  }
  if (endDateTime) {
    query = query.lte("created_at", endDateTime);
  }
  if (deviceId) {
    query = query.eq("deviceId", deviceId);
  }
  if (typeId !== null) {
    query = query.eq("typeId", typeId);
  }

  const { data, error } = await query;
  if (error) {
    const err = new Error(error.message);
    err.code = "SUPABASE_SELECT";
    throw err;
  }

  return data ?? [];
}

/**
 * Returns simplified location points from tcp records (typeId = 1).
 * @param {{
 *   startDateTime?: string,
 *   endDateTime?: string,
 *   deviceId?: string
 * }} filters
 */
export async function getTcpLocationPoints(filters = {}) {
  const records = await getTcpIngressRecords({
    startDateTime: filters.startDateTime,
    endDateTime: filters.endDateTime,
    deviceId: filters.deviceId,
    typeId: TYPE_ID_LOCATION_PING,
  });

  return records.map((row) => {
    const decoded = row?.decodedMessage && typeof row.decodedMessage === "object"
      ? row.decodedMessage
      : {};
    const parsed = decoded?.body?.parsed && typeof decoded.body.parsed === "object"
      ? decoded.body.parsed
      : {};
    const header = decoded?.header && typeof decoded.header === "object"
      ? decoded.header
      : {};

    const additionalItems = Array.isArray(parsed.additionalItems)
      ? parsed.additionalItems
      : [];
    const findById = (id) =>
      additionalItems.find((item) => item && typeof item === "object" && item.id === id) ?? {};

    const signalItem = findById("0x30");
    const satellitesItem = findById("0x31");
    const temperatureItem = findById("0x51");
    const batteryItem = findById("0xe1");

    const temperatures = Array.isArray(temperatureItem.temperaturesCelsius)
      ? temperatureItem.temperaturesCelsius
      : [];

    return {
      deviceId: row?.deviceId ?? header?.terminalId ?? null,
      timestamp: parsed?.timestamp ?? null,
      location: {
        lat: parsed?.latitude ?? null,
        lng: parsed?.longitude ?? null,
      },
      speed: parsed?.speedKph ?? null,
      heading: parsed?.directionDegrees ?? null,
      status: Array.isArray(parsed?.statusFlags?.active) ? parsed.statusFlags.active : [],
      alarms: Array.isArray(parsed?.alarmFlags?.active) ? parsed.alarmFlags.active : [],
      battery: batteryItem?.batteryPercent ?? null,
      signal: signalItem?.signalStrength ?? null,
      satellites: satellitesItem?.satelliteCount ?? null,
      temperature: temperatures.length > 0 ? temperatures[0] : null,
    };
  });
}
