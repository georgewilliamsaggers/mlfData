import { Router } from "express";
import {
  getTcpIngressRecords,
  saveTcpIngress,
  TcpIngressValidationError,
} from "./tcpService.js";

const router = Router();

/**
 * POST /ingest (mounted under /api/tcp in server). API key enforced globally in server.js.
 * Body:
 * {
 *   protocol: "jt808",
 *   receivedAt: string,
 *   connectionId: string,
 *   remoteAddress: string,
 *   deviceId: string | null,
 *   originalMessageHex: string,
 *   decodedMessage: JSON
 * }
 */
router.post("/ingest", async (req, res, next) => {
  try {
    const body =
      req.body && typeof req.body === "object" && !Array.isArray(req.body)
        ? req.body
        : {};

    await saveTcpIngress({
      protocol: body.protocol,
      receivedAt: body.receivedAt,
      connectionId: body.connectionId,
      remoteAddress: body.remoteAddress,
      deviceId: body.deviceId,
      originalMessageHex: body.originalMessageHex,
      decodedMessage: body.decodedMessage,
    });

    res.status(200).json({ ok: true, message: "saved ok" });
  } catch (err) {
    if (err instanceof TcpIngressValidationError) {
      res.status(400).json({ ok: false, error: err.message });
      return;
    }
    if (err.code === "SUPABASE_UNAVAILABLE") {
      res.status(503).json({ ok: false, error: err.message });
      return;
    }
    if (err.code === "SUPABASE_INSERT") {
      res.status(502).json({ ok: false, error: err.message });
      return;
    }
    next(err);
  }
});

/**
 * GET /ingest (mounted under /api/tcp in server).
 * Optional query params:
 * - startDateTime: ISO date-time string (filters created_at >= startDateTime)
 * - endDateTime: ISO date-time string (filters created_at <= endDateTime)
 * - deviceId: exact device match
 * - typeId: exact message type id match
 */
router.get("/ingest", async (req, res, next) => {
  try {
    const records = await getTcpIngressRecords({
      startDateTime:
        typeof req.query.startDateTime === "string"
          ? req.query.startDateTime
          : undefined,
      endDateTime:
        typeof req.query.endDateTime === "string"
          ? req.query.endDateTime
          : undefined,
      deviceId: typeof req.query.deviceId === "string" ? req.query.deviceId : undefined,
      typeId: typeof req.query.typeId === "string" ? req.query.typeId : undefined,
    });

    res.status(200).json({ ok: true, count: records.length, records });
  } catch (err) {
    if (err instanceof TcpIngressValidationError) {
      res.status(400).json({ ok: false, error: err.message });
      return;
    }
    if (err.code === "SUPABASE_UNAVAILABLE") {
      res.status(503).json({ ok: false, error: err.message });
      return;
    }
    if (err.code === "SUPABASE_SELECT") {
      res.status(502).json({ ok: false, error: err.message });
      return;
    }
    next(err);
  }
});

export default router;
