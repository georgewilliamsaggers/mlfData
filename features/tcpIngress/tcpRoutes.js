import { Router } from "express";
import {
  saveTcpIngress,
  TcpIngressValidationError,
} from "./tcpService.js";

const router = Router();

/**
 * POST /ingest (mounted under /api/tcp in server). API key enforced globally in server.js.
 * Body: { textData?, jsonData?, deviceId? } — all optional / nullable
 */
router.post("/ingest", async (req, res, next) => {
  try {
    const body =
      req.body && typeof req.body === "object" && !Array.isArray(req.body)
        ? req.body
        : {};

    await saveTcpIngress({
      textData: body.textData,
      jsonData: body.jsonData,
      deviceId: body.deviceId,
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

export default router;
