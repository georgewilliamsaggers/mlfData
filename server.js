import "dotenv/config";
import express from "express";
import cors from "cors";
import {
  getPgSchema,
  runQuery,
  verifySupabaseAdminConnection,
} from "./database/hobbiton/index.js";
import { scheduleDailyJobs } from "./jobs/cronLogic.js";
import { chatWithAgent } from "./services/chatAgent.js";
import { getClientSnapshot } from "./services/clientSnapshot.js";
import { getActiveUserUsageLive } from "./services/activeUserUsageLive.js";
import { getHourlyClientsWithBalanceLast72Hours } from "./services/hourlyClientsWithBalanceApi.js";

const app = express();
const port = Number(process.env.PORT || 3000);

app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "1mb" }));

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "mlf-partner-db",
    health: "/health",
    time: new Date().toISOString(),
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "mlf-partner-db",
    time: new Date().toISOString(),
  });
});

/** Confirms Supabase admin (service role) can connect (lists Storage buckets). */
app.get("/api/admin/supabase", async (req, res, next) => {
  try {
    const result = await verifySupabaseAdminConnection();
    res.status(result.ok ? 200 : 503).json(result);
  } catch (err) {
    next(err);
  }
});

/** Live client snapshot (read-only, not stored). */
app.get("/api/data/clientSnapshot", async (req, res, next) => {
  try {
    const data = await getClientSnapshot();
    res.json(data);
  } catch (err) {
    next(err);
  }
});

/** Supabase `hourly_clients_with_balance`: last 72 hours (hour bucket + clients per row). */
app.get("/api/data/hourlyClientsWithBalance", async (req, res, next) => {
  try {
    const data = await getHourlyClientsWithBalanceLast72Hours();
    res.json(data);
  } catch (err) {
    if (err.statusCode === 503) {
      res.status(503).json({ error: err.message || "Supabase not configured" });
      return;
    }
    next(err);
  }
});

/** Active-user histogram (rolling N days ending **now** — not the cron/midnight window). */
const ACTIVE_USAGE_DAYS = [7, 30, 60, 90];
for (const days of ACTIVE_USAGE_DAYS) {
  app.get(`/api/data/${days}`, async (req, res, next) => {
    try {
      const data = await getActiveUserUsageLive(days);
      res.json(data);
    } catch (err) {
      next(err);
    }
  });
}

/** Example metric: total clients (read-only). */
app.get("/api/metrics/clients/count", async (req, res, next) => {
  try {
    const s = getPgSchema();
    const result = await runQuery(
      `SELECT COUNT(*)::text AS n FROM ${s}.integration_clients`
    );
    res.json({ count: result.rows[0]?.n ?? null });
  } catch (err) {
    next(err);
  }
});

/** Natural-language → SQL via OpenAI tool calling (read-only SELECT). */
app.post("/api/chat", async (req, res, next) => {
  try {
    const message = req.body?.message;
    const history = Array.isArray(req.body?.history) ? req.body.history : [];
    if (!message || typeof message !== "string") {
      res.status(400).json({ error: 'Body must include { "message": "..." }' });
      return;
    }
    const out = await chatWithAgent(message, history);
    res.json(out);
  } catch (err) {
    if (err.message?.includes("OPENAI_API_KEY")) {
      res.status(503).json({ error: err.message });
      return;
    }
    next(err);
  }
});

app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: err.message || "Internal error" });
});

app.listen(port, "0.0.0.0", () => {
  console.log(`HTTP listening on 0.0.0.0:${port}`);
  scheduleDailyJobs();
});
