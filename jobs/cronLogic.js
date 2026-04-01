import cron from "node-cron";
import { BUSINESS_TIMEZONE } from "./businessTimezone.js";
import { runDailyClient } from "./dailyClient.js";
import { runDailyTrans } from "./dailyTrans.js";
import { runDailyActiveUsers } from "./dailyActiveUsers.js";
import { runDailyTransactionValueMetrics } from "./dailyTransactionValueMetrics.js";

/**
 * Runs every day at 00:00 in {@link BUSINESS_TIMEZONE} (default Central Africa / Harare).
 *
 * At that instant the clock has just started a **new** local calendar day — so “check for
 * **yesterday**”: day-scoped jobs use the local **full day that just ended** (yesterday
 * 00:00 → yesterday 23:59:59… as [start, end) in that zone). Transaction totals are for
 * that yesterday; active-user histograms roll back from **end of yesterday** (local midnight).
 *
 * dailyClient is a point-in-time snapshot when the job runs (same moment).
 * dailyTransactionValueMetrics: value stats + histograms for local yesterday → \`transaction_value_metrics\`.
 */
export function scheduleDailyJobs() {
  const tz = BUSINESS_TIMEZONE;

  cron.schedule(
    "0 0 * * *",
    async () => {
      console.log(`[cron] Midnight (${tz}) — reporting for local yesterday…`);
      try {
        await runDailyClient();
        console.log("[cron] dailyClient done");
      } catch (err) {
        console.error("[cron] dailyClient failed:", err.message);
      }
      try {
        await runDailyTrans();
        console.log("[cron] dailyTrans done");
      } catch (err) {
        console.error("[cron] dailyTrans failed:", err.message);
      }
      try {
        await runDailyActiveUsers();
        console.log("[cron] dailyActiveUsers done");
      } catch (err) {
        console.error("[cron] dailyActiveUsers failed:", err.message);
      }
      try {
        await runDailyTransactionValueMetrics();
        console.log("[cron] dailyTransactionValueMetrics done");
      } catch (err) {
        console.error("[cron] dailyTransactionValueMetrics failed:", err.message);
      }
    },
    { timezone: tz }
  );

  console.log(
    `[cron] Scheduled daily at 00:00 (${tz}): dailyClient + dailyTrans + dailyActiveUsers + dailyTransactionValueMetrics`
  );

  cron.schedule(
    "*/15 * * * *",
    () => {
      console.log("CRON alive!");
    },
    { timezone: tz }
  );
  console.log(`[cron] Heartbeat every 15 min (${tz})`);
}