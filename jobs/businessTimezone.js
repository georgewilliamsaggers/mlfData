/**
 * IANA timezone for “which day is yesterday?” and midnight cron.
 * When cron runs at 00:00 here, “yesterday” means the local calendar date that just ended.
 * Central Africa Time (UTC+2): Africa/Harare (also used for Malawi, Zambia, etc.).
 * Override with CRON_TIMEZONE in .env if needed.
 */
export const BUSINESS_TIMEZONE =
  process.env.CRON_TIMEZONE || "Africa/Harare";

/**
 * Local calendar date the metrics are *for* when the job runs at 00:00 in `timezone`:
 * the full day that just ended (not “today” that just started). ISO date string YYYY-MM-DD.
 */
export function reportingForDate(timezone = BUSINESS_TIMEZONE) {
  const today = new Date().toLocaleDateString("en-CA", { timeZone: timezone });
  const [y, m, d] = today.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() - 1);
  return dt.toISOString().slice(0, 10);
}
