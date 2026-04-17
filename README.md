# mlf-partner-db

Express API plus scheduled daily jobs: read PostgreSQL (`PGSCHEMA` / `integration_*` views, default `partner_schema`), write aggregates to Supabase.

## Run locally

```bash
cp .env.example .env
# fill in credentials
npm install
npm start
```

- Health: `GET /health`
- Cron runs at **00:00** in `CRON_TIMEZONE` (default `Africa/Harare`) when the server process is up.

## Railway

1. **New project → Deploy from GitHub** (or deploy this repo).
2. **Variables**: open the service **Variables** tab. Railway can **suggest or import** keys from **`.env.example`** in the repo (or paste the file into **Raw Editor** and fill secrets). Use the same names as in `.env.example`.
   - **PORT** is usually injected by Railway; leave blank unless you override.
   - Set **PG\*** (including **PGSCHEMA**, **PGSSL** for managed Postgres) and **SUPABASE\_\*** for jobs and admin checks.
3. **Start command**: `npm start` (also set in **`railway.toml`**).
4. **Health check**: **`/health`** (set in **`railway.toml`** `healthcheckPath`; confirm in service settings if needed).

The app listens on **`0.0.0.0`** and **`process.env.PORT`** so it works in Railway’s container network.

### Jobs

Jobs are triggered by **node-cron** inside the same Node process as the HTTP server. Keep **one long-running service** (not serverless) so midnight jobs execute. Manual one-off runs:

```bash
npm run job:daily-client
npm run job:daily-trans
npm run job:daily-active-users
npm run job:daily-transaction-value-metrics
```

## Requirements

- Node.js **20+** recommended (`package.json` `engines`).
