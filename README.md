# mlf-partner-db

Express API plus scheduled daily jobs: read partner PostgreSQL (`partner_schema` views), write aggregates to Supabase.

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
2. **Variables**: copy keys from `.env.example` and set values in Railway (no quotes needed for simple values).
   - **PORT** is injected by Railway; do not set it unless you know you need to override.
   - Set **PG\*** for the partner database and **SUPABASE\_\*** for Supabase. Without them, API routes that query Postgres or jobs that upsert to Supabase will fail or skip writes.
3. **Start command**: `npm start` (default from `package.json`).
4. **Health check path**: `/health` (optional in Railway service settings).

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
