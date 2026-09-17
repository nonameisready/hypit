# Trend worker

`@hypit/trend-worker` discovers or ingests public short-form video URLs, records repeated metric
snapshots in SQLite, ranks candidates by configurable viral velocity, downloads approved media with
Hypit's pinned `@hypit/yt-dlp` implementation, and uploads the original media plus JSON metadata to
Cloudflare R2.

The worker runs one batch and exits. It does not contain an infinite scheduler. Use the examples in
`deploy/` to invoke `pnpm trend:run` hourly (or from your existing scheduler).

## Setup

Copy `.env.example` to `services/trend-worker/.env` or export the variables in your shell. The CLI
automatically loads that service-local `.env` before parsing configuration, including when invoked
by `pnpm trend:run` from launchd. Existing shell or launchd variables take precedence over values in
the file. The worker reads environment variables directly; it does not persist credentials.
`R2_ENDPOINT` should be the S3-compatible R2 endpoint for the account.

The local database defaults to `data/trend-worker.sqlite`. Download staging lives below
`data/tmp/`. Set `DELETE_LOCAL_AFTER_UPLOAD=false` when retaining local copies is necessary.

## Public URL ingestion

With `TREND_PROVIDERS=manual`, put one URL per line in `data/trend-urls.jsonl`. Lines may also be JSON
objects with optional metadata and metrics:

```json
{"url":"https://www.tiktok.com/@creator/video/123","creator":"creator","caption":"example","metrics":{"views":1000,"likes":100,"comments":10,"shares":5}}
```

The CLI also accepts `--url <url>` one or more times. Manual input supports TikTok, Instagram Reel,
and YouTube/Shorts URL forms. Metrics omitted from manual input are zero, so repeated snapshots or
an official provider are needed for useful velocity scores.

## Providers

`TrendProvider` is the extension point in `src/providers/provider.ts`. The included YouTube provider
uses the public YouTube Data API when `YOUTUBE_API_KEY` is configured. It sends `publishedAfter`
based on `MAX_VIDEO_AGE_HOURS`, keeping the API request inside the same recent window used by the
SQLite ranking safety filter. `YOUTUBE_QUERY` is optional; when blank, the provider omits `q`.
The manual provider is the fallback for all three platforms. TikTok and Instagram discovery should be added through approved
official/public API providers when the required access is available; the worker does not scrape
pages or bypass access controls.

## Commands

```bash
pnpm trend:discover
pnpm trend:rank
pnpm trend:download
pnpm trend:upload
pnpm trend:run
pnpm trend:discover -- --url 'https://youtube.com/shorts/example'
```

`trend:run` performs discovery, snapshot persistence, ranking, top-N download, R2 upload, metadata
upload, metadata refresh, and retention cleanup. `TOP_N` is only the ranking pool; new downloads are
bounded by `MAX_NEW_DOWNLOADS_PER_RUN` and `MAX_NEW_DOWNLOADS_PER_DAY`. A candidate needs the
configured number of metric snapshots and is never downloaded again after a successful upload.

The hot library retains non-classic raw videos for `HOT_RETENTION_DAYS`. The strongest uploaded
videos are classified as classics, up to `MAX_CLASSIC_VIDEOS`, subject to the `R2_SOFT_LIMIT_GB`
soft cap for raw MP4 bytes. Maintenance targets 7.5GB when the configured cap is 8GB, leaving
headroom for hourly uploads. Expired or storage-pressure videos are moved to `archive/metadata/`
before their active raw and metadata objects are removed. The archive namespace is permanent and is
not scanned by Adscream's active metadata sync.

Run maintenance without discovery with `pnpm trend:prune` (or `pnpm --filter @hypit/trend-worker
run prune`). Use `pnpm --filter @hypit/trend-worker prune -- --dry-run` to print the planned archive
and raw-object deletions without changing SQLite or R2. `pnpm trend:stats` reports database counts,
UTC-day downloads, raw object count, and raw-byte usage.

## R2 layout

```text
raw/{platform}/YYYY/MM/DD/{video_id}.mp4
metadata/{platform}/YYYY/MM/DD/{video_id}.json
```

The media object is uploaded from the completed local file. SHA-256 is calculated before upload;
when another successful upload has the same hash, the existing media object is reused and only the
new metadata object is written.

Adscream's existing Postgres sync is currently upsert-oriented: archiving active metadata does not
automatically delete old Adscream database rows. This worker intentionally does not perform any
cross-repository database deletion.

## Scheduling examples

See `deploy/cron.example` and `deploy/launchd.plist.example`. Both run the existing manual batch
command and leave scheduling outside the application.
