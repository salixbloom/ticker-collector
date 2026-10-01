# Micro Plan: Continuous Market Data Collector

A small, self-healing service that collects US equity market data every day, validates it, and stores it compactly. It runs in Docker on an Unraid server and is the first concrete piece of Phase 1 (point-in-time data pipeline) of the Quant School project plan.

> **Technology choices in this plan** (record each in the decision log with its rationale): Massive (formerly Polygon.io) free plan as the primary source, `yfinance` as an optional cross-check, Parquet with zstd compression for storage, Docker Compose on Unraid for deployment.

---

## 1. Scope

### In scope
- **End-of-day (EOD) bars** for every US stock, including companies that later delist, with a 2-year backfill and daily updates.
- **Minute bars** for a defined universe (~1,000 liquid stocks to start), with backfill and daily updates.
- **Corporate actions** (splits and dividends) and **ticker reference data** (active/inactive, delisting date).
- Validation before anything is published, compact columnar storage, and a status file the dashboard can read later.

### Out of scope (for now)
- Real-time or intraday streaming (the free plan provides end-of-day access).
- Fundamentals, filings and alternative data (later collectors).
- Adjusted prices on disk. Raw prices plus corporate actions are stored; adjustment happens at read time.

---

## 2. Sources and limits

| Data | Primary source | Method | Notes |
|---|---|---|---|
| EOD bars, all stocks | Massive free plan | Grouped daily endpoint: one call returns every ticker's bar for one date | Includes stocks that later delisted, so it is survivorship-free by construction. Request with OTC excluded |
| Minute bars | Massive free plan | Aggregates endpoint per ticker, minute timespan, large date ranges per call | Free plan includes minute aggregates and 2 years of history |
| Splits, dividends | Massive free plan | Corporate actions endpoints | Needed to adjust prices at read time |
| Ticker reference | Massive free plan | Tickers endpoint, including inactive tickers | Provides active flag and delisting date |
| Cross-check (optional) | `yfinance` | Small daily sample | Unofficial interface to Yahoo Finance; low volume only, and review Yahoo's terms before use. Never a primary source |

**Free plan constraints:** 5 API calls per minute, 2 years of history, end-of-day data. Verify current plan contents when signing up, since they change, and use the data within the provider's terms (personal, non-redistributed research).

**Call budget.** 5 calls/minute ≈ 7,200 calls/day.
- EOD: ~1 call per trading day. The 2-year backfill (~500 trading days) takes under 2 hours.
- Minute: each call returns up to ~50,000 bars, which covers roughly 50–125 trading days per ticker depending on whether extended hours are included. A 2-year backfill of 1,000 tickers is roughly 10,000–15,000 calls, about 1.5–2 days of continuous running. Daily updates then need ~1 call per ticker (~1,000 calls, under 4 hours).
- Reference and corporate actions: a few dozen calls per day.

The daily budget comfortably fits all jobs, with headroom for retries.

---

## 3. Design principles

1. **Self-healing, not "fetch today."** Every run asks "which expected trading days are missing or failed?" and fills them. A missed day, outage or server reboot is repaired automatically on the next run.
2. **Idempotent.** Re-running any job for any date produces identical output and never creates duplicates.
3. **Validate before publish.** Data is written to staging, checked, and only then atomically moved into the published store. Failed batches go to quarantine, never to the published store.
4. **Raw is immutable.** Published files are never edited in place. Corrections are written as a new version with a manifest entry explaining why.
5. **Point-in-time metadata.** Every batch records when it was downloaded (`ingested_at`), so later phases know what was knowable when.
6. **One process owns the API key.** A single shared rate limiter for all jobs prevents two jobs from jointly exceeding 5 calls/minute.

---

## 4. Storage format

### Layout
```
/data/
  published/
    eod/year=2026/month=09/eod_2026-09.parquet
    minute/year=2026/month=09/day=30/minute_2026-09-30.parquet
    corporate_actions/splits.parquet
    corporate_actions/dividends.parquet
    reference/tickers_2026-09-30.parquet      (daily snapshot, point-in-time)
  staging/        (in-progress batches, cleared after publish)
  quarantine/     (failed batches with their check report)
  state/
    manifest.sqlite   (every batch: source, params, rows, checksum, schema version, ingested_at, check results)
    status.json       (latest run summary for the dashboard)
  logs/
```

EOD is partitioned monthly (daily files would be tiny and inefficient); minute data is partitioned daily (each day is a natural unit for backfill and validation). Small monthly EOD files are rewritten by appending a new version when a day is added, via the staging → validate → atomic swap flow.

### Schema (strict, versioned)

**EOD bars** (`schema_version = 1`)

| Column | Type | Notes |
|---|---|---|
| `ticker` | string, dictionary-encoded | |
| `date` | date32 | Trading date (exchange local) |
| `open`, `high`, `low`, `close` | int64 | Price in units of 1/10,000 USD (e.g. 123.4567 → 1234567) |
| `vwap` | int64 | Same units; nullable if unavailable |
| `volume` | int64 | Shares |
| `transactions` | int32 | Nullable if unavailable |
| `ingested_at` | timestamp[ms, UTC] | Batch download time |

**Minute bars** (`schema_version = 1`): same columns, with `ts` (timestamp[ms, UTC], bar start) instead of `date`, plus `session` (int8: 0 = pre-market, 1 = regular, 2 = after-hours).

**Why scaled integers for prices:** they give exact, consistent values (no floating-point drift between runs, which also makes checksums and idempotency checks reliable), and integers compress better than floats. `float32` is not allowed: its ~7 significant digits lose precision on higher-priced stocks. 4 decimal places covers sub-dollar stocks quoted in fractions of a cent.

### Compression settings
- Parquet with zstd compression (start at level 9; benchmark levels 3, 9 and 15 on a month of minute data and record the size/speed tradeoff in the decision log).
- Sort rows by `ticker`, then time, before writing. Consecutive rows for the same ticker have similar values, which compression exploits heavily.
- Dictionary encoding for `ticker`; delta encoding for time columns where supported.
- Target row group size around 128 MB uncompressed.

### Size estimates (rough, to be measured)
| Dataset | Rows per year | Expected compressed size per year |
|---|---|---|
| EOD, all US stocks (~10,000 tickers) | ~2.5 million | Tens of MB |
| Minute, 1,000 tickers, regular session | ~100 million | ~1–2 GB |
| Minute, 1,000 tickers, with extended hours | ~200–250 million | ~2–4 GB |

Decide in the decision log whether to collect extended hours. Recommendation: collect them (they're cheap to store and impossible to recover after the 2-year window passes), but tag with `session`.

---

## 5. Data quality checks

Every batch passes through these checks in staging. Each check is **hard** (failure → quarantine, nothing published) or **soft** (published, but flagged in the manifest and status file for review).

### Structural (hard)
- Schema matches the registered version exactly: column names, order, types, nullability.
- Required columns have no nulls (`ticker`, time, OHLC, `volume`).
- No duplicate (`ticker`, time) keys.
- All timestamps fall within the batch's requested date and expected session hours.
- File reads back correctly after write, and its checksum matches the manifest.

### Value sanity (hard)
- All prices > 0; volume ≥ 0.
- `low ≤ open ≤ high` and `low ≤ close ≤ high` for every bar.
- `vwap`, when present, lies between `low` and `high` (soft if within a small tolerance).

### Completeness (hard or soft)
- The date is an expected trading day (exchange calendar with holidays and early closes). Data for a non-trading day → hard fail.
- EOD ticker count within ±10% of the trailing 20-day median → soft; beyond ±30% → hard.
- Minute bar count per ticker on a regular full session ≤ 390 regular-session bars; tickers with far fewer bars than usual are listed → soft.
- Every ticker in the minute universe has data for the day, or is explained (halted, delisted per reference data) → soft.

### Consistency (soft)
- Minute-to-EOD reconciliation: per ticker and day, the max of minute highs ≈ EOD high, min of lows ≈ EOD low, summed regular-session volume within a tolerance of EOD volume.
- Large moves: absolute day-over-day close change > 50% is flagged and cross-referenced against the splits table. An unexplained huge move is flagged for review, not rejected (real crashes happen).
- Corporate actions: a split with no matching price discontinuity, or a discontinuity with no split, is flagged.
- Optional `yfinance` cross-check on a random sample of ~20 tickers: closes agree within tolerance after accounting for adjustment differences.

### Run-level (hard)
- Rate-limit and HTTP errors are retried with exponential backoff; a batch with partial responses is never published.
- If more than a set share of batches in a run fail (default 20%), the run stops and alerts rather than continuing to hammer a broken source.

---

## 6. Jobs and schedule

All times US Eastern. Every job is a "fill missing" job, so schedule times only control *when* catch-up happens.

| Job | Schedule | Action |
|---|---|---|
| `reference` | Daily 18:00 | Snapshot all tickers (active and inactive) |
| `corporate_actions` | Daily 18:15 | Fetch new splits and dividends |
| `eod` | Daily 18:30, retry 07:00 next day | Fill missing trading days via grouped daily |
| `minute` | Daily 19:00, runs until done | Fill missing ticker-days for the universe |
| `universe` | Monthly, first trading day | Rebuild the minute universe (e.g. top ~1,000 by trailing 20-day dollar volume from EOD data), stored as a dated snapshot |
| `checks_report` | Daily after jobs | Write `status.json` and a daily summary |
| `backfill` | On demand | Same code path as daily jobs, with a wider date range and resumable state |

If the provider's data for the latest day isn't available yet at 18:30, the job finds the gap again in the 07:00 retry.

**Universe stability:** when a stock leaves the minute universe, keep collecting it for at least 1 month after it drops out; when a stock joins, backfill its minute history to the start of the collection window. This avoids holes for stocks near the cutoff.

---

## 7. Docker and Unraid deployment

### Container
- One image: slim Python base, pinned dependencies, non-root user, timezone data installed.
- One long-running container with an internal scheduler (a single process owns the rate limiter and API key).
- `restart: unless-stopped`.
- Health check: fails if `status.json` hasn't been updated in more than 26 hours or the last run had hard failures.

### Volumes
- `/data` → an Unraid user share on the array, e.g. `/mnt/user/quant-data` (bulk storage).
- `/data/state` → optionally on the cache pool, e.g. `/mnt/user/appdata/quant-collector/state` (small, frequently written SQLite manifest).
- Configuration (universe size, extended hours on/off, thresholds) in a mounted config file.

### Secrets
- API key supplied via an environment file excluded from version control, never baked into the image or logged.

### Resources
- Set a memory limit (minute-day batches are small; 1–2 GB is ample).
- Logs rotated to avoid filling the cache drive.

### Backups
- Published data plus `manifest.sqlite` backed up on a schedule (e.g. an Unraid backup plugin or an offsite copy). Data older than 2 years can't be re-downloaded on the free plan, so the archive becomes irreplaceable over time.

### Alerts
- On hard failure or failed health check: send a notification via a webhook (Unraid notifications, email or a chat channel). Later, these feed the dashboard Inbox and Journal.

---

## 8. Steps and definitions of done

### M0: Repo and container skeleton
- [ ] Container builds, starts on Unraid, writes a heartbeat to `status.json`, and survives a server reboot.
- [ ] API key loads from the environment file and never appears in logs (verified by grep of logs).
- [ ] Technology choices recorded in the decision log.

### M1: Schema and storage library
- [ ] Writing and reading round-trips exactly for sample data, including scaled-integer prices.
- [ ] Atomic publish: a crash mid-write (simulated) leaves no partial file in `published/`.
- [ ] Manifest records every batch with checksum and `ingested_at`.

### M2: Quality checks
- [ ] Each hard check has a unit test with a deliberately corrupted fixture (bad type, null, duplicate key, `high < low`, non-trading day) that is quarantined.
- [ ] Each soft check has a fixture that publishes with a flag.
- [ ] Check results appear in the manifest and `status.json`.

### M3: Rate limiter and API client
- [ ] Shared limiter never exceeds 5 calls in any 60-second window (verified over a 30-minute test log).
- [ ] 429 and 5xx responses retry with backoff; partial responses never publish.

### M4: EOD backfill and daily job
- [ ] 2-year backfill complete with all trading days present and all checks passing (or flags reviewed).
- [ ] Delisted tickers present: at least some tickers in the backfill are marked inactive in reference data.
- [ ] Deleting a published day and re-running restores an identical file (same checksum).

### M5: Reference and corporate actions
- [ ] Daily ticker snapshots accumulate as point-in-time records.
- [ ] Split cross-check runs and its flags are reviewed on at least one known historical split.

### M6: Universe and minute backfill
- [ ] Universe snapshot built from EOD data and stored with its date.
- [ ] Minute backfill resumes correctly after being stopped mid-run.
- [ ] Minute-to-EOD reconciliation passes for ≥ 99% of ticker-days, with exceptions reviewed.

### M7: Minute daily job
- [ ] Runs to completion daily within its call budget.
- [ ] Measured compressed size per day recorded; compression level chosen and logged.

### M8: Optional cross-check
- [ ] `yfinance` sample comparison runs at low volume and reports disagreements as soft flags.

### M9: Monitoring and backups
- [ ] Simulated outage (block network for a day) triggers an alert, and the next run fills the gap automatically.
- [ ] Backup job runs and a restore from backup is tested once.

### M10: Burn-in
- [ ] 10 consecutive trading days with no hard failures, no unexplained gaps and no manual intervention.
- [ ] Short write-up in the journal: storage size, check flag rates, issues found.

When M10 is complete, the collector is considered done and Phase 1 continues with fundamentals and filings collectors built on the same storage, check and manifest code.

---

## 9. Known limitations to record

- **History window:** the free plan provides 2 years; everything older than the backfill window is unrecoverable on this plan, which is why continuous collection and backups matter.
- **Coverage of minute data** is limited to the chosen universe; stocks outside it have EOD data only.
- **`yfinance`** is unofficial and may break or be blocked without notice; nothing in the pipeline depends on it.
- **Provider plan terms may change**; recheck them periodically and record any change in the decision log.
