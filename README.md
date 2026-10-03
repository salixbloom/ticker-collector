# tick-collector

A self-healing collector for US equity market data: end-of-day bars for every
stock (delistings included), minute bars for a liquid universe, corporate actions
and ticker reference data. Validates every batch before publishing it, stores it
as zstd-compressed Parquet, and repairs its own gaps.

Design: [`data-collector-micro-plan.md`](data-collector-micro-plan.md).
Deploying to Unraid with Dockge: [`DEPLOY.md`](DEPLOY.md).
Current state, decisions and follow-ups: [`.agent/CONTINUITY.md`](.agent/CONTINUITY.md).

**Status:** feature-complete and verified against the live API. EOD bars, minute
bars, ticker snapshots, splits and dividends all collect on the plan's schedule,
with alerting and verified backups. M8 (the optional `yfinance` cross-check) was
skipped by choice. M10's burn-in is a 10-trading-day observation that runs once
deployed — `collector burnin` measures it.

## Quick start

Everything runs in a container; nothing is installed on the host.

```sh
# Local development: a .env is fine here. Under Dockge use a key file instead
# (see DEPLOY.md) -- the stack directory is not a safe place for secrets.
cp .env.example .env        # then put your API key in it
make check                  # ruff + mypy + pytest, all inside the dev image
make up                     # build and run the collector
make doctor                 # verify config, paths, permissions, connectivity
make logs                   # follow it
```

**Deploying to Unraid with Dockge:** put this repository in the Dockge stack
directory, write the API key to a file outside it, press Deploy. Compose builds the
image itself, so there is no registry involved. [`DEPLOY.md`](DEPLOY.md) has the
walkthrough, the daily schedule and a troubleshooting table.

Every setting lives in `compose.yaml`, which is the file Dockge edits — check the
data-share path and that `APP_UID`/`APP_GID` match its owner (99:100 =
`nobody:users`). Nothing is read from a `.env` in the stack directory: Dockge
manages that directory, and a cleared `.env` would leave the collector healthy and
idle with no key.

## Commands

The container entry point is `python -m collector`:

| Command | Does |
|---|---|
| `serve` | Run the scheduler. This is what the container runs. |
| `init` | Create the data layout and manifest, clear stale staging, then exit. |
| `healthcheck` | Exit 0 if healthy. Wired into the image's `HEALTHCHECK`. |
| `status` | Print `state/status.json`. |
| `doctor` | Check config, paths, permissions, disk, the API key and connectivity. |
| `verify` | Re-check every published batch against the manifest (see below). |
| `info --json` | Print the resolved config. Reports whether a key is present, never its value. |
| `eod` | Fill missing EOD trading days. `--days N` sets the window, `--max-days N` bounds one run. |
| `reference` | Snapshot every ticker, active and delisted, for today. |
| `corporate-actions` | Fetch new and revised splits and dividends. |
| `universe` | Rebuild the minute universe from EOD data. No API calls. |
| `minute` | Fill missing minute ticker-days. `--chunk-days 40` for backfilling. |
| `backup` / `restore` | Verified backup and restore. `backups`, `verify-backup` too. |
| `burnin` | M10 status: consecutive clean trading days. `--journal` for the write-up. |
| `alert-test` | Send a test alert to the configured webhook. |
| `history` | Show or reset the learned provider history floor. |
| `gaps` | Show what a dataset still owes. |

## How it works

**Self-healing.** No job asks "what is today?". Each asks the manifest's
`coverage` table "which expected trading days are missing or failed?" and fills
those. A missed run, outage or reboot is repaired by the next run, which is why
the schedule times only control *when* catch-up happens. The same property makes
the startup catch-up pass safe: a freshly deployed container begins collecting
within a couple of minutes rather than idling until 18:00, and on an
already-complete install it finds nothing to do.

**Validate, then publish atomically.** A batch is written to `staging/`, read back
and verified, then moved into `published/` with `os.replace`. A crash before the
move leaves `published/` untouched; a crash during it leaves either the old file
or the new one. Failed batches go to `quarantine/` with their check report, never
to `published/`. This requires `published/`, `staging/` and `quarantine/` on one
filesystem — `init` checks that and refuses to start otherwise.

**Raw is immutable.** Published files are never edited. A correction is a new
manifest revision with its reason recorded.

**Survivorship-free by construction.** The grouped-daily feed is a snapshot of
what traded that day, so delistings are already in the price history. Daily
ticker snapshots then record `active` and `delisted_utc`, which is what lets a
reader reconstruct the investable universe as it stood on a past date. Getting
the delisted names takes an explicit second request — the endpoint returns active
tickers by default — and a hard check refuses to publish a snapshot that contains
no delisted names at all.

**Prices and volumes are integers.** OHLC, vwap and volume are int64 in units of
1/1,000,000. Six decimals rather than four because the provider actually uses
them: 276 of 62,235 prices on a measured day carry 5-6 decimals, and volume is
*fractional* in 92% of rows. The scale is stamped into every Parquet file's
metadata so a reader cannot misinterpret it.

**Nothing on disk is adjusted.** The aggregate endpoints default to
`adjusted=true`; the client sends `adjusted=false` and then verifies the response
said so, because adjusted prices change retroactively whenever a split happens.
Raw prices plus the corporate-actions tables are stored, and adjustment happens
at read time.

**Two checksums per batch.** `checksum_sha256` covers the file.
`content_sha256` covers the data with `ingested_at` excluded, so re-fetching a
day matches while a provider *restating* that day does not — which a file hash
cannot distinguish from an ordinary append. `collector verify` checks both, and
checks each day's content individually inside the shared monthly file.

**The key stays out of everything.** It is read from the environment only, held
behind a property rather than on the config object, and a logging filter strips
known secret values plus any `api_key`/`token`/`Bearer` material from every log
record and from `status.json`.

## Minute bars

A published minute file covers one whole day for every ticker in the universe, but
it is assembled from ~1,000 separate API calls. So each ticker's bars land in
`work/minute/<date>/<ticker>.parquet` as they arrive and the ticker-day is marked
done in the manifest; the day is merged, validated and published only once nothing
is outstanding for it. A container killed half way through restarts with the
fetched parts still on disk and refetches only what is actually missing.

`--chunk-days 40` switches to the backfill strategy: one call covers a range of
days and the response is split across daily partitions. Measured against the live
API, 3 calls filled 18 ticker-days — roughly a sixth of the requests, which is the
difference between a two-year backfill taking days and taking weeks.

**Measured costs** (10 real megacaps, 2026-09-29, extended hours on):

| | |
|---|---|
| Bars per ticker-day | 891 (33% pre-market, 44% regular, 23% after-hours) |
| Bytes per row | 21.4 at zstd 9 |
| A 1,000-ticker day | ~19 MB |
| A year | ~4.8 GB |
| Daily collection | ~1,000 calls ≈ 200 min at 5 calls/min |
| Two-year backfill | ~13,000 calls ≈ 1.8 days of continuous running |

zstd level 9 is the default because it was measured, not assumed: on real minute
bars, levels 3/9/15 give 20.8 / 20.0 / 19.5 bytes per row, so 9 takes most of the
available saving and 15 adds 2.5% for noticeably more CPU.

## The provider's history window

The plan serves a rolling ~730-day window whose edge moves forward daily — EOD and
minute share the same boundary, measured. A date not collected before it ages out
cannot be fetched again, which is what makes continuous collection and backups
matter rather than being nice to have.

The collector **learns** that boundary instead of assuming it. One refusal triggers
a single probe (a range request straddling the edge returns OK and silently clips,
so its first bar *is* the boundary), the floor is persisted, and every outstanding
unit older than it is retired in one write. Two API calls, not one per dead date.
`collector history` shows it; `--reset` re-discovers it after a plan upgrade.

## Monitoring and backups

A hard failure posts to `COLLECTOR_ALERT_WEBHOOK`, and so does the recovery. The
payload carries the message in both `text` and `content`, so one URL works with
Slack, Discord, ntfy and most self-hosted receivers. Alerts are deduplicated with
a cooldown and the state survives a restart, because an outage at 5-minute health
intervals would otherwise send hundreds of messages. Soft flags never alert.

Backups copy the manifest through SQLite's backup API — it is a live WAL database,
so a plain file copy can be torn — and record every file's SHA-256, so
`verify-backup` can confirm one is sound and `restore` refuses a corrupt one. The
published data cannot be re-downloaded once it ages past the provider's two-year
window, which is what makes this the only real safety net.

## Layout

```
/data/
  published/      eod/ minute/ corporate_actions/ reference/
  work/           minute parts awaiting a complete day -- resumable progress
  staging/        in-progress batches, cleared at startup
  quarantine/     failed batches with their check report
  state/          manifest.sqlite, status.json
  logs/           rotated
```

`work/` exists because `staging/` is cleared at startup: a half-collected minute
day is real progress worth keeping across a restart, so it cannot live there.

EOD is partitioned monthly, minute data daily. See the plan for the full schema.

## Development

```sh
make test       # pytest
make lint       # ruff check + format --check
make typecheck  # mypy
make check      # all three
make lock       # regenerate requirements*.txt from requirements*.in
make shell      # a shell in the dev image
```

`make lock` runs `pip-compile` inside the image's own base, so the pins match what
the image actually installs.
