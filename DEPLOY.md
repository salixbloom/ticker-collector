# Deploying to Unraid with Dockge

Dockge keeps each stack in its own directory and gives you an editor for
`compose.yaml`. This stack is built to suit that: **every setting lives in
`compose.yaml`**, which is the file Dockge edits. Compose builds the image itself,
so there is no registry to push to and nothing to pull.

> **Why not `.env`?** Dockge treats `compose.yaml` as the stack's source of truth
> and manages the stack directory around it, so a `.env` placed there can be
> rewritten or cleared. Most settings would fall back to a sensible default and you
> would never notice — but the API key has no default, so the collector would come
> up *healthy*, register no jobs and silently collect nothing. The key therefore
> comes from a file outside the stack directory, where nothing else manages it.

---

## 1. Create the data share

Do this **before** the first deploy. If the directory does not exist, Docker
creates it owned by `root`, and the container — which runs unprivileged — cannot
write to it.

On the Unraid terminal:

```sh
mkdir -p /mnt/user/quant-data
chown 99:100 /mnt/user/quant-data     # nobody:users
stat -c '%u %g %n' /mnt/user/quant-data
```

Use a share that lives on the array. Both figures are measured, not estimated:
EOD is roughly **70 MB/year**, and minute bars with extended hours are about
**4.8 GB/year** for a 1,000-ticker universe.

`published/`, `staging/`, `quarantine/`, `state/`, `logs/` and `work/` are all
created inside it and **must stay on one filesystem** — the atomic publish depends on
`os.replace` being atomic, which only holds within a filesystem. Startup checks
this and refuses to run if it is not true, so don't split them across mounts.

## 2. Get the code into the stack directory

Dockge's stacks usually live at `/mnt/user/appdata/dockge/stacks`. Create a
directory for this one and put the repository in it:

```sh
cd /mnt/user/appdata/dockge/stacks
git clone <your-repo> tick-collector     # or copy the folder over SMB
ls tick-collector/compose.yaml           # Dockge looks for this
```

Copying over SMB works just as well — the directory needs `compose.yaml`,
`Dockerfile`, `src/`, `config/`, `pyproject.toml`, `requirements.txt` and
`README.md`.

Dockge will now show **tick-collector** in its stack list.

## 3. Put the API key in a file

Outside the stack directory, so nothing Dockge does can clear it:

```sh
mkdir -p /mnt/user/appdata/tick-collector
printf '%s' 'your-key-here' > /mnt/user/appdata/tick-collector/api_key
chmod 600 /mnt/user/appdata/tick-collector/api_key
chown 99:100 /mnt/user/appdata/tick-collector/api_key
```

`printf` rather than `echo` so no trailing newline is stored — though the
collector strips whitespace either way. The file is mounted read-only at
`/run/secrets/api_key` and read via `MASSIVE_API_KEY_FILE`; whatever it contains is
redacted from logs exactly like an environment variable.

## 4. Check `compose.yaml`

This is the file Dockge edits, and it carries every setting. Three things are
worth a look before deploying; the rest can stay as shipped.

```yaml
    volumes:
      - /mnt/user/appdata/tick-collector/api_key:/run/secrets/api_key:ro
      - /mnt/user/quant-data:/data          # your data share
    environment:
      TZ: America/New_York
      # COLLECTOR_ALERT_WEBHOOK: https://discord.com/api/webhooks/...
```

And the build args, which must match the owner of the data share:

```yaml
      args:
        APP_UID: "99"
        APP_GID: "100"
```

## 5. Deploy

Press **Deploy** in Dockge, or:

```sh
docker compose up -d
```

The first deploy builds the image, which takes a few minutes (pyarrow and pandas
are large). Subsequent deploys reuse it.

## 6. Check it

```sh
docker exec tick-collector python -m collector doctor
```

`doctor` checks config, paths, permissions, the atomic-publish precondition, disk
space, the manifest schema, the API key, and that the API is reachable and
authenticated. It prints a line per check and exits non-zero if anything failed:

```
[  ok] config           loaded from /app/config/collector.toml
[  ok] timezone         America/New_York -> America/New_York
[  ok] data root        /data
[  ok] permissions      published/ staging/ quarantine/ state/ logs/ work/ all writable
[  ok] atomic publish   published/, staging/, quarantine/ share a filesystem
[  ok] disk space       1009.7 GB free
[  ok] manifest         schema v4, 0 batch(es) recorded
[  ok] api key          present, from MASSIVE_API_KEY_FILE=/run/secrets/api_key
[  ok] api reachable    api.massive.com reachable and authenticated
[  ok] published files  0

All checks passed.
```

Then watch it work:

```sh
docker exec tick-collector python -m collector status
docker exec tick-collector python -m collector gaps --dataset eod
```

Dockge shows the container's health badge, which goes unhealthy if `status.json`
goes stale (26 h) or the last run hard-failed.

---

## What happens on first start

About two minutes after startup the collector runs a **catch-up pass** over every
job, so a fresh deployment starts collecting immediately rather than idling until
18:00. Each job only fills what the manifest says is missing, so this is safe to
re-run and does nothing on an already-complete install.

**That means the first start begins the EOD backfill.** With the shipped
`backfill_days = 730` that is ~500 trading days at 5 API calls/minute — roughly
**100 minutes**. It is resumable: if the container restarts, the next run picks up
exactly where it stopped.

To bring it up without backfilling, set a smaller window in
`config/collector.toml` before deploying:

```toml
[collection]
backfill_days = 30
```

…or turn the catch-up off and let the 18:30 slot do it:

```toml
[scheduler]
catch_up_on_start = false
```

The first reference snapshot takes ~15 minutes on its own: it pages through all
36,000-odd tickers, active and delisted, at 5 calls/minute.

**Minute collection is chunked**, including on the nightly schedule: one call
covers up to `minute_backfill_chunk_days` (40) days for a ticker. In steady state
a ticker has one day outstanding so the request is a single day anyway; during a
backfill it is ~40x fewer calls. Unchunked, 1,000 tickers over two years is
~502,000 calls — over a year of nightly runs instead of about eleven days.

**Minute bars are the expensive part.** Measured on real data: ~891 bars per
ticker-day with extended hours, ~19 MB for a 1,000-ticker day, ~4.8 GB a year.

Steady state costs one call per ticker per evening, so a 1,000-ticker universe is
~200 minutes a night at 5 calls/minute. A two-year backfill is ~13,000 chunked
calls, which at the default budget is about eleven nights.
`[collection] minute_calls_per_run` bounds each scheduled run so it cannot still
be going when the next slot arrives; the remainder resumes the following evening,
and nothing is lost because progress is recorded per ticker-day.

To backfill minute history deliberately rather than waiting for the nightly job:

```sh
docker exec tick-collector python -m collector minute --days 730 --chunk-days 40
docker exec tick-collector python -m collector history
```

To finish a minute backfill faster, raise the per-run budget in
`config/collector.toml`:

```toml
[collection]
minute_calls_per_run = 3000
```

For a 1,000-ticker universe the chunked backfill is ~13,000 calls:

| budget | run length | backfill |
|---|---|---|
| 1200 (default) | 4.0 h | ~11 nights |
| 3000 | 10.0 h | ~5 nights |
| 5000 | 16.7 h | ~3 nights |

**Do not go much past 3,000.** Jobs run sequentially in one thread, so a job that
takes 16.7 hours leaves the heartbeat untouched for that long and delays the 07:00
EOD retry. At 3,000 the run ends around 05:15, before anything else is due.

## The provider's history window

**The plan serves a rolling ~730-day window, and its edge moves forward every
day.** Measured on 2026-10-02: a request for 2024-10-02 was served, 2024-10-01 was
refused with `403 NOT_AUTHORIZED`. EOD and minute share exactly the same boundary.

Two consequences worth understanding before the first backfill:

- **A date not collected before it ages out is gone permanently.** That is the
  whole reason continuous collection and backups matter — the archive becomes
  irreplaceable as the window slides past it.
- **The collector learns the boundary rather than assuming it.** The first refusal
  triggers a single probe that returns the real edge, and every outstanding unit
  older than it is retired in one write. That is 2 API calls, not one per dead
  date.

```sh
docker exec tick-collector python -m collector history
```

```
configured oldest date: 2024-10-07  (backfill_days=730 less history_safety_days=5)
eod      learned floor: 2024-10-02  (7 unit(s) retired as aged out)
minute   learned floor: 2024-10-02  (0 unit(s) retired as aged out)
```

`history_safety_days` keeps requests off the very edge. Without it,
`backfill_days = 730` asks for precisely the date being refused, and every run
spends calls rediscovering that the oldest day has just aged out.

If you upgrade your plan to a longer history, clear the learned floor so the
collector re-discovers it:

```sh
docker exec tick-collector python -m collector history --reset
```

## The daily schedule

All times US Eastern, set by `[scheduler] timezone`:

| Time  | Job                 | Does |
|-------|---------------------|------|
| 18:00 | `reference`         | Snapshot every ticker, active and delisted |
| 18:15 | `corporate_actions` | New and revised splits and dividends |
| 18:30 | `eod`               | Fill missing EOD trading days |
| 19:00 | `universe`          | Rebuild the minute universe (monthly) |
| 19:15 | `minute`            | Fill missing minute ticker-days |
| 20:00 | `checks_report`     | Write the daily summary to `status.json` |
| 21:00 | `backup`            | Verified backup, then prune the oldest |
| 07:00 | `eod` (retry)       | Catch anything the provider published late |

The order matters twice over: corporate actions run before EOD so the day's splits
are on the books when the large-move check looks for them, and the universe is
rebuilt before the minute job so a new universe takes effect the same evening.

Times only decide when catch-up happens. Every job derives its work from the
manifest, so a missed slot — a reboot, an outage — is simply picked up at the next
one. Nothing needs to be re-run by hand.

## Running a job now

```sh
docker exec tick-collector python -m collector eod --days 730    # full backfill
docker exec tick-collector python -m collector eod --max-days 5  # bounded
docker exec tick-collector python -m collector reference
docker exec tick-collector python -m collector corporate-actions
docker exec tick-collector python -m collector universe          # no API calls
docker exec tick-collector python -m collector minute --days 730 --chunk-days 40
docker exec tick-collector python -m collector history
```

A manual run is bounded by `--max-calls`, not by `minute_calls_per_run` — that
setting only governs the nightly job. See **What happens on first start** for how
to size it.

These share the container's rate limiter only if run inside it, as above. Don't
run a second container against the same key — the 5 calls/minute limit is per key,
and two limiters would jointly exceed it.

## Alerting

Uncomment the webhook in `compose.yaml` and hard failures will reach you:

```yaml
    environment:
      COLLECTOR_ALERT_WEBHOOK: https://discord.com/api/webhooks/...
```

The payload carries the message in both `text` and `content`, so one URL works
with Slack, Discord, ntfy, Gotify, Apprise and most self-hosted receivers without
any format configuration.

Check it before you rely on it:

```sh
docker exec tick-collector python -m collector alert-test
```

What triggers an alert:

| | |
|---|---|
| A job hard-fails or aborts | One alert, naming the job and the first few errors |
| The collector becomes unhealthy | Stale `status.json`, or a job left it failing |
| Recovery | A `[resolved]` message, so you know it fixed itself |

**Soft flags never alert.** A real EOD day flags ~8% of tickers for vwap falling
outside the day's high/low — that is the provider's data, not a fault, and
alerting on it would bury the failures that matter.

**Alerts are deduplicated.** The same condition fires once, then not again for
`[alerts] cooldown_hours` (6 by default). Without that, a provider outage would
post a message every heartbeat all night. The dedup state lives in
`state/alerts.json`, so a container restart does not re-announce everything it
already told you about.

Leaving the webhook unset is fine — failures still reach `status.json`, the logs,
and the container's health badge in Dockge.

## Backups

**The published data cannot be re-downloaded.** The provider serves two years of
history, so anything that ages out of that window is gone permanently. The archive
gets more irreplaceable every month it runs.

Point `[backup] directory` in `config/collector.toml` at a different share from
the data — ideally an offsite target — and the 21:00 job handles it:

```toml
[backup]
directory = "/backups"
include_data = true   # false if an Unraid plugin already copies the array
keep = 7
```

Then uncomment the backup mount in `compose.yaml`:

```yaml
    volumes:
      - /mnt/user/backups/tick-collector:/backups
```

By hand:

```sh
docker exec tick-collector python -m collector backup --to /backups
docker exec tick-collector python -m collector backups --in /backups
docker exec tick-collector python -m collector verify-backup /backups/backup-20261001T181535Z
docker exec tick-collector python -m collector restore /backups/backup-20261001T181535Z
```

Two things this does that `cp -r` cannot:

- **The manifest is copied through SQLite's backup API.** It is a live WAL
  database, so a plain file copy can capture a torn page or miss committed data
  sitting in the write-ahead log.
- **Every file's SHA-256 is recorded**, so `verify-backup` can confirm a backup is
  sound and `restore` refuses to proceed from a corrupt one. A restore you have
  never tested is a guess.

`restore` will not overwrite existing published data without `--force`, so a
mistyped command cannot destroy the archive it was meant to protect. After a
restore, `collector verify` re-checks the restored manifest against the restored
files.

`include_data = false` backs up only the manifest and status — the cheap daily
case when something else already copies `published/`. The manifest is the part
that cannot be rebuilt from the Parquet files, because it holds the checksums, the
point-in-time `ingested_at` and the check results.

## Checking the burn-in

The plan's last milestone is an observation rather than a feature: **10
consecutive trading days with no hard failures, no unexplained gaps and no manual
intervention.** That is two calendar weeks of running, so it can only be completed
on your server. This command measures it:

```sh
docker exec tick-collector python -m collector burnin
```

```
burn-in: IN PROGRESS -- 4/10 consecutive clean trading days
window: 2026-09-15 .. 2026-09-30 (12 trading day(s) examined)

day          clean        rows  notes
----------------------------------------------------------------------------
2026-09-25   yes        12,591  1 soft-flagged batch(es)
2026-09-28   NO         12,572  minute: 7 unit(s) still owed
...
```

It exits 0 once it passes and 1 while it is in progress, so it can go straight
into a cron job or a monitoring check. A day counts as clean when nothing is still
owed, nothing was quarantined, and every published batch matches its checksums.
Soft flags do not count against a day; the streak is counted backwards from the
most recent day, because the run has to be unbroken up to now.

For the write-up the plan asks for — storage size, flag rates, issues found:

```sh
docker exec tick-collector python -m collector burnin --journal /data/logs/burnin.md
```

## Updating

```sh
cd /mnt/user/appdata/dockge/stacks/tick-collector
git pull
```

Compose only rebuilds when the image tag changes, so bump it in `compose.yaml`:

```yaml
    image: tick-collector:0.2.0
```

then Deploy again. Or force it:

```sh
docker compose up -d --build
```

## When something is wrong

| Symptom | Cause and fix |
|---|---|
| Container restarts repeatedly | Check `docker logs tick-collector`. A config error exits 2 with the reason on stderr. |
| `cannot write to /data/...` | The share is not writable by `APP_UID:APP_GID`. The error names the uids involved. `chown 99:100` the host directory, or set `APP_UID`/`APP_GID` to match it and rebuild. |
| Healthy but collecting nothing | Almost always a missing API key: jobs are not registered at all and the log says so once at startup. `doctor` names the cause exactly — `is empty`, `could not be read`, or `no API key`. If you had put the key in a `.env` in the stack directory, check whether Dockge has cleared it, and move it to a key file as in step 3. |
| `must be on one filesystem` | `published/`, `staging/` and `quarantine/` are split across mounts. Point `COLLECTOR_DATA_DIR` at a single share. |
| Unhealthy in Dockge | `collector status` shows which job failed and why. Quarantined batches sit in `/data/quarantine/<batch>/` with a `check_report.json` explaining the rejection. |
| `api key ... is empty` | The key file exists but has no contents. If it lives in the stack directory, Dockge may have rewritten it — move it to `/mnt/user/appdata/tick-collector/api_key` per step 3. |
| `NOT_AUTHORIZED` / "plan doesn't include this data timeframe" | The date is older than the rolling window. Expected at the trailing edge, and handled: the collector learns the boundary and retires those dates. `collector history` shows it. If it is happening for *recent* dates, check the key. |
| Minute days never publish | A day publishes only when every universe ticker is accounted for. `collector gaps --dataset minute` lists what is outstanding; a ticker that keeps failing is named there with its error. |
| `no universe snapshot to collect against` | The minute job needs a universe, which derives from published EOD data. Run `collector universe` (or let the monthly slot do it) once EOD exists. |
| No alerts arriving | `collector alert-test`. If that works but a real failure was silent, it was probably inside the dedup cooldown — check `state/alerts.json`. |
| Hitting 429s | Something else is using the same key. The limiter allows 5 calls/minute and counts failed attempts, since rejected requests consume quota too. |

## Verifying what is on disk

Separately from backups, `collector verify` re-checks every published batch against
the manifest — both the file checksum and each day's own content checksum:

```sh
docker exec tick-collector python -m collector verify
```

The per-day content checksum is the interesting one. It covers the data with
`ingested_at` excluded, so re-fetching a day matches while the provider *restating*
that day does not — which a file-level hash cannot distinguish from an ordinary
append. Worth running after a restore, and occasionally against the live data.
