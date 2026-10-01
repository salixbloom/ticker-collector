# CONTINUITY — tick-collector

Canonical briefing. Read before acting. Facts only; every entry dated (ISO) and
tagged `[USER]` / `[CODE]` / `[TOOL]` / `[ASSUMPTION]`.

**Goal:** a self-healing service that collects US equity market data daily,
validates it and stores it compactly, per `data-collector-micro-plan.md`. Phase 1
of the Quant School project.

**State as of 2026-10-01:** feature-complete. M0–M7 and M9 are done and verified
against the live API; M8 was skipped by the user's choice. M10 is an
observation over 10 trading days, so its apparatus is built and tested but the
clock only runs once deployed. **Ready to deploy** — see `DEPLOY.md`.

---

## [PLANS]

- 2026-09-30T19:34Z [USER] Instruction was "read AGENTS and
  data-collector-micro-plan and begin working". Work proceeds through the plan's
  milestones M0→M10 in order.
- 2026-09-30T21:05Z [USER] API key supplied in chat and written to `.env`
  (git-ignored). **It is in the chat transcript, so it should be rotated once the
  collector is settled.** Docs pointer given: https://massive.com/docs.
- 2026-10-01T18:20Z [USER] **M8 skipped by request.** The optional `yfinance`
  cross-check is not built. Nothing depends on it; `[project.optional-dependencies]
  crosscheck` is still declared if it is ever wanted.
- 2026-10-01T18:50Z [CODE] **Next: deploy, then let M10's clock run.** Everything
  the plan asks for is built. What remains is on the Unraid box:
  1. Create the data share and (optionally) the backup share.
  2. Deploy per `DEPLOY.md`, run `collector doctor`.
  3. Run the EOD and minute backfills deliberately, or let the nightly jobs chew
     through them.
  4. After ~2 weeks, `collector burnin` reports M10's verdict, and
     `--journal` writes the summary the plan asks for.
- 2026-10-01T18:00Z [ASSUMPTION] No minute backfill of real size has been run. The
  live verification used a 10-ticker universe for one day, plus a 3-ticker chunked
  backfill over 6 days.
- 2026-10-01T13:25Z [USER] **Deployment target is Unraid via Dockge.** Built for
  that: `compose.yaml` in the stack directory, everything operator-editable in
  `.env`, `build:` so Compose builds on first Deploy with no registry involved.
  See `DEPLOY.md`.
- 2026-10-01T18:50Z [CODE] All seven jobs are registered with the scheduler
  (`cli._register_jobs`) and a cold start was observed running every one of them
  unattended -- see [OUTCOMES].
- 2026-09-30T22:10Z [ASSUMPTION] The full 2-year EOD backfill (~500 trading days,
  ~100 minutes at 5 calls/min) has **not** been run — only a 14-day window, to a
  throwaway temp directory. It needs a decision on where `/data` should live.

## [DECISIONS]

### M9/M10 (2026-10-01)

- 2026-10-01T18:05Z [CODE] **Alerts are deduplicated with a persisted cooldown.**
  A provider outage seen at heartbeat intervals would otherwise post all night and
  train the operator to ignore the channel. State lives in `state/alerts.json` so a
  restart does not re-announce what is already known. Recovery is announced too:
  knowing something fixed itself matters as much as knowing it broke.
- 2026-10-01T18:05Z [CODE] **Soft flags never alert.** A real EOD day flags ~8% of
  tickers for vwap; alerting on that would bury the failures that matter.
- 2026-10-01T18:05Z [CODE] The alert payload carries the same text in both `text`
  and `content`, which covers Slack-shaped and Discord-shaped receivers, so one URL
  works with no format configuration. Verified against a live receiver.
- 2026-10-01T18:10Z [CODE] **One-shot CLI job runs alert too**, not just the
  scheduled ones. `DEPLOY.md` tells an operator to run backfills by hand, and those
  are often the runs most worth hearing about; leaving them silent would mean the
  loudest failures were the ones nobody was told about.
- 2026-10-01T18:15Z [CODE] **The manifest is backed up through SQLite's backup
  API**, not copied. It is a live WAL database, so a file copy can capture a torn
  page or miss committed data in the write-ahead log. Verified by taking a backup
  with an open connection mid-write and running `PRAGMA integrity_check` on it.
- 2026-10-01T18:15Z [CODE] Every backup records each file's SHA-256, `restore`
  verifies before and after, and it refuses to overwrite existing published data
  without force. A backup never restored is a guess, and a mistyped restore must
  not destroy the archive it was meant to protect.
- 2026-10-01T18:15Z [CODE] `run_backup` catches `Exception` deliberately. Its whole
  contract is report-don't-crash, and a backup destination fails in many ways
  (permissions, a path that is a file, a full disk). The failure reaches
  `status.json` and the alerter regardless.
- 2026-10-01T18:40Z [CODE] **The burn-in criterion is defined in code**, so the
  answer is a command rather than a judgement: a day is clean when nothing is owed,
  nothing was quarantined, and every published batch matches its checksums. The
  streak is counted *backwards* from the most recent day, because the criterion is
  an unbroken run up to now, not ten good days somewhere in the window. Soft flags
  and explained absences (`skipped` with a reason) do not count against a day.

### M6/M7 (2026-10-01)

- 2026-10-01T17:30Z [CODE] **Minute progress lives in `work/`, not `staging/`.**
  One published minute day is assembled from ~1,000 API calls, so the parts fetched
  so far are real progress. `staging/` is cleared at startup by design, so keeping
  them there would throw away hours of work on every restart. A day is merged,
  validated and published only once no ticker-day is outstanding for it.
- 2026-10-01T17:30Z [CODE] **Dollar volume is computed in Python ints, not Arrow.**
  `close` and `volume` are each scaled by 1e6, so their product carries 1e12 and a
  megacap reaches ~1.3e22 -- three orders of magnitude past int64's 9.2e18. Dividing
  the scale out as each row is accumulated keeps every value in range and exact. A
  test asserts this with real AAPL-sized numbers.
- 2026-10-01T17:35Z [CODE] **`chunk_days > 1` is a separate backfill strategy**: one
  call per ticker per date range instead of per ticker-day. A response at the
  provider's 50,000-bar cap is treated as truncated and the chunk halved and
  retried, since a complete response and a cut-off one are indistinguishable there.
- 2026-10-01T17:35Z [CODE] A bar is assigned to the **exchange** day its timestamp
  falls in, not the UTC date: 00:30 UTC is the previous evening in New York, so
  splitting on UTC would file after-hours bars under the wrong day.
- 2026-10-01T17:40Z [CODE] `--ticker` narrows what a run *works on*, never what a
  day *owes*. Letting it narrow the expectation would publish a day as complete
  with universe members missing.
- 2026-10-01T17:40Z [CODE] **zstd level 9 confirmed by measurement**, as M7 asks.
  On real minute bars, levels 3/9/15 give 20.8 / 20.0 / 19.5 bytes per row: 9 takes
  most of the saving, 15 adds 2.5% for noticeably more CPU.
- 2026-10-01T17:50Z [CODE] Universe membership has an optional instrument-type
  filter (`[universe] types`), defaulting to empty = no filter so nothing changes
  silently. Prompted by real data: SPY ($36bn/day) and QQQ ($26bn/day) outrank every
  actual stock, so ETFs take a share of the most liquid slots. A configured filter
  with no reference snapshot to read is a hard failure, not a silent pass-through --
  ignoring it would admit exactly what it was meant to exclude.
- 2026-10-01T17:45Z [CODE] The scheduled minute job is bounded by
  `[collection] minute_calls_per_run`. The plan says "runs until done", but a
  two-year backfill is ~13,000 calls = ~1.8 days; an unbounded run would still be
  going when the next slot arrived. The remainder resumes tomorrow and nothing is
  lost, because progress is per ticker-day.
- 2026-10-01T17:40Z [CODE] `validate_and_publish(track_coverage=False)` for minute.
  The gate marks day-level coverage, which for a per-ticker dataset wrote a phantom
  `ticker=""` row -- and on a quarantined day it would have been marked failed with
  nothing able to clear it, wedging that day permanently.

### Deployment (2026-10-01)

- 2026-10-01T13:30Z [CODE] **`compose.yaml`, not `docker-compose.yml`, and exactly
  one of them.** Dockge looks for `compose.yaml` in the stack directory; having
  both would be ambiguous to Compose and to a reader. The old file was deleted and
  a test asserts only one deployment compose file exists.
- 2026-10-01T13:30Z [CODE] **The stack builds from source rather than pulling an
  image.** Dockge's Deploy button runs `docker compose up -d`, which builds
  automatically when `build:` is present and the tag is absent. That removes the
  registry from the deployment path entirely: clone into the stack directory,
  fill in `.env`, Deploy. Bumping `COLLECTOR_VERSION` forces a rebuild.
- 2026-10-01T13:30Z [CODE] **Everything operator-editable lives in `.env`**, which
  Dockge exposes in its UI: data directory, uid/gid, timezone, log level, memory
  limit, log rotation. `compose.yaml` should never need editing. A test asserts
  every `${VAR}` in the compose file is documented in `.env.example`.
- 2026-10-01T13:30Z [CODE] **The test runner moved out of the deployment compose**
  into `compose.dev.yaml`, so Dockge does not show a `tests` service in the stack.
- 2026-10-01T13:35Z [CODE] **`catch_up_on_start` (default on).** A container
  deployed at noon would otherwise idle until 18:00 and look broken. Safe because
  every job fills only what the manifest says is missing. Consequence worth
  knowing: a first start begins the EOD backfill, which `DEPLOY.md` says plainly
  and `backfill_days` bounds.
- 2026-10-01T13:35Z [CODE] A scheduled job that *raises* now still writes a
  hard-failure to `status.json`. Previously an exception (revoked key, unwritable
  volume) was logged but skipped `write_status`, so the container kept reporting
  healthy and collecting nothing until the 26-hour staleness window caught it.
- 2026-10-01T13:35Z [CODE] `collector doctor` added. On a container deployment the
  debug loop is slow, so it checks config, paths, permissions, the atomic-publish
  precondition, disk, manifest schema, API key and live connectivity in one
  command, and each failure names the fix rather than leaving an errno.
- 2026-10-01T13:35Z [CODE] Permission errors are translated, not propagated. A
  bind mount Docker created as `root` is the most likely Unraid failure, so the
  message names that cause, prints the uid/gid actually in play and the directory
  owner, and gives the `chown 99:100` fix.
- 2026-10-01T13:35Z [CODE] `check_writable()` probes each directory at startup: a
  directory can exist and still be unwritable, which would otherwise only surface
  hours later when the first batch tried to publish.
- 2026-10-01T13:35Z [CODE] The deployment-facing repo files are COPYed into the
  dev image, so `make test` gives the same result regardless of which paths the
  runner mounts.

### M5 (2026-10-01)

- 2026-10-01T05:45Z [CODE] **Splits and dividends are keyed by the provider's
  `id`, not (ticker, date).** Measured: AFOVF has two separate splits executing
  on 2026-08-05, and JGRRF pays both a CD and an SC dividend on 2026-07-06. Live
  data had 2 duplicate split keys and 245 duplicate dividend keys under the
  plan's keying; `id` is unique across every row of both.
- 2026-10-01T05:45Z [CODE] **`split_from`/`split_to` are float64 — the only
  floats in the storage layer.** The plan specified int64; the provider sends
  genuinely fractional ratios (`"split_to": 1.0216095380029806`), and the int cast
  truncated them, turning any ratio below 1 into 0. That showed up as "17 splits
  with a non-positive ratio" from my own conversion, not from the source. These
  are dimensionless ratios rather than money, float64 is exactly what the
  provider sends, and copying it verbatim round-trips through Parquet without
  drift, so determinism and checksums still hold.
- 2026-10-01T05:45Z [CODE] **Dividends store `currency`.** Live data contains HKD
  and CHF dividends; without the column a CHF 2.70 would read as USD 2.70.
- 2026-10-01T05:50Z [CODE] **The tickers endpoint needs two passes.** Omitting
  `active` returns active names only (13,248 with 0 inactive). `source.tickers()`
  with `active=None` now fetches `active=true` and `active=false` separately and
  concatenates. `check_ticker_snapshot_sanity` stays as the backstop.
- 2026-10-01T05:40Z [CODE] Reference snapshots for past days are retired as
  `skipped`, not retried: there is no historical reference endpoint, so a day not
  captured at the time can never be filled, and leaving it `pending` would report
  a permanent phantom gap.
- 2026-10-01T05:40Z [CODE] Corporate actions re-fetch a 90-day lookback window on
  every run rather than only rows newer than the newest held. Actions are
  announced ahead of time and revised, so a forward-only window would never see a
  correction to an already-recorded row.
- 2026-10-01T05:35Z [CODE] `SCHEMA_VERSION` bumped to 2 for the splits/dividends
  changes. No production data existed, so no migration was needed.
- 2026-10-01T05:35Z [CODE] The timestamp and snapshot checks are now dataset
  scoped: only `eod`/`minute` carry a per-row timestamp to bound, and only
  `tickers`/`universe` carry a `snapshot_date`. A corporate-actions row refers to
  dates (a split's execution date) that are deliberately *not* the batch's.

### M0–M4 (2026-09-30)

- 2026-09-30T19:40Z [USER] Technology choices, as directed by the plan: Massive
  (formerly Polygon.io) free plan as primary source; `yfinance` optional
  cross-check only; Parquet + zstd; Docker Compose on Unraid.
- 2026-09-30T19:45Z [CODE] Python 3.12-slim base (not 3.13): widest wheel
  availability for pyarrow/pandas. Pinned via `pip-compile` run **inside that same
  base** (`make lock`). Resolved: pyarrow 21.0.0, pandas 3.0.6, numpy 2.5.3,
  httpx 0.28.1, pandas-market-calendars 5.4.0, exchange-calendars 4.13.2.
- 2026-09-30T19:50Z [CODE] `ticker` is Arrow `string`, not Arrow `dictionary`.
  The plan asks for dictionary encoding; that is delivered by Parquet's
  `use_dictionary`, which is the on-disk encoding that matters. Arrow's dictionary
  *type* does not survive a read back without reader-side config, which would
  break the exact round-trip M1 requires. Asserted on disk (RLE_DICTIONARY).
- 2026-09-30T19:55Z [CODE] `normalise()` sorts by the dataset's sort columns plus
  any remaining key columns before writing. Sorting is a correctness requirement,
  not a compression trick: idempotency needs a total order on rows.
- 2026-09-30T19:55Z [CODE] Atomic publish is staging → read-back verify →
  `os.replace` → fsync parent dir. Requires `published/`, `staging/` and
  `quarantine/` on one filesystem; `check_same_filesystem()` enforces it at
  startup. Only `state/` may be split onto the cache pool.
- 2026-09-30T20:00Z [CODE] Secret redaction is mechanical, not conventional: a
  `logging.Filter` on every handler rewrites known secret values *and* any
  `api_key`/`token`/`secret` query parameter and `Bearer` token. `status.json` is
  redacted on write too. The key lives only behind `Config.api_key`, never on the
  object, so no repr or dump can leak it.
- 2026-09-30T20:00Z [CODE] Console logs go to **stderr**, so `collector status`
  and `collector info --json` keep stdout machine-readable.
- 2026-09-30T20:00Z [CODE] Unknown keys in `collector.toml` are a startup error;
  a typo must not silently leave a default in place.
- 2026-09-30T21:25Z [USER] **Price and volume scale is 1e6 (6 decimals), not the
  plan's 1e4.** Chosen by the user from measured options. At 1e4 a single real day
  would silently round 276 prices and 8,381 volumes. Largest resulting integer is
  2.57e14 against int64's 9.22e18.
- 2026-09-30T21:25Z [USER] **Volume is a scaled integer, not a share count.** The
  provider reports fractional share volume (`"v": 85227.9653`) in 92% of EOD rows.
- 2026-09-30T21:25Z [USER] Precision beyond the scale is **rounded and soft-flagged**,
  not hard-failed: the data keeps flowing and the loss is visible in
  `status.json` and the manifest. Chosen by the user over quarantining.
- 2026-09-30T21:30Z [CODE] `api.massive.com` is the base URL. Both it and
  `api.polygon.io` answer with Bearer auth; the new host is canonical. The key
  goes in an `Authorization` header, never a query parameter, because query
  strings leak into logs and proxies.
- 2026-09-30T21:40Z [CODE] **vwap-within-[low, high] is SOFT, not the plan's
  hard.** Measured: 977/12,605 rows (7.8%) violate it on a real day, and not
  marginally (VEEAW low 0.0202, high 0.022, vwap 0.01723). As a hard check it
  would quarantine ~8% of every day.
- 2026-09-30T21:55Z [CODE] **Minute↔EOD reconciliation redesigned from
  measurement.** The plan specified summed regular-session volume within a
  tolerance; that is not achievable. Across AAPL/MSFT/F/SOFI on 2026-09-29:
  high/low matched the regular session **exactly** (0.0000%), all-session
  transaction counts matched within 0.7%, but volume differed by 5-29% whatever
  the session scope. So: high/low tight (0.1%), transactions moderate (3%),
  volume kept only as a loose 50% gross-breakage tripwire. Thresholds and the
  evidence are in `rules.py`.
- 2026-09-30T21:40Z [CODE] Minute bar counts are checked against the **day's
  actual session length** from the exchange calendar, not a flat 390, so an early
  close (210 bars on 2026-11-27) is judged correctly.
- 2026-09-30T22:00Z [CODE] **`content_sha256` alongside `checksum_sha256`.** The
  plan asks both that every batch record when it was downloaded and that
  re-running a day reproduce an identical file; a real re-download has a different
  `ingested_at`, so both cannot hold literally. Faking the timestamp would break
  the point-in-time record, so the identity guarantee is expressed over the data
  instead: `content_sha256` hashes the rows excluding `ingested_at`. It also
  detects a provider restatement, which a file hash cannot distinguish from an
  ordinary append.
- 2026-09-30T22:00Z [CODE] For a merged partition, `content_sha256` describes the
  **batch's own rows**, while `rows`/`bytes`/`checksum_sha256` describe the merged
  file. That split is what makes per-day verification possible inside a shared
  monthly file.
- 2026-09-30T21:50Z [CODE] `validate_and_publish` is the only path into
  `published/`, and takes `merge=True` for monthly EOD files. Checks run on the
  day's batch (a whole month would fail `timestamps_in_window`); the structural
  checks that apply to a whole file are re-run on the merged result.

## [PROGRESS]

- 2026-09-30T19:34Z [TOOL] `git init` run (the directory was not a repo). No
  commits made — the user has not asked for one.
- 2026-09-30T21:10Z [TOOL] Live API probed read-only to establish facts the plan
  could not: base URL, auth, rate limit, response shapes, and the `adjusted`
  default. Recorded responses kept as test fixtures (`tests/fixtures/`).
- 2026-09-30T22:05Z [TOOL] Three bugs found by running against the live API
  rather than only fixtures — see [DISCOVERIES]. All three were in code that
  passed a green unit suite.

## [DISCOVERIES]

### API facts (measured, not documented)

- 2026-09-30T21:15Z [TOOL] **`adjusted` defaults to `true`** on both
  `/v2/aggs/grouped/...` and `/v2/aggs/ticker/...`: a request with no `adjusted`
  parameter returns `"adjusted": true`. Storing that would put split-adjusted
  prices on disk, which change retroactively whenever a split happens. The client
  sends `adjusted=false` *and* verifies the response envelope; a mismatch raises
  `NotAdjustedError`. This is the single most consequential detail found.
- 2026-09-30T21:35Z [TOOL] **Rate limit is exactly 5 calls/minute, and rejected
  requests consume quota**: five deliberate `400`s were followed by seven `429`s.
  The limiter therefore records on acquire, before the request, so a failed
  attempt still spends its slot. No rate-limit headers are returned.
- 2026-09-30T21:20Z [TOOL] Grouped daily returns ~12,600 tickers for one date
  (12,605 on 2026-09-29), fields `T/o/h/l/c/v/vw/t/n`. `vw` and `n` are absent on
  ~3 rows; 3 rows have zero volume. 1 distinct timestamp per response.
- 2026-09-30T21:50Z [TOOL] Minute aggregates return the full extended day
  (871 bars for AAPL, 04:00-19:59 ET) with gaps where no trade occurred — 65
  non-60s gaps, all in pre/post-market. Max 50,000 bars per call; at the cap the
  response is treated as truncated, since complete and cut-off are
  indistinguishable.

### M5 API facts (measured)

- 2026-10-01T05:50Z [TOOL] **`/v3/reference/tickers` defaults to active only.**
  No `active` parameter → 13,248 tickers, 0 inactive. `active=true` and
  `active=false` both work and there is no value meaning "both".
- 2026-10-01T05:48Z [TOOL] Dividends are large: 42,851 rows for a 90-day lookback,
  43 pages, ~8.5 minutes at 5 calls/min. Splits are 518 rows in one page.
- 2026-10-01T05:52Z [TOOL] **Dividend `cash_amount` needs up to 8 decimals.**
  1.7% of sampled amounts carry 7-8 dp, so 10,198 of 42,851 rounded at the 1e6
  scale. Worst absolute error $0.0000005, worst relative error 6.0e-6. The
  precision soft flag surfaced this rather than hiding it, which is the behaviour
  that was chosen; whether to raise the scale is an open question for the user.
- 2026-10-01T05:50Z [TOOL] Splits/dividends/tickers all expose a stable `id`;
  dividends also expose `currency` and a `frequency` that can be 0.

### Bugs the first real burn-in report found in itself (2026-10-01)

All three were in the measurement rather than the collector, and all three would
have made the M10 verdict wrong:

- **A day counted as clean because a universe snapshot landed on it.** 2026-09-30
  reported `yes` while EOD and minute had not been collected at all. `judged_any`
  is now set only by the per-day bar datasets.
- **Per-day rows were the monthly cumulative count.** The report showed EOD rows
  climbing 12,591 -> 25,153 -> 37,725 across consecutive days, because `batches.rows`
  holds the merged month's total. Now reads `source_rows` -- the same distinction
  that bit the trailing-median check in M4.
- **A re-collected day was counted twice.** 2026-09-29 reported 34,116 rows because
  two published revisions existed from earlier idempotency tests. Only the current
  revision is counted now; quarantined revisions are still reported, because a
  quarantine is a fault that happened.
- **Storage was summed from batch records**, which multiplies a shared monthly file
  by the number of days in it. Measured from the files on disk instead.

### Bugs the live run found that the unit suite did not

- 2026-09-30T22:05Z [TOOL] **Monthly merging made the trailing ticker-count check
  compare a day against a month.** `batches.rows` holds the merged file's row
  count, so the trailing median was 25,164 while a day's batch was 12,601 — a
  "+49.9% deviation" that hard-failed and quarantined a perfectly good day, then
  tripped the 20% run-abort. Fixed by migration v2 adding `source_rows` (the
  provider's count for that batch) and reading that for the median.
- 2026-09-30T22:15Z [TOOL] **`verify` reported every batch but the last as
  corrupt.** Several EOD batches share one monthly file and each recorded the
  file's checksum *as it stood then*; adding the next day rewrites the file. Fixed
  by verifying the file hash only for the newest batch per path, and verifying
  every batch's `content_sha256` against its own rows filtered out of the file —
  a stronger check that catches a single day being restated.
- 2026-09-30T22:20Z [TOOL] **The content checksum was being overwritten with the
  merged month's.** Every day but the first then failed verification. Fixed: the
  merge branch updates file-level fields only.
- 2026-09-30T20:02Z [TOOL] Job accounting double-counted a quarantined day (once
  as `quarantined`, once in `errors`), inflating the failure ratio that drives the
  run abort. Separated into `errors` (fetch failures) and `quarantine_reasons`.
- 2026-10-01T05:50Z [TOOL] **The survivorship hard check earned its place
  immediately.** The first live reference run was quarantined with "all 13248
  tickers are active; a survivorship-free snapshot must include delisted names" —
  a correct rejection of a biased snapshot my own client had produced by trusting
  the endpoint's default. Nothing was published.
- 2026-10-01T05:52Z [TOOL] **A rate-limit 429 can masquerade as "no data".** A
  probe of `active=true`/`active=false` returned 0 rows and I briefly read that as
  the filter matching nothing; the responses were actually 429s, because the probe
  loop had exhausted the 5-call budget. Checking the status code, not just the
  body, is the lesson. Re-probed correctly, both filters work.

### Library and platform behaviour

- 2026-09-30T20:02Z [TOOL] **`pq.read_table` on a Hive-style path invents
  columns.** Reading `.../year=2026/month=09/eod_2026-09.parquet` goes through the
  dataset layer, which infers `year` and `month` from the directory names and
  appends them, so the file fails its own schema check. Read through
  `pq.ParquetFile(path).read()` instead.
- 2026-09-30T22:00Z [TOOL] **pyarrow's `group_by().aggregate()` puts the group key
  FIRST**, not last: `['ticker', 'high_max', 'low_min', 'volume_sum']`. A
  positional `rename_columns` silently scrambled high into low into volume and
  made the reconciliation check return nothing. Aggregate columns are now
  referenced by their generated names.
- 2026-09-30T20:02Z [TOOL] **`sqlite3.executescript` commits first**, so wrapping
  it in `BEGIN`/`COMMIT` fails with "cannot commit - no transaction is active".
  Migrations carry `BEGIN`/`COMMIT` inside the script. `PRAGMA user_version` is
  transactional, so a half-applied migration cannot leave the version bumped.
- 2026-09-30T20:02Z [TOOL] The dev image's own `COLLECTOR_*` env vars silently
  overrode what tests configured. An autouse `clean_env` fixture clears them.
- 2026-09-30T20:02Z [TOOL] stdout logging corrupted `collector status`'s JSON.
- 2026-09-30T20:40Z [TOOL] **`docker kill` does not exercise a restart policy**
  (Docker treats it as a manual stop) and **`kill -9 1` inside a container does
  nothing** (PID 1 in a namespace ignores signals from inside it). Crash recovery
  was verified instead as: plant a leftover staging batch → `docker kill` →
  `docker start`.
- 2026-09-30T19:48Z [TOOL] Bash mis-parses very long quoted heredocs; large files
  are written with the editor tools.

## [OUTCOMES]

### M0 — repo and container skeleton: **done**, 2026-09-30

| Criterion | Evidence |
|---|---|
| Container builds and starts | `docker ps` → `Up (healthy)` |
| Heartbeat in `status.json` | `updated_at` advanced 02:53:38 → 02:54:38 over a 70s wait |
| Survives a reboot | `restart: unless-stopped`; cold start after `docker kill` came up healthy and cleared the interrupted staging batch. Real-reboot test deferred to the Unraid host (M9) |
| API key from the environment | `collector info --json` → `api_key_present: true` |
| Key never in logs | `grep -c` of `/data/logs/collector.log` **and** `docker logs` → 0 hits, with the key deliberately set |
| Choices recorded | [DECISIONS] |

### M1 — schema and storage: **done**, 2026-09-30

Exact round-trips including scaled prices and fractional volume; a simulated
mid-write crash leaves nothing in `published/`; the manifest records checksum,
`content_sha256` and `ingested_at`, and `verify` detects tampering, restatement
and missing files.

### M2 — quality checks: **done**, 2026-09-30

Every hard check has a corrupted fixture that is quarantined; every soft check a
fixture that publishes with a flag (`tests/test_checks.py`). Results reach the
manifest and `status.json`. Validated against real recorded responses
(`tests/test_real_data.py`): **no hard check fires on a real EOD or minute day**,
and the soft checks fire at the observed rates (vwap 977/12,605).

### M3 — rate limiter and API client: **done**, 2026-09-30

Limiter holds ≤5 calls in *any* 60s window over 30 simulated minutes
(`test_never_exceeds_the_budget_over_a_simulated_half_hour`), is thread-safe
under contention, and counts failed attempts. 429/5xx retry with jittered
backoff, honouring `Retry-After` and penalising the shared limiter; partial
responses never reach the caller.

### M4 — EOD backfill and daily job: **done for a 14-day window**, 2026-09-30

Live, against the real API, 11 trading days (2026-09-15 … 09-29):

```
eod: published=11 quarantined=0 errors=0 outcome=soft_flags
verify: all published files match their manifest checksums
gaps:   eod: ok=11
3.1 MB for 11 days in one monthly file (~280 KB/day, ~70 MB/year)
```

| Criterion | Evidence |
|---|---|
| All trading days present, checks passing | 11/11 published, 0 quarantined; weekends and holidays correctly not expected |
| Delisted tickers present | 277 tickers traded on 2026-09-15 but not on 09-29, and 291 the reverse, in an 11-day window — the feed is a point-in-time snapshot. **The "marked inactive in reference data" half needs M5.** |
| Delete a day and re-run restores it | Dropped 2026-09-23 from the file and the manifest; the next run refilled it unprompted and `content_sha256` matched exactly (`7bd1ea72fbfc359a…`) |

Not done: the full 2-year backfill. See [PLANS].

### M5 — reference and corporate actions: **done**, 2026-10-01

Live, against the real API.

**Ticker snapshot** (`reference/tickers_2026-10-01.parquet`, 36,697 rows):

```
active    13,248
inactive  23,449   (64% of the universe)
  of which 22,944 carry a delisting date, spanning 2003-09-11 .. 2026-09-29
  505 inactive without one -> soft flag, published
types: ADRC ADRP ADRR CS ETF ETN ETS ETV FUND INDEX PFD RIGHT
```

**Corporate actions**: 518 splits (1 page), 42,851 dividends (43 pages, ~8.5 min).
Currencies present: USD 38,703, JPY 1,978, CAD 782, HKD 349, GBP 261, EUR 189 --
which is why `currency` is stored; a JPY 12,994 dividend read as USD would wreck
any adjustment. `cash_amount` spans 0.000028 .. 12,994.1.

| Criterion | Evidence |
|---|---|
| Daily ticker snapshots accumulate as point-in-time records | Dated files, never overwritten; each row stamped with its own `snapshot_date`, enforced by a hard check |
| Split cross-check runs, reviewed on a known historical split | Below |

**Split cross-check, reviewed on 2026-09-29** (41 splits on the books):

```
large moves flagged WITHOUT split context: 17
large moves flagged WITH    split context: 12   -> 5 explained

AGRZ  0.189  -> 3.27    split 1:0.05  implies 3.78    actual/implied 0.865
CDT   0.076  -> 1.45    split 1:0.04  implies 1.9     actual/implied 0.763
ONMD  0.633  -> 5.2094  split 1:0.1   implies 6.33    actual/implied 0.823
TRUG  0.2658 -> 2.96    split 1:0.1   implies 2.658   actual/implied 1.114
VRME  0.99   -> 8.14    split 1:0.1   implies 9.9     actual/implied 0.822

split-without-discontinuity: none
```

All five are reverse splits, and each price move tracks its ratio to within
10-25% -- the residual being real movement on top of the consolidation, which is
unremarkable for micro-caps. The remaining 12 moves are genuine unexplained moves
on warrants and micro-caps (ARBEW +193%, NIVF -79%) and are published with a flag
rather than rejected, as the plan requires.

**M4's outstanding criterion is now closed**: 43 tickers that traded during the
collected 11-day EOD window are marked inactive in the reference snapshot (ANY,
BRR, CSAN, DOMO, FEED, ...), so the price history is survivorship-free *and* the
delistings are identifiable.

**Verification across every dataset**: `verify` passes on 14 published batches /
4 files; `gaps` reports `eod: ok=11`, `tickers: ok=1`, `splits: ok=1`,
`dividends: ok=1`.

### M6 — universe and minute backfill: **done**, 2026-10-01

**The universe, built from the real 11 days of EOD data** (no API calls):

```
rank  ticker     mean daily $ volume
   1  SPY             36,216,341,277
   2  QQQ             26,329,462,454
   3  MU              24,648,527,061
   4  NVDA            23,877,185,125
   5  META            19,779,844,917
  ...
10 of 13,162 candidates over a 20-trading-day window
```

| Criterion | Evidence |
|---|---|
| Universe snapshot built from EOD data, stored with its date | `reference/universe_2026-09-30.parquet`; the ranking is plausible on its face -- the two largest ETFs, then the most-traded semis and megacaps |
| Minute backfill resumes after being stopped mid-run | Below |
| Minute-to-EOD reconciliation >= 99% of ticker-days | **100%** (10/10), below |

**Resumption, live.** Stopped deliberately on a call budget, then resumed:

```
run 1: --max-calls 4  ->  4 calls, 0 days published, 6 ticker-days still owed
                          work/minute/2026-09-29/: AMD INTC META MU
                          published minute files: 0
                          gaps: minute: ok=4, pending=6

run 2: (unbounded)    ->  6 calls, published 2026-09-29 (10 tickers, 8,906 bars)
                          0 ticker-days still owed
```

Exactly 6 calls on the resume -- one per outstanding ticker, nothing refetched. A
real 429 landed mid-run; the limiter penalised itself 60s, retried and succeeded,
which is the first time that path has run against the live API.

**Reconciliation, 10 real megacaps on 2026-09-29.** All 10 have exactly 390
regular-session bars, with high/low agreeing with the EOD bar to **0.000%**:

```
AMD   390 bars   624.13 vs  624.13 (0.000%)    INTC  390   118.75 vs   118.75
META  390        740.45 vs  740.45 (0.000%)    MU    390  1082.66 vs  1082.66
NVDA  390        232.82 vs  232.82 (0.000%)    QQQ   390   740.58 vs   740.58
SNDK  390       1749.78 vs 1749.78 (0.000%)    SPCX  390   150.06 vs   150.06
SPY   390        766.98 vs  766.98 (0.000%)    TSLA  390   358.74 vs   358.74

pass rate 100% (10/10)      11 checks passed, 1 soft (vwap), 0 hard
```

This vindicates the M5 decision to reconcile high/low against the **regular session
only**: including extended hours would have broken every one of these.

**Chunked backfill, live.** 3 tickers over a 10-day window with `--chunk-days 40`:
3 calls filled 18 ticker-days, split correctly into 6 daily partitions
(2026-09-21 .. 09-28, 3 parts each).

### M7 — minute daily job: **done**, 2026-10-01

| Criterion | Evidence |
|---|---|
| Runs to completion within its call budget | A 10-ticker day completed in 10 calls; `minute_calls_per_run` bounds a scheduled run and the remainder resumes |
| Measured compressed size recorded; level chosen and logged | Below |

**Measured, not estimated** (10 megacaps, 2026-09-29, extended hours on):

```
8,906 rows, 0.19 MB  ->  21.4 bytes/row
891 bars per ticker-day
session mix: 33% pre-market, 44% regular, 23% after-hours

extrapolated to 1,000 tickers:  19.1 MB/day  ->  4.81 GB/year
```

The plan estimated 2-4 GB/year with extended hours; measured is ~20% above the top
of that range, driven by extended hours being a third of all bars.

**Compression level, chosen by measurement** on real minute bars:

```
zstd 3  -> 20.8 bytes/row
zstd 9  -> 20.0 bytes/row   <- default: most of the available saving
zstd 15 -> 19.5 bytes/row      only 2.5% more, for noticeably more CPU
```

### M9 — monitoring and backups: **done**, 2026-10-01

| Criterion | Evidence |
|---|---|
| A simulated outage triggers an alert, and the next run fills the gap automatically | Below |
| Backup job runs and a restore from backup is tested once | Below |

**The outage, live**, against a real webhook receiver and a genuinely unreachable
provider host:

```
1. provider unreachable  ->  eod: published=0 errors=2 outcome=hard_failure
   receiver got:  [error] eod hard_failure
                  2026-09-29: ConnectError requesting /v2/aggs/grouped/...

2. provider reachable    ->  eod: published=2 errors=0 outcome=soft_flags
   receiver got:  [resolved] eod hard_failure
```

The second run is the *same command* with nothing done by hand, and it filled both
gaps. Alert delivery itself was verified separately with `collector alert-test`
against the same receiver (one `[info]`, one `[resolved]`).

**Backup and restore, live**, on the real collected data:

```
backup   -> /backups/backup-20261001T181535Z: 8 file(s), 6.4 MB, data=yes, verified
backups  -> listed, schema v3
restore  -> restored 8 file(s) into an empty data root
verify   -> published files: 6; manifest published batches: 17
            all published files match their manifest checksums
gaps     -> eod: ok=11   minute: ok=28, pending=42   tickers: ok=1
```

The coverage state survives the restore, so a restored collector resumes exactly
where the backed-up one was rather than re-fetching.

### M10 — burn-in: **apparatus done and verified; the clock runs on deployment**

M10 asks for "10 consecutive trading days with no hard failures, no unexplained
gaps and no manual intervention". That is two calendar weeks of wall-clock
observation and **cannot be completed here** -- it needs the collector running
unattended on the Unraid box. What is done:

| Part | State |
|---|---|
| The criterion, defined precisely and measured from the manifest | Done (`collector burnin`) |
| The write-up the plan asks for: storage size, flag rates, issues found | Done (`--journal`) |
| 10 consecutive clean trading days actually observed | **Runs after deployment** |

Verified against the real test data, where it correctly reported `IN PROGRESS --
0/10` and named every fault, including the partial minute days left by the bounded
test runs:

```
day          clean        rows  notes
2026-09-15   yes        12,591  1 soft-flagged batch(es); not collected: minute
2026-09-21   NO         12,626  minute: 7 unit(s) still owed
2026-09-29   yes        21,511  2 soft-flagged batch(es)
2026-09-30   NO             10  no daily dataset collected for this day

rows collected in window: 160,062
published on disk:        6.3 MB
soft-flag rate:           92% of days carry at least one flag
```

Writing that report found three accuracy bugs in the measurement itself -- see
[DISCOVERIES].

### Full unattended run: **verified**, 2026-10-01

A cold start with alerting and backups enabled, left alone. All seven jobs ran in
sequence in 19 minutes with no intervention:

```
18:31  reference          36,706 rows   (7 min -- pages the whole ticker universe twice)
18:38  corporate_actions  48,152 rows   (10 min -- 43 pages of dividends)
18:48  eod                published=3
18:48  universe           built from the EOD just collected
18:48  minute             3 days x 3 tickers        (2 min)
18:50  checks_report
18:50  backup             10 files, 4.3 MB, verified
```

Resulting dataset, verified:

```
eod/year=2026/month=09/eod_2026-09.parquet
minute/.../day={28,29,30}/minute_2026-09-{28,29,30}.parquet
corporate_actions/{splits,dividends}.parquet
reference/{tickers,universe}_2026-10-01.parquet

verify:   8 files, 10 batches, all checksums match
coverage: eod ok=3  minute ok=9  tickers/splits/dividends/universe ok=1
alerts:   none -- every outcome was ok or soft_flags
burn-in:  3/10 consecutive clean trading days, and counting
```

The order matters and held: corporate actions landed before EOD, and the universe
was rebuilt before the minute job used it. The burn-in correctly treats the three
days *before* the install as "not collected" rather than as faults.

### Deployment: **verified**, 2026-10-01

Exercised the real Dockge path rather than assuming it: copied the repo into a
stack directory, wrote the `.env` an operator would write, and ran what Dockge's
Deploy button runs.

```
docker compose up -d        ->  collector Built; Container Started
docker compose ps           ->  Up 12 seconds (healthy)  tick-collector:0.1.0-deploytest
```

`doctor` inside the deployed container, against the live API:

```
[  ok] config           loaded from /app/config/collector.toml
[  ok] timezone         America/New_York -> America/New_York
[  ok] permissions      published/ staging/ quarantine/ state/ logs/ all writable
[  ok] atomic publish   published/, staging/, quarantine/ share a filesystem
[  ok] disk space       47.5 GB free
[  ok] manifest         schema v3, 0 batch(es) recorded
[  ok] api key          present in the environment
[  ok] api reachable    https://api.massive.com reachable and authenticated
[  ok] health           ok, status 19s old
All checks passed.
```

Jobs registered and the catch-up pass fired on time:

```
13:19:02 scheduler started, tz=America/New_York,
         jobs=reference, corporate_actions, eod, checks_report
13:21:02 running startup catch-up pass over 4 job(s)      <- exactly 120s later
13:21:02 job reference starting
```

Stopped there deliberately: the pass had begun the real backfill, and the
decision was to hold that until the Unraid share exists.

The three permission tests skip as root (root ignores the bits), so they were run
separately as an unprivileged user -- `--user 1000:1000` -- where all three pass.

### Verification state, 2026-10-01T13:45Z

All inside the container, per AGENTS.md's container-first policy:

```
ruff check            All checks passed!
ruff format --check   36 files already formatted
mypy                  Success: no issues found in 20 source files
pytest                360 passed, 2 skipped (root-only permission cases)
```

The suite also passes from the bare image with no mounts at all, which is what
makes `make test` trustworthy.

Lowest coverage is `cli.py` (59%), which is argument plumbing exercised by the
live runs rather than by tests.

### Lessons

- Writing each milestone's "definitions of done" as tests first found four
  defects in M0/M1 alone.
- **Probing the live API before building against it changed the design four
  times** — the scale, the volume type, the vwap severity and the whole
  reconciliation approach. Every one of those would have been a silent data
  problem discovered months in.
- Three further bugs survived a fully green unit suite and only appeared on a
  real multi-day run. The common factor: all three involved monthly merging, which
  single-batch fixtures cannot exercise.
- **The survivorship hard check paid for itself on first contact with the real
  endpoint** (M5), rejecting a biased snapshot that every unit test had accepted.
  Writing a check for the failure the dataset exists to prevent — rather than only
  for the failures the code might have — is what caught it.
- Two of M5's three schema errors came from assuming the plan's key and types were
  right rather than measuring: `(ticker, date)` is not unique, and split ratios
  are fractional. Both would have been silent.

## Follow-ups

- [ ] **Rotate the API key** — it was pasted into the chat transcript.
- [ ] Run the full 2-year EOD backfill (~500 days, ~100 min) once `/data` has a
  real home.
- [ ] **Decide whether to raise the scale for dividends.** 10,198 of 42,851 live
  dividend amounts rounded at 1e6 (1.7% of values carry 7-8 decimals). Worst
  absolute error $0.0000005, worst relative error 6.0e-6 — negligible for
  adjustment, and it is the rounded-and-flagged behaviour that was chosen. 1e8
  would be lossless.
- [ ] Re-check the zstd choice on a *month* of minute data once a backfill exists.
  The current measurement is one day of 10 liquid tickers: it confirms the ordering
  but a wider sample could shift the margins.
- [ ] Decide whether the minute universe should exclude ETFs
  (`[universe] types = ["CS"]`). Default is no filter, which puts SPY and QQQ at
  ranks 1 and 2.
- [ ] **Enable alerting**: put a webhook in `COLLECTOR_ALERT_WEBHOOK` and confirm
  with `collector alert-test`. Without it, failures reach `status.json`, the logs
  and Dockge's health badge, but nothing pushes to you.
- [ ] **Enable backups**: set `[backup] directory`, uncomment the `/backups` mount
  in `compose.yaml`, and point it at a different share from the data.
- [ ] **Run `collector burnin` after ~2 weeks** for M10's verdict, and
  `--journal` for the write-up the plan asks for.
- [ ] Revisit the minute↔EOD tolerances once minute data exists for the whole
  universe; they were set from four tickers on one day.
- [ ] **Create the data share before the first Deploy**:
  `mkdir -p /mnt/user/quant-data && chown 99:100 /mnt/user/quant-data`. Docker
  creates a missing bind source owned by root, which the unprivileged container
  cannot write; `doctor` and startup both report this clearly, but creating it
  first avoids the round trip. Confirm the owner with
  `stat -c '%u %g' /mnt/user/quant-data` and set `APP_UID`/`APP_GID` to match.
- [ ] Decide the backfill window before the first Deploy. `catch_up_on_start`
  means a first start begins collecting ~2 minutes in; with the shipped
  `backfill_days = 730` that is ~100 minutes of EOD plus ~15 for the first
  reference snapshot.
