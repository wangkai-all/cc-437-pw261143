# Incremental Sync Service

An incremental synchronization service for a source whose **cursors cannot be
trusted** (they expire, get repeated, or move backwards) feeding a target that
must stay **idempotent by business key + version**. Pure Python 3.11 standard
library — no dependencies, no services besides a sqlite file.

## Guarantees

| Requirement | How it is enforced |
|---|---|
| Idempotent target | `target` primary key is `(biz_key, version)`; re-applying any record is a no-op or a logged conflict, never a duplicate row |
| Version-aware conflicts | An incoming version older than the target's newest version for a key is rejected and written to `conflicts` (`stale_version`) |
| Expired / duplicate / rolled-back cursor | Every cursor response is validated; any anomaly suspends cursor mode and starts **time-window compensation** |
| No data loss at window edges | Windows are half-open `[start_ts, end_ts)`; adjacent windows meet exactly (`end == next.start`), so an edge row belongs to exactly one window |
| No duplicates from overlap | Overlapping/retried windows re-read the same rows; the `(biz_key, version)` key plus deterministic batch ids make re-application harmless |
| Interruptible / resumable | Each batch is one sqlite transaction with a deterministic `batch_id`; after any crash, an already-committed batch is skipped and an uncommitted one is re-applied |
| Partial batch failure | A failed window stays `failed`, later windows stay `pending`; the console's **Continue** retries failed windows and finishes pending ones |
| Full request audit | Every source request attempt is logged in `request_log` with cursor/window, attempt number, response summary (row ids), error type and retry reason |

## Architecture

```
sync_service/
  source.py    Source API + ScriptedSource (deterministic anomaly injection)
  storage.py   sqlite state: target, batches, request_log, windows, conflicts
  engine.py    State machine: cursor mode → anomaly → window compensation
  console.py   HTTP dashboard + /continue resume entry (stdlib http.server)
  __main__.py  CLI: demo / console
tests/         unittest fixtures: anomalies, boundaries, crashes, retries
run_tests.py   Test runner (works under `python -I`)
run_demo.py    Demo launcher (works under `python -I`)
```

### The state machine

1. **Cursor mode** pages with the source cursor. Each page is applied
   atomically under batch id `cur::<cursor>`. The durable watermark
   (`safe_updated_at`) only advances from data that is actually in the target.
2. Cursor responses are checked for three pathologies:
   - `CursorExpiredError` raised by the source → **expired**
   - next cursor decodes to the same offset as the inbound one → **duplicate**
   - next cursor decodes to a smaller offset → **rollback**
3. On any anomaly, the engine builds half-open compensation windows from the
   last durable watermark up to the source's newest timestamp and switches to
   **window mode**. Window pagination uses row offsets inside a fixed,
   immutable time range, so it cannot roll back.
4. When every window is `done`, the cursor is re-anchored past the watermark
   (`cursor_at_timestamp`) and cursor mode resumes.
5. Transient errors are retried per page (`max_retries`); exhausting retries
   leaves durable state exactly where it stopped for a later resume.

### Durable tables (sqlite)

- `target` — idempotent row versions, `PRIMARY KEY (biz_key, version)`
- `batches` — applied batches (the resume checkpoint), unique `batch_id`
- `request_log` — one row per source request **attempt** (see below)
- `windows` — `pending | running | done | failed` with `last_offset` resume point
- `conflicts` — rejected stale versions with the incoming payload preserved
- `state` — watermark, mode, current/committed cursor, compensation flags

## Running

```bash
# Full suite (24 tests), isolated mode recommended
python3 -I run_tests.py

# Scripted demo: expired cursor, transient blip, rollback, redelivery,
# stale version, simulated mid-run crash + automatic resume
python3 -I run_demo.py demo --db data/demo.db --no-serve

# Same demo plus the web console on http://127.0.0.1:8080
python3 -I run_demo.py demo --db data/demo.db --port 8080

# Console against an existing database
python3 -I run_demo.py console --db data/demo.db --port 8080
```

## Console

| Route | Purpose |
|---|---|
| `GET /` | Dashboard: water-level cards, durable state, windows, conflicts, target preview, latest 50 request-log rows, Continue button |
| `GET /api/state` | Water level, mode, cursor, row/conflict/window counts |
| `GET /api/windows` | Compensation windows and statuses |
| `GET /api/conflicts` | Conflict records |
| `GET /api/requests` | Full request audit trail |
| `POST /continue` | Resume the durable state machine (form → 303 redirect; JSON content-type → run summary). Returns `409` while a run is already in progress |

### What the request log captures

For every attempt against the source: mode (`cursor`/`window`), inbound
cursor or `[window_start, window_end)` + page offset, attempt number, row
count, next cursor, a stable **response summary** (sorted `biz_key#version`
fingerprint), result (`ok`/`error`/`compensation`), error type and the
**retry reason** (`transient: …`, `cursor_expired`, `duplicate_cursor`,
`cursor_rollback`).

## Test scenarios

`tests/test_engine.py` — happy path, rerun no-op, redelivery dedup, expired
cursor, duplicate cursor, rollback, transient retry/backoff, half-open
boundaries, overlapping windows, stale-version conflict, failed-window
retry.

`tests/test_resume.py` — crash before commit (rollback + re-apply), crash
after commit (durable batch skipped on resume), crash mid-compensation
(resumes at the window's `last_offset`), two consecutive crashes converging,
partial batch failure with good windows preserved and failed window retried.

`tests/test_console.py` — dashboard sections, JSON APIs, `/continue`
resumption, conflicts API.

## Design notes

- **Deterministic batch ids** (`cur::<cursor>`, `win:<window>:<offset>`) are
  the single mechanism that makes every replay path safe: the same logical
  batch can never apply twice, regardless of when the process died.
- The watermark is derived from **durable target data**, never from a
  source-claimed cursor, so a lying cursor cannot push the sync point past
  data we do not actually hold.
- Window ids embed their time bounds, so resuming processes rebuild
  identical windows (`ON CONFLICT DO NOTHING`) and never split or duplicate a
  range.
- `ScriptedSource` is a test double, but its interface (`fetch_page`,
  `fetch_window_page`, `cursor_at_timestamp`, `max_updated_at`) is the
  adapter contract a real source client would implement.
