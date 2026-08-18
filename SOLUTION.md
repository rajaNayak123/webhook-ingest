# SOLUTION.md

## What was broken, and why

**Duplicate records / inflated stats.** `events.event_id` had only a
non-unique index, and `Ingest()` deduplicated with check-then-act:
`SELECT ... exists`, then a separate `INSERT`. Two redeliveries of the same
`event_id` arriving close together — normal for an at-least-once provider —
could both pass the exists-check before either inserted, then both write
the call and increment `account_stats`.

**Recordings never marked processed, no logs.** The background goroutine
ran on the inbound request's context. `net/http` cancels that context the
instant the handler returns, before the goroutine's 50ms sleep elapses, so
`MarkRecordingProcessed` always failed — and the error was discarded by a
bare `// TODO: handle`.

**In-flight work disappearing on deploy.** That goroutine was untracked;
`main()` returned right after `srv.Shutdown` with no regard for work still
in flight, so `SIGTERM` on deploy killed it.

**Bonus, same symptom:** `stats.Cache.Record` took no lock at all while
`Get` uses `RLock` — a second, independent source of the reported drift.

## Fixes

- `migrations/002`: `UNIQUE` constraint on `events.event_id`.
- `store.IngestEvent`: event insert (`ON CONFLICT DO NOTHING`), call
  upsert, and stats increment in **one transaction**, gated by that
  constraint; reports whether the delivery was new. The cache is only
  updated when it is.
- `Cache.Record` now takes the write lock.
- Recording processing runs on a detached, timed-out context, tracked by a
  `sync.WaitGroup`; new `Service.Shutdown` waits on it, called from
  `main()` after `srv.Shutdown`.
- Tests added that fail pre-fix, pass post-fix: concurrent-duplicate
  delivery, recording-marked-after-response, shutdown-drains-in-flight
  work, and a `-race` test for the cache.

## Why Postgres over Redis for dedup

Doing the exists-check and the write in the same Postgres transaction
removes any window where a Redis flag and the Postgres row could disagree
(e.g. `SETNX` succeeds, the Postgres write fails) — one source of truth,
and `ON CONFLICT DO NOTHING` gives a race-proof gate for free on a write
we're already making. Redis would add a round trip with no correctness
gain here; it would earn its place as a fast-path pre-check ahead of
Postgres, which I left out as an optimization rather than a fix.
`internal/redisclient` is wired if that's wanted later.

## At 10,000 webhooks/sec

- Redis pre-check so most redeliveries short-circuit before Postgres.
- Batch writes (`COPY`/multi-row upsert) or a durable queue in front of
  ingestion, instead of one `INSERT` per request.
- Bound recording-processing goroutines with a worker pool sized to
  `DB_MAX_CONNS`.
- Make `account_stats` eventually consistent — a single hot row per
  popular account becomes a lock bottleneck at this rate.
- The core invariant (a unique constraint on `event_id`) stays; only how
  cheaply we check it before hitting Postgres would change.
