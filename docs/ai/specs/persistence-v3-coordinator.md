# Persistence v3: FRP-Aligned Coordinator Design

> **Status:** Design approved. Ready for implementation plan.
> **Branch:** `v3-persistence` (dedicated; clean cut-over from v2).
> **Target release:** `3.0.0`.

## Goal

Extract transactional coordination out of the statechart persistence layer's domain rows into its own schema relations and its own Clojure abstraction, so that `statechart_events` and `statechart_jobs` become append-only facts and the env map stops carrying infrastructure hooks.

## Motivation

Moseley & Marks (*Out of the Tar Pit*, 2006) argue that accidental complexity grows when essential state and control state are welded together. The v2 JDBC persistence layer welds them in two places:

1. **Schema-level:** `statechart_events.claimed_at`/`claimed_by`/`processed_at` and `statechart_jobs.status`/`attempt`/`lease_*`/`result`/`error`/`terminal_event_*` live on the same rows as the events/jobs they coordinate. Claim release mutates the event row; retry stomps the prior `error`; cancel DELETEs the event row.
2. **Clojure-level:** `JdbcEventQueue` and `JdbcWorkingMemoryStore` cooperate transactionally via loose env keys (`::sc/on-save-hooks` — a vector of callbacks; `::sc/save-attempted?` — a shared atom; `::wake-signal` — a `LinkedBlockingQueue`). The coordination contract is implicit and hand-maintained.

Both are precisely the "accidental complexity scattered across modules" the paper predicts. Downstream symptoms in the v2 codebase:
- `create-job!` runs a 5-attempt race loop against adversarial ON CONFLICT / SELECT interleavings.
- `claim-events!` enforces per-session FIFO via a cross-row `NOT EXISTS` on the same table.
- `save-working-memory!` threads a `save-attempted?` atom through env so the event queue can distinguish "handler never saved" from "save rolled back".
- `cancel-events!` hard-DELETEs events (violates the append-only-facts principle the SCXML spec permits).

This spec extracts the coordination into its own layer — both in schema (side relations) and in Clojure (`PersistenceCoordinator` protocol) — so that essential domain state (the events sent, the jobs defined) is observably separate from the coordination state (who's processing what, which attempt is running, what has been dispatched).

## Scope

### In scope

- Schema v3 for the JDBC backend: append-only `statechart_events` / `statechart_jobs` plus side relations for claims, cancellations, leases, results, terminal dispatches.
- `PersistenceCoordinator` protocol and `JdbcCoordinator` implementation.
- Replacement of env-carried coordination hooks (`::sc/on-save-hooks`, `::sc/save-attempted?`, `::wake-signal`) with one typed env key (`::sc/persistence-coordinator`) plus `::sc/current-event-handle`.
- Idempotent `v2-to-v3/migrate!` function with quiesce-first operational contract.
- Retention functions (`purge-processed-events!`, new `purge-terminated-jobs!`) updated for v3 schema.
- Test migration and new coordinator/migration specs.

### Explicitly out of scope (separate future threads)

- **Working-memory decomposition.** `statechart_sessions.working_memory` stays BYTEA-via-nippy. Making it queryable relations fights the library's generic-engine mission.
- **Id encoding simplification.** The `encode-id` / `decode-id` text-via-`pr-str` scheme stays. Replacing it with typed columns or JSONB is its own migration.
- **Protocol signature changes.** `EventQueue` and `WorkingMemoryStore` protocol signatures are unchanged. All cleanup happens within existing signatures via env-key replacement.

### Non-goals

- Zero-downtime migration. The migration contract is **quiesce workers → run `migrate!` → redeploy on v3 code**.
- Dual-write / dual-read transition. No v2/v3 coexistence.
- Backwards compatibility of DB schema. `2.x` → `3.0` requires the migration.

## SCXML Conformance

The design was verified against the W3C SCXML 2015-09-01 Recommendation. Summary:

- **External event queue ordering** (§3.13 mainEventLoop): FIFO-by-implication. v3 preserves per-session FIFO via the same invariant as v2, shifted to a cross-table `EXISTS` query on `statechart_event_claims`.
- **Exactly-once delivery** (protocols.cljc:132-137 contract): preserved by `commit-event-processing` wrapping the WM-save and event-ACK in the same tx.
- **`<cancel>` semantics** (§6.3): the spec requires "prevents event delivery", not "removal from storage". v3's `statechart_event_cancellations` side relation is strictly more spec-aligned than v2's hard-DELETE.
- **`<invoke>` lifecycle** (§3.7): `done.invoke.ID` / `error.invoke.ID` must fire on termination. v3 computes these at dispatch time from `invokeid` + `job_results.status`/`.result`/`.error`/`.metadata`. The event shape is identical to v2's stored `terminal_event_*` values; the migration test includes a golden-file check that v3's computed terminal event matches every v2 row's stored value.
- **Macrostep boundary** (§3.13): external queue consulted after internal-queue drain. `commit-event-processing` wraps exactly one external-event macrostep. No change.

## Schema v3

### Unchanged from v2

```sql
CREATE TABLE statechart_sessions (
  session_id     TEXT PRIMARY KEY,
  statechart_src TEXT NOT NULL,
  working_memory BYTEA NOT NULL,
  version        BIGINT NOT NULL DEFAULT 1,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Events + claims + cancellations

```sql
-- Append-only event facts. No claim/processing columns.
CREATE TABLE statechart_events (
  id                BIGSERIAL PRIMARY KEY,
  target_session_id TEXT NOT NULL,
  source_session_id TEXT,
  send_id           TEXT,
  invoke_id         TEXT,
  event_name        TEXT NOT NULL,
  event_type        TEXT DEFAULT 'external',
  event_data        BYTEA,
  deliver_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_events_deliver_at ON statechart_events(deliver_at, id);
CREATE INDEX idx_events_cancel     ON statechart_events(source_session_id, send_id, deliver_at);
CREATE INDEX idx_events_target     ON statechart_events(target_session_id, deliver_at);

-- Existence = claim taken. processed_at IS NOT NULL = handler committed.
CREATE TABLE statechart_event_claims (
  event_id     BIGINT PRIMARY KEY REFERENCES statechart_events(id) ON DELETE CASCADE,
  claimed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_by   TEXT NOT NULL,
  processed_at TIMESTAMPTZ
);
CREATE INDEX idx_event_claims_recovery
  ON statechart_event_claims(claimed_at)
  WHERE processed_at IS NULL;

-- Append-only cancellation facts. Row existence = event cancelled.
CREATE TABLE statechart_event_cancellations (
  event_id     BIGINT PRIMARY KEY REFERENCES statechart_events(id) ON DELETE CASCADE,
  cancelled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  cancelled_by TEXT
);
```

### Jobs + leases + results + terminal dispatches

```sql
-- Append-only job definitions. terminated_at is explicit denormalization
-- (see "Derived-state discipline" below); written in the same transaction
-- as the matching statechart_job_results row.
CREATE TABLE statechart_jobs (
  id            UUID PRIMARY KEY,
  session_id    TEXT NOT NULL,
  invokeid      TEXT NOT NULL,
  job_type      TEXT NOT NULL,
  payload       BYTEA NOT NULL,
  max_attempts  INT NOT NULL DEFAULT 3,
  terminated_at TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Idempotency: at most one active (non-terminated) job per (session, invoke).
CREATE UNIQUE INDEX idx_jobs_active_per_invoke
  ON statechart_jobs(session_id, invokeid)
  WHERE terminated_at IS NULL;
CREATE INDEX idx_jobs_session ON statechart_jobs(session_id, created_at DESC);

-- A lease row exists iff the job is not yet terminated.
-- attempt_error is transient (per retention policy C): overwritten on each claim.
-- Terminal errors go to job_results, not here.
CREATE TABLE statechart_job_leases (
  job_id           UUID PRIMARY KEY REFERENCES statechart_jobs(id) ON DELETE CASCADE,
  attempt          INT NOT NULL DEFAULT 0,
  next_run_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_owner      TEXT,
  lease_expires_at TIMESTAMPTZ,
  attempt_error    BYTEA,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_leases_claimable
  ON statechart_job_leases(next_run_at, lease_expires_at);

-- Append-only terminal outcomes. One row per terminated job.
CREATE TABLE statechart_job_results (
  job_id       UUID PRIMARY KEY REFERENCES statechart_jobs(id) ON DELETE CASCADE,
  status       TEXT NOT NULL CHECK (status IN ('succeeded', 'failed', 'cancelled')),
  result       BYTEA,
  error        BYTEA,
  metadata     BYTEA,
  completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_results_completed_at ON statechart_job_results(completed_at);

-- Append-only dispatch claim. Row existence = terminal event sent
-- (or committing to be sent in the same tx).
CREATE TABLE statechart_terminal_dispatches (
  job_id        UUID PRIMARY KEY REFERENCES statechart_jobs(id) ON DELETE CASCADE,
  dispatched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Derived-state discipline

- **Job status** (pending / running / succeeded / failed / cancelled): computed, not stored. Derived from which side tables have a row:
  - `running` = `job_leases.lease_owner IS NOT NULL AND lease_expires_at > now() AND no job_results row`
  - `pending` = has lease row, not running, no job_results row
  - `terminated` = has job_results row (its `status` column says which terminal state)
- **Terminal event name/data**: computed at dispatch time from `invokeid` + `job_results.status`/`.result`/`.error`/`.metadata`. Never stored.
- **The one denormalization:** `statechart_jobs.terminated_at` mirrors `job_results.completed_at`. Written in the same transaction that inserts the job_results row. Exists solely because PostgreSQL partial indexes cannot query across tables — we need `idx_jobs_active_per_invoke` to filter active rows for the idempotency check. Invariant: `statechart_jobs.terminated_at IS NULL IFF no statechart_job_results row exists for that job_id`.

### Key semantic shifts from v2

- Events are immutable facts. Claim/processing state lives in `event_claims`. Cancellation is an append-only fact in `event_cancellations`.
- Jobs are immutable definitions. All mutable coordination (lease, attempt count, retry error, result) moves to side tables.
- No column stores a derived value anywhere except `terminated_at` (explicitly denormalized, transactionally consistent).
- The v2 race-retry loop in `create-job!` is gone: dedup is one query against the partial-unique index.

## `PersistenceCoordinator` protocol

New namespace: `com.fulcrologic.statecharts.persistence.jdbc.coordinator`.

```clojure
(defprotocol PersistenceCoordinator
  "Owns the transactional cooperation between an event queue and a working-memory
   store. A coordinator's job is to keep the ACK of an in-flight event in the
   same transaction as the working memory write that processed it — so a crash
   between them is impossible.

   In v2 this cooperation leaked through env as `::sc/on-save-hooks` (a loose
   vector of callbacks) and `::sc/save-attempted?` (a shared atom). v3 replaces
   both with one typed object threaded on env as `::sc/persistence-coordinator`."

  (begin-event-processing [coord session-id event-id]
    "Open a processing window for the claimed event. Returns an opaque
     handle — caller threads it into env under `::sc/current-event-handle`
     for the duration of handler execution. A handle represents exactly
     one claim; do not reuse across events.")

  (commit-event-processing [coord handle write-fn]
    "Execute `write-fn` (a 1-arg fn taking a tx connection) inside a
     transaction that also commits the ACK for `handle`. On success both
     commit atomically. On throw both roll back; the claim stays open and
     the next claim-cycle will pick the event up again.

     `write-fn`'s return value is returned through. `write-fn` must not
     open its own transaction — it is already inside one.")

  (abort-event-processing [coord handle]
    "Release the claim without committing any WM change. Called by the
     event queue when the handler returned without attempting a save —
     e.g., the session had already terminated. Runs in its own tx.")

  (wake [coord]
    "Signal the event loop that new work is available. In-process `send!`
     calls this so intra-JVM events don't wait for the next poll. No-op
     for coordinators without a running loop.")

  (recover-stale-claims! [coord timeout-seconds]
    "Release claims older than `timeout-seconds` whose processing never
     committed. Replaces v2's standalone `recover-stale-claims!` function.
     Returns the number of claims released."))
```

### JDBC implementation

`JdbcCoordinator` is a `defrecord` holding `datasource` + `node-id` + internal wake signal. Key internal state: a per-handle flag indicating whether `commit-event-processing` was called for that handle (replacing v2's `save-attempted?` atom on env).

### No-op coordinator

`NoopCoordinator` record for tests and JDBC components instantiated without a live queue loop:
- `begin-event-processing` → sentinel handle
- `commit-event-processing` → runs `write-fn` without wrapping in a tx
- `wake`, `recover-stale-claims!` → no-ops

## Protocol & env evolution

### Protocol signatures: unchanged

`EventQueue/receive-events!`, `EventQueue/send!`, `EventQueue/cancel!`, `WorkingMemoryStore/save-working-memory!`, `WorkingMemoryStore/get-working-memory`, `WorkingMemoryStore/delete-working-memory!` — all signatures identical to v2.

### Env keys: replaced

| v2 env key                      | v3 env key                        | Notes |
|---------------------------------|-----------------------------------|-------|
| `::sc/on-save-hooks` (vector)   | *(removed)*                       | Piggy-back save-tx hooks → `commit-event-processing` |
| `::sc/save-attempted?` (atom)   | *(removed)*                       | Coordinator's internal per-handle state |
| `::wake-signal` (`BlockingQueue`) | *(removed, callers use `(wake coord)`)* | `JdbcCoordinator` owns the wake queue internally |
| *(none)*                        | `::sc/persistence-coordinator`    | Typed coordinator object |
| *(none)*                        | `::sc/current-event-handle`       | Opaque handle from `begin-event-processing` |

### Flow of one event through the system

```
┌─ JdbcEventQueue.receive-events! ───────────────────────────────────────┐
│ 1. claim N events in tx T1:                                            │
│    SELECT ... FROM statechart_events e                                 │
│      WHERE deliver_at <= now()                                         │
│        AND NOT EXISTS (SELECT 1 FROM statechart_event_claims c         │
│                          WHERE c.event_id = e.id)                      │
│        AND NOT EXISTS (SELECT 1 FROM statechart_event_cancellations x  │
│                          WHERE x.event_id = e.id)                      │
│        AND NOT EXISTS (SELECT 1 FROM statechart_event_claims c2        │
│                          JOIN statechart_events e2 ON c2.event_id=e2.id│
│                         WHERE e2.target_session_id = e.target_session_id│
│                           AND c2.processed_at IS NULL)  -- FIFO guard  │
│      ORDER BY deliver_at, id                                           │
│      LIMIT :batch-size FOR UPDATE SKIP LOCKED                          │
│    then INSERT INTO statechart_event_claims (event_id, claimed_by)     │
│    for each claimed row.                                               │
│                                                                        │
│ 2. for each claimed event e:                                           │
│    handle ← (begin-event-processing coord session-id e.id)             │
│    env'   ← env + {::sc/persistence-coordinator coord                  │
│                    ::sc/current-event-handle    handle}                │
│    (handler env' event)   ← returns when handler done                  │
│    if handler never caused save-working-memory! to fire:               │
│      (abort-event-processing coord handle)                             │
└────────────────────────────────────────────────────────────────────────┘

┌─ JdbcWorkingMemoryStore.save-working-memory! ──────────────────────────┐
│ Signature unchanged: [this env session-id wmem]                        │
│                                                                        │
│ if (::sc/current-event-handle env):                                    │
│   (commit-event-processing                                             │
│     (::sc/persistence-coordinator env)                                 │
│     (::sc/current-event-handle env)                                    │
│     (fn [tx] (upsert-session! tx session-id wmem)))                    │
│ else:                                                                  │
│   ;; no event in flight (e.g., start! path, non-coordinated caller)    │
│   (upsert-session! datasource session-id wmem)                         │
└────────────────────────────────────────────────────────────────────────┘
```

### Constructor wiring

```clojure
(defn pg-env [{:keys [datasource node-id ...] :as opts}]
  (let [coord   (coord/new-jdbc-coordinator datasource node-id)
        q       (pg-eq/new-queue coord)
        wmstore (pg-wms/new-store coord)]
    {::sc/event-queue             q
     ::sc/working-memory-store    wmstore
     ::sc/persistence-coordinator coord
     ::coord                      coord
     ...}))
```

Both record constructors take `coord` rather than `datasource` directly. The coordinator owns the datasource and node-id; queue and store own a reference to the coordinator.

### Non-JDBC implementations — no changes

`manually_polled_queue.cljc`, `core_async_event_loop.cljc`, and any in-memory working-memory store never carried v2's coordination env keys (they have no transactions to coordinate) and do not look for v3's either. A mixed configuration (e.g., in-memory queue + JDBC store): the store's `save-working-memory!` falls through to the no-handle branch and does a plain tx-less upsert.

## Event lifecycle operations (v3 shape)

- **send!** — `INSERT INTO statechart_events (...)` as in v2 (no claim/processing columns). For intra-JVM callers, follow with `(wake coord)`.
- **receive-events! / claim** — the query shown in the flow diagram above. Events are claimable iff: `deliver_at <= now()`, no existing `statechart_event_claims` row, no `statechart_event_cancellations` row, and no in-flight claim for the same `target_session_id` (per-session FIFO).
- **cancel!** — single statement replacing v2's DELETE:

  ```sql
  INSERT INTO statechart_event_cancellations (event_id, cancelled_by)
  SELECT e.id, ? FROM statechart_events e
  WHERE e.source_session_id = ? AND e.send_id = ?
    AND e.deliver_at > now()
    AND NOT EXISTS (SELECT 1 FROM statechart_event_claims c WHERE c.event_id = e.id)
  ON CONFLICT (event_id) DO NOTHING;
  ```

  Cancel-loses-race (event claimed between the `send-after` and the `cancel`) stays possible — SCXML permits this. The event delivers; an operator observing both rows sees claim-before-cancel ordering.
- **mark-processed!** — `UPDATE statechart_event_claims SET processed_at = now() WHERE event_id = ?`. Called inside `commit-event-processing`'s tx.
- **release-claim!** — `DELETE FROM statechart_event_claims WHERE event_id = ?`. Called on handler failure or `abort-event-processing`.
- **recover-stale-claims!** — `DELETE FROM statechart_event_claims WHERE processed_at IS NULL AND claimed_at < (now() - interval)`. Returns the number of claims released. Lives on the coordinator (see protocol).

## Job lifecycle operations (v3 shape)

Summary of how each operation in `job_store.clj` reshapes against the new schema:

- **create-job!** — single `INSERT ... ON CONFLICT DO NOTHING` on `statechart_jobs` (guarded by `idx_jobs_active_per_invoke` partial unique), plus `INSERT` into `statechart_job_leases` with `attempt=0, next_run_at=now(), lease_owner=NULL`. Both in one tx. No race-retry loop needed — the partial index handles dedup deterministically.
- **claim-jobs!** — `UPDATE statechart_job_leases SET attempt=attempt+1, lease_owner=?, lease_expires_at=now()+interval WHERE job_id IN (SELECT ... FOR UPDATE SKIP LOCKED WHERE lease_owner IS NULL OR lease_expires_at < now())`.
- **heartbeat!** — `UPDATE statechart_job_leases SET lease_expires_at=now()+interval WHERE job_id=? AND lease_owner=? AND NOT EXISTS (job_results row)`.
- **complete!** — in one tx: `INSERT INTO statechart_job_results (job_id, status='succeeded', result, ...)`, `UPDATE statechart_jobs SET terminated_at=now() WHERE id=?`, `DELETE FROM statechart_job_leases WHERE job_id=?`.
- **fail!** — same shape as v2: if attempts remain, `UPDATE statechart_job_leases SET attempt=..., next_run_at=..., attempt_error=?, lease_owner=NULL WHERE job_id=?`. If exhausted, `INSERT INTO statechart_job_results (status='failed', error=?, ...)` + update `terminated_at` + delete lease, one tx.
- **cancel!** — in one tx: `INSERT INTO statechart_job_results (status='cancelled', metadata=nippy{:reason :cancelled})`, update `terminated_at`, delete lease.
- **cancel-by-session!** / **cancel-by-session-in-tx!** — loop over active jobs for the session, each row cancelled as above. The `-in-tx!` variant stays for cooperation with session-deletion's outer tx.
- **claim-terminal-dispatch!** — `INSERT INTO statechart_terminal_dispatches (job_id) ON CONFLICT DO NOTHING`. Returns true if inserted (this caller won the claim), false otherwise.
- **release-terminal-dispatch-claim!** — `DELETE FROM statechart_terminal_dispatches WHERE job_id=?`. Used after a failed dispatch send.
- **get-undispatched-terminal-jobs** — `SELECT jr.* FROM statechart_job_results jr LEFT JOIN statechart_terminal_dispatches td ON jr.job_id = td.job_id WHERE td.job_id IS NULL ORDER BY jr.completed_at LIMIT ?`. The reconciler then computes the terminal event name/data from `invokeid` + status/result/error/metadata and dispatches via `send!`.

## Migration function

Namespace: `com.fulcrologic.statecharts.persistence.jdbc.migrations.v2-to-v3`.

### Operational contract

- **Workers MUST be quiesced** before calling `migrate!`. Not zero-downtime. Not dual-write-safe.
- All work runs in a single PostgreSQL transaction (DDL and DML). Partial failure rolls back to untouched v2.
- Idempotent: safe to re-run after success (no-op) or after a crash (resumes).

### Steps, in order

1. **Create v3 tables** (`CREATE TABLE IF NOT EXISTS`): `statechart_event_claims`, `statechart_event_cancellations`, `statechart_job_leases`, `statechart_job_results`, `statechart_terminal_dispatches`.
2. **Add `terminated_at` column on `statechart_jobs`** (`ALTER TABLE ADD COLUMN IF NOT EXISTS`).
3. **Populate `statechart_event_claims`** from v2 event rows where `claimed_at IS NOT NULL`:

   ```sql
   INSERT INTO statechart_event_claims (event_id, claimed_at, claimed_by, processed_at)
   SELECT id, claimed_at, COALESCE(claimed_by, 'legacy-migration'), processed_at
   FROM statechart_events WHERE claimed_at IS NOT NULL
   ON CONFLICT (event_id) DO NOTHING;
   ```

4. **Populate `statechart_job_leases`** from v2 jobs in `('pending', 'running')`:

   ```sql
   INSERT INTO statechart_job_leases
       (job_id, attempt, next_run_at, lease_owner, lease_expires_at, attempt_error, updated_at)
   SELECT id, attempt, next_run_at, lease_owner, lease_expires_at, error, updated_at
   FROM statechart_jobs WHERE status IN ('pending', 'running')
   ON CONFLICT (job_id) DO NOTHING;
   ```

5. **Populate `statechart_job_results`** from v2 jobs in `('succeeded', 'failed', 'cancelled')`. The `metadata` column carries a nippy-frozen `{:reason :cancelled}` payload for cancelled rows, produced at migration time by the Clojure migration function (SQL cannot nippy-freeze directly; rows are read into Clojure, metadata synthesised, then inserted via prepared statements):

   ```sql
   -- Pseudocode shape; actual execution is row-by-row from Clojure
   INSERT INTO statechart_job_results (job_id, status, result, error, metadata, completed_at)
   VALUES (?, ?, ?, ?, ?, ?)  -- metadata=nippy({:reason :cancelled}) when status='cancelled', else NULL
   ON CONFLICT (job_id) DO NOTHING;
   ```

6. **Backfill `statechart_jobs.terminated_at`** for terminal rows:

   ```sql
   UPDATE statechart_jobs SET terminated_at = updated_at
   WHERE status IN ('succeeded', 'failed', 'cancelled') AND terminated_at IS NULL;
   ```

7. **Populate `statechart_terminal_dispatches`** from v2 jobs where `terminal_event_dispatched_at IS NOT NULL`:

   ```sql
   INSERT INTO statechart_terminal_dispatches (job_id, dispatched_at)
   SELECT id, terminal_event_dispatched_at FROM statechart_jobs
   WHERE terminal_event_dispatched_at IS NOT NULL
   ON CONFLICT (job_id) DO NOTHING;
   ```

8. **Drop v2 columns** (`ALTER TABLE ... DROP COLUMN IF EXISTS`):
   - `statechart_events`: `claimed_at`, `claimed_by`, `processed_at`
   - `statechart_jobs`: `status`, `attempt`, `next_run_at`, `lease_owner`, `lease_expires_at`, `result`, `error`, `terminal_event_name`, `terminal_event_data`, `terminal_event_dispatched_at`, `updated_at`

9. **Drop v2 indexes, create v3 indexes** (`DROP INDEX IF EXISTS` / `CREATE INDEX IF NOT EXISTS`). See the Schema section for the full v3 index list.

### What the migration deliberately drops

`terminal_event_name` and `terminal_event_data` are **not migrated**. v3 computes them on dispatch from `invokeid` + `job_results.status`/`.result`/`.error`/`.metadata`. The first v3 reconciler pass after migration dispatches any v2 terminal jobs whose dispatch was pending.

### Guard against computation drift

The migration test (see Testing section) includes a golden-file check: for every v2 terminal job row, compute the v3 terminal event using v3 logic and compare against the v2-stored `terminal_event_name` + `terminal_event_data`. Any mismatch fails the test and blocks the migration from shipping.

### Retention functions updated

- `purge-processed-events!` — query shifts to join through `statechart_event_claims`:

  ```sql
  DELETE FROM statechart_events WHERE id IN (
    SELECT event_id FROM statechart_event_claims
    WHERE processed_at IS NOT NULL AND processed_at < ?
  );
  -- ON DELETE CASCADE cleans the claim row automatically.
  ```

- New `purge-terminated-jobs!(retention-days)`: deletes jobs where `terminated_at < cutoff AND EXISTS (terminal_dispatch)`. Cascade cleans `job_leases` (shouldn't exist by this point), `job_results`, `terminal_dispatches`.

## Behavior changes vs v2 (CHANGELOG-worthy)

- **`<cancel>` storage semantics.** v2 hard-DELETEs matching unclaimed events. v3 inserts into `statechart_event_cancellations`. Observable to operators querying `statechart_events` directly: cancelled event rows persist until retention purges them. SCXML semantics unchanged (cancelled events are not delivered either way).
- **Per-attempt retry error visibility.** Same overwrite-on-retry behavior, now in `job_leases.attempt_error` instead of `jobs.error`. Terminal failures land in `job_results.error`.
- **Terminal event data for cancelled jobs.** `{:reason :cancelled}` payload moves from the removed `terminal_event_data` column to `job_results.metadata` (nippy-frozen).
- **Job status as an observable.** v2 exposed `status` as a TEXT column. v3 derives it (pending/running/succeeded/failed/cancelled). Callers reading `status` directly must switch to the derived-status query:

  ```sql
  -- conceptual; real code is in the job_store ns
  CASE
    WHEN jr.status IS NOT NULL THEN jr.status
    WHEN jl.lease_owner IS NOT NULL AND jl.lease_expires_at > now() THEN 'running'
    ELSE 'pending'
  END
  FROM statechart_jobs j
  LEFT JOIN statechart_job_leases  jl ON jl.job_id = j.id
  LEFT JOIN statechart_job_results jr ON jr.job_id = j.id
  ```

- **`create-job!` race-retry loop removed.** Under adversarial concurrency, v2 could loop up to 5 times then throw; v3's partial-unique dedup is deterministic in one tx. Observable only in artificial race tests.

## Testing

### Migrating specs

- `core_spec.clj` — id encode/decode + query wrappers; trivial updates.
- `event_queue_spec.clj` — claim/release tests rewrite against `statechart_event_claims`; FIFO-per-session guard verified via the cross-table `EXISTS` query.
- `job_store_spec.clj` — splits into job-creation, lease-claim, results-write, and dispatch-claim subsections; each isolates one side relation.
- `chaos_test.clj` — concurrent-claim-storm intent unchanged; queries updated.
- `regression_test.clj` — every v2 regression re-asserted against v3. Tests that asserted on v2-specific columns (e.g., `terminal_event_name` existence) are removed and documented in CHANGELOG.
- `integration_test.clj` — end-to-end session lifecycle through the public API (`start!`, `send!`, …); should need minimal change.
- `id_property_test.clj`, `fixtures.clj` (already present) — kept as-is; id encoding is not touched by this spec.

### New specs

- **`coordinator_spec.clj`** — direct contract tests for `PersistenceCoordinator`:
  - `begin` / `commit` / `abort` lifecycle
  - `commit-event-processing` with a throwing `write-fn` rolls back both WM and ACK
  - `abort-event-processing` releases the claim without a WM write
  - stale-claim recovery releases old claims whose `processed_at IS NULL`
  - `wake` semantics: intra-JVM events wake the loop without poll delay
- **`migrations/v2_to_v3_spec.clj`** — seeded v2 fixtures for every row-shape combination:
  - events: `unclaimed`, `claimed-unprocessed`, `processed`
  - jobs: `pending` × `running` × `succeeded` × `failed` × `cancelled`, each × `dispatched` × `undispatched`, each × `attempt=1` × `attempt>1`
  - Assertions:
    1. Post-migration reads return equivalent data via v3 queries.
    2. Idempotency: running `migrate!` a second time is a no-op.
    3. **Golden-file check**: v3's computed terminal event name + data equals every v2 row's stored `terminal_event_name` + `terminal_event_data`. Any mismatch fails the test.
    4. Cancellations migrate correctly: v2's `terminal_event_data = {:reason :cancelled}` becomes `job_results.metadata = {:reason :cancelled}`.

## File layout

### New files

```
src/main/com/fulcrologic/statecharts/persistence/jdbc/coordinator.clj
src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj
src/test/com/fulcrologic/statecharts/persistence/jdbc/coordinator_spec.clj
src/test/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3_spec.clj
```

### Rewritten

```
src/main/com/fulcrologic/statecharts/persistence/jdbc.clj             (pg-env wires coord)
src/main/com/fulcrologic/statecharts/persistence/jdbc/schema.clj       (v3 DDL)
src/main/com/fulcrologic/statecharts/persistence/jdbc/event_queue.clj  (coord-based)
src/main/com/fulcrologic/statecharts/persistence/jdbc/working_memory_store.clj
src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj
```

### Unchanged

```
src/main/com/fulcrologic/statecharts/persistence/jdbc/core.clj         (id encoding, query utils)
src/main/com/fulcrologic/statecharts/event_queue/*                      (non-JDBC queues)
src/main/com/fulcrologic/statecharts/working_memory_store/*             (non-JDBC stores)
src/main/com/fulcrologic/statecharts/protocols.cljc                    (no signature changes)
src/main/com/fulcrologic/statecharts/algorithms/v20150901_impl.cljc    (processor untouched)
```

## Release

- Dedicated branch: `v3-persistence`.
- Cuts tag `3.0.0`.
- CHANGELOG documents every behavior change from the "Behavior changes vs v2" section under a **Breaking changes** heading.
- Migration instructions (quiesce → `migrate!` → redeploy) documented in `src/main/com/fulcrologic/statecharts/persistence/jdbc/README.md` with a worked example.
