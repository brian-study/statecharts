# Persistence v3 Coordinator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement v3 of the JDBC persistence layer: append-only event/job facts with coordination state in side relations, a `PersistenceCoordinator` protocol replacing env-threaded hooks, and an idempotent v2→v3 migration function.

**Architecture:** Five new side-relation tables (`event_claims`, `event_cancellations`, `job_leases`, `job_results`, `terminal_dispatches`) + one denormalized `terminated_at` column on `statechart_jobs`. New `PersistenceCoordinator` protocol owns atomic ACK-on-save semantics. Non-JDBC backends unchanged. Dedicated `v3-persistence` branch cutting `3.0.0`.

**Tech Stack:** Clojure 1.12, next.jdbc 1.3, HoneySQL 2.7, PostgreSQL 15+, nippy 3.4, fulcro-spec 3.2, kaocha 1.91, HikariCP 7.

**Reference spec:** `docs/ai/specs/persistence-v3-coordinator.md`

---

## Operating conventions (read first)

- **Unit tests** (no DB): `com.fulcrologic.statecharts.persistence.jdbc.*-spec` using `fulcro-spec` (`specification`, `component`, `behavior`, `assertions`, `=>`). Run with `clojure -M:clj-tests unit`.
- **Integration tests** (need live PG): `^:integration` metadata, `deftest`/`is`/`testing` style, use fixtures from `com.fulcrologic.statecharts.persistence.jdbc.fixtures`. Run with `clojure -M:clj-tests integration`.
- **Guardrails** are on by default in tests; `:nrepl` alias adds `-Dguardrails.enabled`. Tests that need it can set `:jvm-opts`.
- **DB for integration tests**: env vars `PG_TEST_HOST`/`PG_TEST_PORT`/`PG_TEST_DATABASE`/`PG_TEST_USER`/`PG_TEST_PASSWORD`; defaults `localhost:5432/statecharts_test` user `postgres`. Requires a running PostgreSQL; `createdb statecharts_test` first.
- **Commit cadence**: one commit per task. Use the commit messages shown — they key off the phase/task number for bisect clarity.
- **Never use `--amend`** on pushed commits. Never `--no-verify`. Pre-commit hooks in this repo run guardrails; let them run.
- **The new branch is `v3-persistence`**; cut from `main`, not from the current working branch.

---

## Phase 0: Branch setup

### Task 0.1: Create v3-persistence branch

**Files:** none (git operation only)

- [ ] **Step 1: Ensure main is up to date**

```bash
git fetch origin
git checkout main
git pull origin main
```

- [ ] **Step 2: Cut the v3 branch**

```bash
git checkout -b v3-persistence
```

- [ ] **Step 3: Confirm starting point**

```bash
git status
git log --oneline -3
```

Expected: clean working tree; HEAD matches `origin/main`.

---

## Phase 1: Schema v3

This phase rewrites `schema.clj` end-to-end. The v2 tables will be dropped and recreated in the new shape. No migration function yet — that's Phase 7.

We update the tests for `create-tables!` / `drop-tables!` / `truncate-tables!` in the same phase because they assert on table existence and structure.

### Task 1.1: Rewrite schema.clj for v3 table shapes

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/schema.clj`
- Test: `src/test/com/fulcrologic/statecharts/persistence/jdbc/integration_test.clj` (smoke test only — detailed schema tests land in Task 1.2)

- [ ] **Step 1: Replace `schema.clj` with v3 DDL**

Overwrite `src/main/com/fulcrologic/statecharts/persistence/jdbc/schema.clj`:

```clojure
(ns com.fulcrologic.statecharts.persistence.jdbc.schema
  "Database schema DDL for statechart persistence (v3).

   v3 schema separates coordination state from domain state:
   - statechart_events are append-only facts; claim/processing state lives in
     statechart_event_claims; cancellation is a fact in statechart_event_cancellations
   - statechart_jobs are append-only definitions; coordination state splits into
     statechart_job_leases (in-flight/retry), statechart_job_results (terminal),
     statechart_terminal_dispatches (dispatch claim)

   statechart_sessions is unchanged from v2 (working-memory decomposition is
   a separate future thread)."
  (:require
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]))

(def ^:private sessions-ddl
  "CREATE TABLE IF NOT EXISTS statechart_sessions (
    session_id     TEXT PRIMARY KEY,
    statechart_src TEXT NOT NULL,
    working_memory BYTEA NOT NULL,
    version        BIGINT NOT NULL DEFAULT 1,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
  )")

(def ^:private sessions-indexes-ddl
  ["CREATE INDEX IF NOT EXISTS idx_sessions_statechart_src ON statechart_sessions(statechart_src)"
   "CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON statechart_sessions(updated_at)"])

(def ^:private events-ddl
  "CREATE TABLE IF NOT EXISTS statechart_events (
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
  )")

(def ^:private events-indexes-ddl
  ["CREATE INDEX IF NOT EXISTS idx_events_deliver_at ON statechart_events(deliver_at, id)"
   "CREATE INDEX IF NOT EXISTS idx_events_cancel     ON statechart_events(source_session_id, send_id, deliver_at)"
   "CREATE INDEX IF NOT EXISTS idx_events_target     ON statechart_events(target_session_id, deliver_at)"])

(def ^:private event-claims-ddl
  "CREATE TABLE IF NOT EXISTS statechart_event_claims (
    event_id     BIGINT PRIMARY KEY REFERENCES statechart_events(id) ON DELETE CASCADE,
    claimed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_by   TEXT NOT NULL,
    processed_at TIMESTAMPTZ
  )")

(def ^:private event-claims-indexes-ddl
  ["CREATE INDEX IF NOT EXISTS idx_event_claims_recovery
      ON statechart_event_claims(claimed_at)
      WHERE processed_at IS NULL"])

(def ^:private event-cancellations-ddl
  "CREATE TABLE IF NOT EXISTS statechart_event_cancellations (
    event_id     BIGINT PRIMARY KEY REFERENCES statechart_events(id) ON DELETE CASCADE,
    cancelled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    cancelled_by TEXT
  )")

(def ^:private jobs-ddl
  "CREATE TABLE IF NOT EXISTS statechart_jobs (
    id            UUID PRIMARY KEY,
    session_id    TEXT NOT NULL,
    invokeid      TEXT NOT NULL,
    job_type      TEXT NOT NULL,
    payload       BYTEA NOT NULL,
    max_attempts  INT NOT NULL DEFAULT 3,
    terminated_at TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
  )")

(def ^:private jobs-indexes-ddl
  ["CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_per_invoke
      ON statechart_jobs(session_id, invokeid)
      WHERE terminated_at IS NULL"
   "CREATE INDEX IF NOT EXISTS idx_jobs_session ON statechart_jobs(session_id, created_at DESC)"])

(def ^:private job-leases-ddl
  "CREATE TABLE IF NOT EXISTS statechart_job_leases (
    job_id           UUID PRIMARY KEY REFERENCES statechart_jobs(id) ON DELETE CASCADE,
    attempt          INT NOT NULL DEFAULT 0,
    next_run_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempt_error    BYTEA,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
  )")

(def ^:private job-leases-indexes-ddl
  ["CREATE INDEX IF NOT EXISTS idx_leases_claimable
      ON statechart_job_leases(next_run_at, lease_expires_at)"])

(def ^:private job-results-ddl
  "CREATE TABLE IF NOT EXISTS statechart_job_results (
    job_id       UUID PRIMARY KEY REFERENCES statechart_jobs(id) ON DELETE CASCADE,
    status       TEXT NOT NULL CHECK (status IN ('succeeded', 'failed', 'cancelled')),
    result       BYTEA,
    error        BYTEA,
    metadata     BYTEA,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )")

(def ^:private job-results-indexes-ddl
  ["CREATE INDEX IF NOT EXISTS idx_results_completed_at ON statechart_job_results(completed_at)"])

(def ^:private terminal-dispatches-ddl
  "CREATE TABLE IF NOT EXISTS statechart_terminal_dispatches (
    job_id        UUID PRIMARY KEY REFERENCES statechart_jobs(id) ON DELETE CASCADE,
    dispatched_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )")

(def ^:private drop-ddls
  ;; Drop order matters: children (FK referrers) before parents.
  ["DROP TABLE IF EXISTS statechart_terminal_dispatches CASCADE"
   "DROP TABLE IF EXISTS statechart_job_results         CASCADE"
   "DROP TABLE IF EXISTS statechart_job_leases          CASCADE"
   "DROP TABLE IF EXISTS statechart_jobs                CASCADE"
   "DROP TABLE IF EXISTS statechart_event_cancellations CASCADE"
   "DROP TABLE IF EXISTS statechart_event_claims        CASCADE"
   "DROP TABLE IF EXISTS statechart_events              CASCADE"
   "DROP TABLE IF EXISTS statechart_sessions            CASCADE"
   ;; Legacy tables from pre-3.0 deployments; dropped defensively here.
   "DROP TABLE IF EXISTS statechart_definitions         CASCADE"])

(defn create-tables!
  "Create all v3 persistence tables and indexes.
   Safe to call multiple times (uses IF NOT EXISTS)."
  [ds-or-conn]
  (doseq [ddl [sessions-ddl events-ddl event-claims-ddl event-cancellations-ddl
               jobs-ddl job-leases-ddl job-results-ddl terminal-dispatches-ddl]]
    (core/execute-sql! ds-or-conn ddl))
  (doseq [idx (concat sessions-indexes-ddl
                      events-indexes-ddl
                      event-claims-indexes-ddl
                      jobs-indexes-ddl
                      job-leases-indexes-ddl
                      job-results-indexes-ddl)]
    (core/execute-sql! ds-or-conn idx))
  true)

(defn drop-tables!
  "Drop all v3 persistence tables.
   WARNING: This will delete all data! Also drops the legacy
   `statechart_definitions` table from pre-2.0.10 deployments."
  [ds-or-conn]
  (doseq [ddl drop-ddls]
    (core/execute-sql! ds-or-conn ddl))
  true)

(defn truncate-tables!
  "Truncate all v3 persistence tables. Removes all data but keeps structure."
  [ds-or-conn]
  (core/execute-sql! ds-or-conn
    "TRUNCATE statechart_terminal_dispatches,
              statechart_job_results,
              statechart_job_leases,
              statechart_jobs,
              statechart_event_cancellations,
              statechart_event_claims,
              statechart_events,
              statechart_sessions
     RESTART IDENTITY CASCADE")
  true)
```

- [ ] **Step 2: Smoke-test create-tables! / drop-tables! still work against a live DB**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.integration-test`

Expected: Most tests will FAIL because they exercise the still-v2-shaped event_queue / working_memory_store / job_store code against v3 tables. That's expected — the rest of the phases make them pass. What must succeed: DB connection, `create-tables!` and `truncate-tables!` DO NOT THROW.

- [ ] **Step 3: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/schema.clj
git commit -m "schema(persistence): v3 DDL with side-relation coordination tables"
```

### Task 1.2: Schema structure specification

**Files:**
- Test: `src/test/com/fulcrologic/statecharts/persistence/jdbc/schema_spec.clj` (new)

This spec is a pinning test: it introspects the live PG schema and asserts the v3 shape is what we declared. It replaces `schema.clj`'s correctness with an independently-verifiable fact.

- [ ] **Step 1: Create the schema spec**

Write `src/test/com/fulcrologic/statecharts/persistence/jdbc/schema_spec.clj`:

```clojure
(ns ^:integration com.fulcrologic.statecharts.persistence.jdbc.schema-spec
  "Pinning tests: v3 schema structure against a live PG database."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.fixtures :as fixtures :refer [*pool*]]
   [com.fulcrologic.statecharts.persistence.jdbc.schema :as schema]))

(use-fixtures :once fixtures/with-pool)

(defn- table-names [ds]
  (->> (core/execute-sql! ds
         (str "SELECT tablename FROM pg_tables"
              " WHERE schemaname = 'public' AND tablename LIKE 'statechart_%'"))
       (map :tablename)
       set))

(defn- column-names [ds table]
  (->> (core/execute-sql! ds
         (str "SELECT column_name FROM information_schema.columns"
              " WHERE table_schema = 'public' AND table_name = ?")
         [table])
       (map :column-name)
       set))

(defn- index-names [ds table]
  (->> (core/execute-sql! ds
         (str "SELECT indexname FROM pg_indexes"
              " WHERE schemaname = 'public' AND tablename = ?")
         [table])
       (map :indexname)
       set))

(deftest ^:integration v3-tables-exist-test
  (testing "after create-tables!, all v3 tables exist"
    (schema/drop-tables! *pool*)
    (schema/create-tables! *pool*)
    (is (= #{"statechart_sessions"
             "statechart_events"
             "statechart_event_claims"
             "statechart_event_cancellations"
             "statechart_jobs"
             "statechart_job_leases"
             "statechart_job_results"
             "statechart_terminal_dispatches"}
           (table-names *pool*)))))

(deftest ^:integration events-table-has-no-coordination-columns-test
  (testing "statechart_events carries only domain columns (coordination moved to side tables)"
    (schema/drop-tables! *pool*)
    (schema/create-tables! *pool*)
    (let [cols (column-names *pool* "statechart_events")]
      (is (contains? cols "id"))
      (is (contains? cols "target_session_id"))
      (is (contains? cols "deliver_at"))
      (is (not (contains? cols "claimed_at")))
      (is (not (contains? cols "claimed_by")))
      (is (not (contains? cols "processed_at"))))))

(deftest ^:integration jobs-table-shape-test
  (testing "statechart_jobs carries only domain columns plus the terminated_at denormalization"
    (schema/drop-tables! *pool*)
    (schema/create-tables! *pool*)
    (let [cols (column-names *pool* "statechart_jobs")]
      (is (contains? cols "id"))
      (is (contains? cols "session_id"))
      (is (contains? cols "invokeid"))
      (is (contains? cols "terminated_at"))
      (is (not (contains? cols "status")))
      (is (not (contains? cols "attempt")))
      (is (not (contains? cols "lease_owner")))
      (is (not (contains? cols "result")))
      (is (not (contains? cols "error")))
      (is (not (contains? cols "terminal_event_name")))
      (is (not (contains? cols "terminal_event_data")))
      (is (not (contains? cols "terminal_event_dispatched_at"))))))

(deftest ^:integration idx-jobs-active-per-invoke-test
  (testing "partial unique index on active (non-terminated) jobs exists"
    (schema/drop-tables! *pool*)
    (schema/create-tables! *pool*)
    (is (contains? (index-names *pool* "statechart_jobs")
                   "idx_jobs_active_per_invoke"))))

(deftest ^:integration truncate-tables-clears-all-rows-test
  (testing "truncate removes all data from all v3 tables without errors"
    (schema/create-tables! *pool*)
    (core/execute-sql! *pool*
      "INSERT INTO statechart_sessions (session_id, statechart_src, working_memory) VALUES ('x', 's', '\\x00')")
    (schema/truncate-tables! *pool*)
    (is (zero? (count (core/execute-sql! *pool* "SELECT session_id FROM statechart_sessions"))))))
```

- [ ] **Step 2: Run and verify**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.schema-spec`

Expected: all tests pass. If a DB connection fails, `createdb statecharts_test` first and retry.

- [ ] **Step 3: Commit**

```bash
git add src/test/com/fulcrologic/statecharts/persistence/jdbc/schema_spec.clj
git commit -m "test(persistence): v3 schema structure pinning tests"
```

---

## Phase 2: `PersistenceCoordinator` protocol

### Task 2.1: Define the protocol

**Files:**
- Create: `src/main/com/fulcrologic/statecharts/persistence/jdbc/coordinator.clj`

- [ ] **Step 1: Create the protocol namespace**

Write `src/main/com/fulcrologic/statecharts/persistence/jdbc/coordinator.clj`:

```clojure
(ns com.fulcrologic.statecharts.persistence.jdbc.coordinator
  "PersistenceCoordinator protocol and implementations.

   A coordinator owns the transactional cooperation between an event queue and a
   working-memory store: keeps the ACK of an in-flight event in the same
   transaction as the WM write that processed it, so a crash between them is
   impossible.

   In v2 this cooperation leaked through env as `::sc/on-save-hooks` (a vector
   of callbacks) and `::sc/save-attempted?` (a shared atom). v3 replaces both
   with one typed object threaded on env as `::sc/persistence-coordinator`.")

(defprotocol PersistenceCoordinator
  (begin-event-processing [coord session-id event-id]
    "Open a processing window for the claimed event. Returns an opaque handle
     the caller threads into env under `::sc/current-event-handle` for the
     duration of handler execution. One handle per claim; do not reuse.")

  (commit-event-processing [coord handle write-fn]
    "Execute `write-fn` (1-arg fn taking a tx connection) inside a transaction
     that also commits the ACK for `handle`. On success both commit atomically.
     On throw both roll back; the claim stays open for re-delivery.
     `write-fn`'s return value is returned through. `write-fn` must NOT open
     its own transaction — it is already inside one.")

  (abort-event-processing [coord handle]
    "Release the claim without committing any WM change. Called when the handler
     returned without attempting a save — e.g., session already terminated.
     Runs in its own tx.")

  (handle-committed? [coord handle]
    "Return true if `commit-event-processing` was called and committed for this
     handle. Used by the event queue to decide whether a fallback ACK is safe.
     Replaces v2's `::sc/save-attempted?` atom.")

  (wake [coord]
    "Signal the event loop that new work is available. In-process `send!` calls
     this so intra-JVM events don't wait for the next poll. No-op if no loop.")

  (recover-stale-claims! [coord timeout-seconds]
    "Release claims older than `timeout-seconds` whose processing never
     committed. Returns the number of claims released."))
```

- [ ] **Step 2: Verify the namespace compiles**

Run: `clojure -M:clj-tests unit --focus com.fulcrologic.statecharts.persistence.jdbc`

Expected: no compile errors (the spec itself has no tests yet; just verifying the ns loads).

- [ ] **Step 3: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/coordinator.clj
git commit -m "feat(persistence): PersistenceCoordinator protocol"
```

### Task 2.2: NoopCoordinator implementation

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/coordinator.clj`
- Test: `src/test/com/fulcrologic/statecharts/persistence/jdbc/coordinator_spec.clj` (new)

- [ ] **Step 1: Write the failing spec for NoopCoordinator**

Write `src/test/com/fulcrologic/statecharts/persistence/jdbc/coordinator_spec.clj`:

```clojure
(ns com.fulcrologic.statecharts.persistence.jdbc.coordinator-spec
  "Unit tests for PersistenceCoordinator implementations that don't require a DB."
  (:require
   [com.fulcrologic.statecharts.persistence.jdbc.coordinator :as sut]
   [fulcro-spec.core :refer [=> assertions behavior component specification]]))

(specification "NoopCoordinator"
  (component "begin-event-processing"
    (behavior "returns an opaque handle"
      (let [c (sut/new-noop-coordinator)
            h (sut/begin-event-processing c :session-1 42)]
        (assertions
          "handle is non-nil"
          (some? h) => true))))

  (component "commit-event-processing"
    (behavior "runs write-fn and records the commit"
      (let [c        (sut/new-noop-coordinator)
            h        (sut/begin-event-processing c :s 1)
            captured (atom nil)
            result   (sut/commit-event-processing
                       c h
                       (fn [tx] (reset! captured tx) ::wrote))]
        (assertions
          "write-fn called with the sentinel tx"
          @captured => ::noop-tx
          "return value is write-fn's return"
          result => ::wrote
          "handle is now marked committed"
          (sut/handle-committed? c h) => true)))

    (behavior "throwing write-fn does NOT mark the handle committed"
      (let [c (sut/new-noop-coordinator)
            h (sut/begin-event-processing c :s 1)]
        (try
          (sut/commit-event-processing c h (fn [_] (throw (ex-info "nope" {}))))
          (catch Exception _))
        (assertions
          "handle remains uncommitted after throw"
          (sut/handle-committed? c h) => false))))

  (component "abort-event-processing"
    (behavior "is a no-op that returns nil"
      (let [c (sut/new-noop-coordinator)
            h (sut/begin-event-processing c :s 1)]
        (assertions
          "returns nil"
          (sut/abort-event-processing c h) => nil
          "handle remains uncommitted"
          (sut/handle-committed? c h) => false))))

  (component "wake"
    (behavior "is a no-op that returns nil"
      (let [c (sut/new-noop-coordinator)]
        (assertions
          (sut/wake c) => nil))))

  (component "recover-stale-claims!"
    (behavior "returns 0 (nothing to recover in-memory)"
      (let [c (sut/new-noop-coordinator)]
        (assertions
          (sut/recover-stale-claims! c 30) => 0)))))
```

- [ ] **Step 2: Run to verify it fails**

Run: `clojure -M:clj-tests unit --focus com.fulcrologic.statecharts.persistence.jdbc.coordinator-spec`

Expected: FAIL — `new-noop-coordinator` is not defined.

- [ ] **Step 3: Implement NoopCoordinator**

Append to `src/main/com/fulcrologic/statecharts/persistence/jdbc/coordinator.clj`:

```clojure
;; -----------------------------------------------------------------------------
;; No-op coordinator
;; -----------------------------------------------------------------------------
;;
;; Used for tests and for code paths that want the coordinator shape without
;; real transactions (in-memory event queues, mock event loops, etc.).

(defrecord NoopCoordinator [committed-handles]
  PersistenceCoordinator
  (begin-event-processing [_ session-id event-id]
    ;; Handle is a unique object; the record tracks which handles saw a
    ;; commit via the committed-handles atom. Use a plain map as the handle
    ;; so callers can see the identity in logs.
    {:session-id session-id :event-id event-id :id (random-uuid)})

  (commit-event-processing [_ handle write-fn]
    (let [result (write-fn ::noop-tx)]
      (swap! committed-handles conj (:id handle))
      result))

  (abort-event-processing [_ _handle] nil)

  (handle-committed? [_ handle]
    (contains? @committed-handles (:id handle)))

  (wake [_] nil)

  (recover-stale-claims! [_ _timeout-seconds] 0))

(defn new-noop-coordinator
  "Create a no-op coordinator. `commit-event-processing` calls `write-fn` with
   the sentinel `::noop-tx` value."
  []
  (->NoopCoordinator (atom #{})))
```

- [ ] **Step 4: Run to verify it passes**

Run: `clojure -M:clj-tests unit --focus com.fulcrologic.statecharts.persistence.jdbc.coordinator-spec`

Expected: all assertions pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/coordinator.clj \
        src/test/com/fulcrologic/statecharts/persistence/jdbc/coordinator_spec.clj
git commit -m "feat(persistence): NoopCoordinator + spec"
```

### Task 2.3: JdbcCoordinator implementation

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/coordinator.clj`
- Test: `src/test/com/fulcrologic/statecharts/persistence/jdbc/coordinator_spec.clj`

- [ ] **Step 1: Create `jdbc_coordinator_integration_spec.clj`**

JDBC integration tests live in a separate ns so `deftest`/`^:integration` style doesn't mix with the fulcro-spec `specification` style of Task 2.2. Both ns's test the same protocol — the split is purely a testing-style convention.

Write `src/test/com/fulcrologic/statecharts/persistence/jdbc/jdbc_coordinator_integration_spec.clj`:

```clojure
(ns ^:integration com.fulcrologic.statecharts.persistence.jdbc.jdbc-coordinator-integration-spec
  "Integration tests for JdbcCoordinator. Require a running PG."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts.persistence.jdbc.coordinator :as coord]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.fixtures :as fixtures :refer [*pool*]]))

(use-fixtures :once fixtures/with-pool)
(use-fixtures :each fixtures/with-clean-tables)

(defn- seed-event!
  "Insert a single statechart_events row and return its id."
  [pool target]
  (:id (core/execute-sql-one! pool
         (str "INSERT INTO statechart_events (target_session_id, event_name)"
              " VALUES (?, ?) RETURNING id")
         [(pr-str target) ":ping"])))

(defn- seed-claim!
  "Insert a claim row for event-id under claimed-by."
  [pool event-id claimed-by]
  (core/execute-sql! pool
    (str "INSERT INTO statechart_event_claims (event_id, claimed_by) VALUES (?, ?)")
    [event-id claimed-by]))

(defn- claim-row [pool event-id]
  (core/execute-sql-one! pool
    "SELECT * FROM statechart_event_claims WHERE event_id = ?" [event-id]))

(deftest ^:integration begin-returns-handle-carrying-session-and-event-test
  (let [c      (coord/new-jdbc-coordinator *pool* "node-a")
        eid    (seed-event! *pool* :target-session)
        _      (seed-claim! *pool* eid "node-a")
        h      (coord/begin-event-processing c :target-session eid)]
    (is (some? h))
    (is (= :target-session (:session-id h)))
    (is (= eid             (:event-id h)))))

(deftest ^:integration commit-marks-claim-processed-and-runs-write-fn-test
  (let [c      (coord/new-jdbc-coordinator *pool* "node-a")
        eid    (seed-event! *pool* :target-session)
        _      (seed-claim! *pool* eid "node-a")
        h      (coord/begin-event-processing c :target-session eid)
        marker (atom nil)]
    (coord/commit-event-processing c h
      (fn [tx]
        (reset! marker :write-fn-ran)
        (core/execute-sql! tx
          "INSERT INTO statechart_sessions (session_id, statechart_src, working_memory) VALUES (?, ?, ?)"
          ["\"s-1\"" "::chart" (core/freeze {:ok true})])))
    (is (= :write-fn-ran @marker))
    (is (some? (:processed-at (claim-row *pool* eid))))
    (is (coord/handle-committed? c h))))

(deftest ^:integration commit-rollback-leaves-claim-unprocessed-test
  (let [c   (coord/new-jdbc-coordinator *pool* "node-a")
        eid (seed-event! *pool* :target-session)
        _   (seed-claim! *pool* eid "node-a")
        h   (coord/begin-event-processing c :target-session eid)]
    (is (thrown-with-msg? Exception #"intentional"
          (coord/commit-event-processing c h
            (fn [tx]
              (core/execute-sql! tx
                "INSERT INTO statechart_sessions (session_id, statechart_src, working_memory) VALUES (?, ?, ?)"
                ["\"s-2\"" "::chart" (core/freeze {:ok true})])
              (throw (ex-info "intentional" {}))))))
    (testing "claim is still unprocessed (rolled back with the throwing tx)"
      (is (nil? (:processed-at (claim-row *pool* eid)))))
    (testing "the WM insert rolled back too (no s-2 row)"
      (is (nil? (core/execute-sql-one! *pool*
                  "SELECT 1 FROM statechart_sessions WHERE session_id = '\"s-2\"'"))))
    (testing "handle not marked committed"
      (is (not (coord/handle-committed? c h))))))

(deftest ^:integration abort-releases-claim-via-delete-test
  (let [c   (coord/new-jdbc-coordinator *pool* "node-a")
        eid (seed-event! *pool* :target-session)
        _   (seed-claim! *pool* eid "node-a")
        h   (coord/begin-event-processing c :target-session eid)]
    (coord/abort-event-processing c h)
    (is (nil? (claim-row *pool* eid)))))

(deftest ^:integration recover-stale-claims-releases-old-unprocessed-test
  (let [c      (coord/new-jdbc-coordinator *pool* "node-a")
        fresh  (seed-event! *pool* :s-fresh)
        stale  (seed-event! *pool* :s-stale)
        other  (seed-event! *pool* :s-other)]
    (core/execute-sql! *pool*
      (str "INSERT INTO statechart_event_claims (event_id, claimed_at, claimed_by)"
           " VALUES (?, now(), 'node-a'),"
           "        (?, now() - interval '90 seconds', 'node-b'),"
           "        (?, now() - interval '90 seconds', 'node-c')")
      [fresh stale other])
    ;; Mark `other` processed so recover leaves it alone.
    (core/execute-sql! *pool*
      "UPDATE statechart_event_claims SET processed_at = now() WHERE event_id = ?" [other])
    (is (= 1 (coord/recover-stale-claims! c 60)))
    (testing "fresh claim retained"
      (is (some? (claim-row *pool* fresh))))
    (testing "processed claim retained"
      (is (some? (claim-row *pool* other))))
    (testing "stale unprocessed claim released"
      (is (nil? (claim-row *pool* stale))))))

(deftest ^:integration wake-accepts-a-signal-without-throwing-test
  (let [c (coord/new-jdbc-coordinator *pool* "node-a")]
    (coord/wake c)
    (is true "completed without exception")))
```

- [ ] **Step 2: Run to verify all fail**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.jdbc-coordinator-integration-spec`

Expected: all fail with `new-jdbc-coordinator` undefined.

- [ ] **Step 3: Implement JdbcCoordinator**

Append to `src/main/com/fulcrologic/statecharts/persistence/jdbc/coordinator.clj`:

```clojure
;; -----------------------------------------------------------------------------
;; JDBC coordinator
;; -----------------------------------------------------------------------------

(defrecord JdbcCoordinator [datasource node-id wake-signal committed-handles]
  PersistenceCoordinator
  (begin-event-processing [_ session-id event-id]
    {:session-id session-id
     :event-id   event-id
     :id         (random-uuid)})

  (commit-event-processing [_ handle write-fn]
    (let [event-id (:event-id handle)
          result   (atom nil)]
      (core/with-tx [tx datasource]
        (reset! result (write-fn tx))
        (core/execute-sql! tx
          "UPDATE statechart_event_claims SET processed_at = now() WHERE event_id = ?"
          [event-id]))
      ;; Only mark committed if the tx above succeeded (we didn't throw out
      ;; before this line).
      (swap! committed-handles conj (:id handle))
      @result))

  (abort-event-processing [_ handle]
    (core/execute-sql! datasource
      "DELETE FROM statechart_event_claims WHERE event_id = ?"
      [(:event-id handle)])
    nil)

  (handle-committed? [_ handle]
    (contains? @committed-handles (:id handle)))

  (wake [_]
    (when wake-signal
      (.offer ^java.util.concurrent.BlockingQueue wake-signal :wake))
    nil)

  (recover-stale-claims! [_ timeout-seconds]
    (let [timeout-seconds (long timeout-seconds)
          result (core/execute-sql! datasource
                   (str "DELETE FROM statechart_event_claims"
                        " WHERE processed_at IS NULL"
                        "   AND claimed_at < (now() - interval '" timeout-seconds " seconds')"))]
      (core/affected-row-count result))))

(defn new-jdbc-coordinator
  "Create a JDBC coordinator.
   `datasource` — javax.sql.DataSource.
   `node-id`    — unique string identifying this worker.
   `:wake-signal` — optional `java.util.concurrent.BlockingQueue`; the event
                   loop installs one so `wake` can signal it."
  ([datasource node-id]
   (new-jdbc-coordinator datasource node-id nil))
  ([datasource node-id wake-signal]
   (require '[com.fulcrologic.statecharts.persistence.jdbc.core])
   (->JdbcCoordinator datasource node-id wake-signal (atom #{}))))

(defn attach-wake-signal
  "Return a new JdbcCoordinator sharing this one's datasource/node-id/committed
   handles, with a fresh wake-signal installed. Used by `start-event-loop!` so
   `(wake coord)` signals the loop's LinkedBlockingQueue."
  [^JdbcCoordinator coord wake-signal]
  (assoc coord :wake-signal wake-signal))
```

You also need to add the `core` require at the top of the ns. Update the ns form:

```clojure
(ns com.fulcrologic.statecharts.persistence.jdbc.coordinator
  "..." ; keep the existing docstring
  (:require
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]))
```

And drop the defensive `(require ...)` call from `new-jdbc-coordinator` (it's redundant with the static require).

- [ ] **Step 4: Run to verify all pass**

Run:
- Unit: `clojure -M:clj-tests unit --focus com.fulcrologic.statecharts.persistence.jdbc.coordinator-spec`
- Integration: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.jdbc-coordinator-integration-spec`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/coordinator.clj \
        src/test/com/fulcrologic/statecharts/persistence/jdbc/jdbc_coordinator_integration_spec.clj
git commit -m "feat(persistence): JdbcCoordinator + integration tests"
```

---

## Phase 3: Rewrite `event_queue.clj` for v3

### Task 3.1: Rewrite `send!` and `cancel!` for v3 schema

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/event_queue.clj`

- [ ] **Step 1: Replace the file with the v3 shape**

Overwrite `src/main/com/fulcrologic/statecharts/persistence/jdbc/event_queue.clj`:

```clojure
(ns com.fulcrologic.statecharts.persistence.jdbc.event-queue
  "PostgreSQL-backed event queue (v3).

   Events are immutable facts in `statechart_events`. Claims live in
   `statechart_event_claims`. Cancellations live in
   `statechart_event_cancellations`. All coordination goes through the
   `PersistenceCoordinator`."
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.events :as evts]
   [com.fulcrologic.statecharts.persistence.jdbc.coordinator :as coord]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.protocols :as sp]
   [promesa.core :as p]
   [taoensso.timbre :as log])
  (:import
   [java.time OffsetDateTime Duration]))

;; -----------------------------------------------------------------------------
;; Event type support
;; -----------------------------------------------------------------------------

(defn- supported-type?
  [type]
  (or (nil? type)
      (and (string? type)
           (str/starts-with? (str/lower-case type) "http://www.w3.org/tr/scxml"))
      (= type ::sc/chart)
      (= type :statechart)))

;; -----------------------------------------------------------------------------
;; Row <-> event conversion
;; -----------------------------------------------------------------------------

(defn- parse-event-type [s]
  (when s
    (cond
      (str/starts-with? s ":")  (try (edn/read-string s) (catch Exception _ s))
      (str/starts-with? s "\"") (try (edn/read-string s) (catch Exception _ s))
      :else                     (keyword s))))

(defn- parse-invoke-id [s]
  (core/decode-id s {:legacy-fallback :keyword}))

(defn- event->row
  [{:keys [event data type target source-session-id send-id invoke-id delay]}]
  (let [now        (OffsetDateTime/now)
        deliver-at (if delay (.plus now (Duration/ofMillis delay)) now)]
    {:target-session-id (core/session-id->str (or target source-session-id))
     :source-session-id (when source-session-id (core/session-id->str source-session-id))
     :send-id           send-id
     :invoke-id         (when invoke-id (pr-str invoke-id))
     :event-name        (pr-str event)
     :event-type        (pr-str (or type :external))
     :event-data        (core/freeze (or data {}))
     :deliver-at        deliver-at}))

(defn- row->event [row]
  (let [event-name (edn/read-string (:event-name row))
        data       (core/thaw (:event-data row))]
    (evts/new-event
      (cond-> {:name   event-name
               :type   (parse-event-type (:event-type row))
               :target (core/str->session-id (:target-session-id row))
               :data   (or data {})}
        (:source-session-id row)
        (assoc ::sc/source-session-id (core/str->session-id (:source-session-id row)))
        (:send-id row)
        (assoc :sendid (:send-id row) ::sc/send-id (:send-id row))
        (:invoke-id row)
        (assoc :invokeid (parse-invoke-id (:invoke-id row)))))))

;; -----------------------------------------------------------------------------
;; Insert / cancel
;; -----------------------------------------------------------------------------

(defn- insert-event!
  [ds send-request]
  (core/execute! ds
    {:insert-into :statechart-events
     :values      [(event->row send-request)]})
  true)

(defn- cancel-events!
  "Cancel pending delayed events matching (source-session-id, send-id) by
   inserting into statechart_event_cancellations. Does NOT delete the event
   row — events are append-only in v3.

   SCXML §6.3 requires only that the event not be delivered; storage policy
   is ours. The claim query excludes events with a cancellation row."
  [ds session-id send-id cancelled-by]
  (let [sid-str (core/session-id->str session-id)
        result (core/execute-sql! ds
                 (str "INSERT INTO statechart_event_cancellations (event_id, cancelled_by)"
                      " SELECT e.id, ? FROM statechart_events e"
                      " WHERE e.source_session_id = ?"
                      "   AND e.send_id = ?"
                      "   AND e.deliver_at > now()"
                      "   AND NOT EXISTS ("
                      "     SELECT 1 FROM statechart_event_claims c WHERE c.event_id = e.id)"
                      " ON CONFLICT (event_id) DO NOTHING")
                 [cancelled-by sid-str send-id])]
    (when (zero? (core/affected-row-count result))
      (log/debug "cancel-events! affected 0 rows (event may have already been delivered or never queued)"
                 {:session-id session-id :send-id send-id}))
    true))

;; -----------------------------------------------------------------------------
;; Claim
;; -----------------------------------------------------------------------------

(defn- claim-events!
  "Claim events ready for delivery. Returns claimed rows (event + side join
   columns).

   A claim is taken by INSERTing into statechart_event_claims. The UPDATE …
   RETURNING pattern from v2 doesn't apply here (no coord columns on events);
   we select candidate rows + insert claim rows in the same tx.

   Per-session FIFO guard: exclude target sessions with an outstanding
   unprocessed claim. Best-effort across simultaneous claim tx's."
  [conn node-id {:keys [session-id batch-size]
                 :or {batch-size 10}}]
  (let [batch-size (long batch-size)
        base-sql   (str "SELECT e.* FROM statechart_events e"
                        " WHERE e.deliver_at <= now()"
                        "   AND NOT EXISTS ("
                        "     SELECT 1 FROM statechart_event_claims c WHERE c.event_id = e.id)"
                        "   AND NOT EXISTS ("
                        "     SELECT 1 FROM statechart_event_cancellations x WHERE x.event_id = e.id)")
        fifo-guard (str "   AND NOT EXISTS ("
                        "     SELECT 1 FROM statechart_event_claims c2"
                        "     JOIN statechart_events e2 ON c2.event_id = e2.id"
                        "     WHERE e2.target_session_id = e.target_session_id"
                        "       AND c2.processed_at IS NULL)")
        session-filter (when session-id "   AND e.target_session_id = ?")
        order-limit (str " ORDER BY e.deliver_at, e.id"
                         " LIMIT " batch-size
                         " FOR UPDATE SKIP LOCKED")
        [sql params] (if session-id
                       [(str base-sql session-filter order-limit)
                        [(core/session-id->str session-id)]]
                       [(str base-sql fifo-guard order-limit)
                        []])
        candidates (core/execute-sql! conn sql params)]
    (when (seq candidates)
      ;; Multi-VALUE INSERT. Avoids pgjdbc-version-dependent array binding.
      (let [placeholders (str/join ", " (repeat (count candidates) "(?, ?)"))
            params       (vec (mapcat (fn [c] [(:id c) node-id]) candidates))]
        (core/execute-sql! conn
          (str "INSERT INTO statechart_event_claims (event_id, claimed_by) VALUES "
               placeholders)
          params)))
    candidates))

(defn- release-claim!
  [conn event-id]
  (core/execute-sql! conn
    "DELETE FROM statechart_event_claims WHERE event_id = ?" [event-id]))

;; -----------------------------------------------------------------------------
;; EventQueue record
;; -----------------------------------------------------------------------------

(defrecord JdbcEventQueue [coordinator datasource node-id]
  sp/EventQueue
  (send! [_ _env send-request]
    (let [{:keys [event type target source-session-id delay]} send-request
          target-id (or target source-session-id)]
      (if (and (supported-type? type) target-id)
        (do (insert-event! datasource send-request)
            (log/debug "Event queued"
                       {:event event :target target-id :delay-ms delay :node-id node-id})
            true)
        (do (log/trace "Event not queued (unsupported type or no target)"
                       {:event event :type type :target target-id})
            false))))

  (cancel! [_ _env session-id send-id]
    (log/debug "Cancelling delayed event"
               {:session-id session-id :send-id send-id :node-id node-id})
    (cancel-events! datasource session-id send-id node-id))

  (receive-events! [this env handler]
    (sp/receive-events! this env handler {}))

  (receive-events! [_ env handler options]
    (let [claimed-events (->> (core/with-tx [tx datasource]
                                (claim-events! tx node-id options))
                              (sort-by (juxt :deliver-at :id)))
          claimed-count  (count claimed-events)]
      (when (pos? claimed-count)
        (log/debug "Claimed events for processing"
                   {:count claimed-count :node-id node-id
                    :session-filter (:session-id options)}))
      (loop [[row & more] claimed-events
             blocked-sessions #{}]
        (when row
          (let [event-id (:id row)
                target   (:target-session-id row)
                blocked? (contains? blocked-sessions target)
                next-blocked
                (cond
                  blocked?
                  (do (release-claim! datasource event-id) blocked-sessions)

                  :else
                  (try
                    (let [event          (row->event row)
                          handle         (coord/begin-event-processing
                                           coordinator
                                           (:target-session-id row)
                                           event-id)
                          env-with-coord (-> env
                                             (assoc ::sc/persistence-coordinator coordinator)
                                             (assoc ::sc/current-event-handle handle))
                          handler-result (handler env-with-coord event)]
                      (when (p/promise? handler-result) @handler-result)
                      (when-not (coord/handle-committed? coordinator handle)
                        ;; Handler never called save-working-memory!
                        ;; (terminated session, etc.) — abort releases the claim.
                        (coord/abort-event-processing coordinator handle))
                      blocked-sessions)

                    (catch Exception e
                      (log/error e "Event handler threw an exception"
                                 {:event-id event-id :target target :node-id node-id})
                      (release-claim! datasource event-id)
                      (conj blocked-sessions target))

                    (catch Error e
                      (log/error e "Event handler threw a fatal error"
                                 {:event-id event-id :target target :node-id node-id})
                      (try (release-claim! datasource event-id)
                           (catch Throwable t
                             (log/error t "Failed to release claim after fatal error")))
                      (throw e))))]
            (recur more next-blocked)))))))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn new-queue
  "Create a new v3 JDBC event queue wired to a coordinator."
  [coordinator]
  (->JdbcEventQueue coordinator (:datasource coordinator) (:node-id coordinator)))

;; -----------------------------------------------------------------------------
;; Maintenance functions
;; -----------------------------------------------------------------------------

(defn purge-processed-events!
  "Delete processed events older than the retention period. Claim rows
   cascade-delete."
  ([ds] (purge-processed-events! ds 7))
  ([ds retention-days]
   (let [retention-days (long retention-days)
         result (core/execute-sql! ds
                  (str "DELETE FROM statechart_events"
                       " WHERE id IN ("
                       "   SELECT event_id FROM statechart_event_claims"
                       "   WHERE processed_at IS NOT NULL"
                       "     AND processed_at < (now() - interval '" retention-days " days'))"))
         purged (core/affected-row-count result)]
     (when (pos? purged)
       (log/info "Purged old processed events"
                 {:count purged :retention-days retention-days}))
     purged)))

(defn queue-depth
  "Unprocessed event count. Unprocessed = no claim row OR claim row with
   processed_at IS NULL."
  ([ds] (queue-depth ds {}))
  ([ds {:keys [session-id]}]
   (let [where-session (when session-id " AND e.target_session_id = ?")
         params        (when session-id [(core/session-id->str session-id)])
         sql (str "SELECT count(*) AS count FROM statechart_events e"
                  " WHERE NOT EXISTS ("
                  "   SELECT 1 FROM statechart_event_claims c"
                  "   WHERE c.event_id = e.id AND c.processed_at IS NOT NULL)"
                  where-session)]
     (:count (core/execute-sql-one! ds sql params)))))
```

- [ ] **Step 2: Verify compilation**

Run: `clojure -M:clj-tests unit --focus com.fulcrologic.statecharts.persistence.jdbc`

Expected: namespace compiles. Existing unit tests may be broken; that's fine for now.

- [ ] **Step 3: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/event_queue.clj
git commit -m "refactor(persistence): event_queue rewrites for v3 schema + coordinator"
```

### Task 3.2: Update `event_queue_spec.clj` for v3

**Files:**
- Modify: `src/test/com/fulcrologic/statecharts/persistence/jdbc/event_queue_spec.clj`

Note: if this spec does not yet exist in the repo, create it based on the test patterns in `integration_test.clj`. Otherwise rewrite its claim/release assertions against the new schema.

- [ ] **Step 1: Read the current spec**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.event-queue-spec 2>&1 | head -40`

(If "no matching test ns" → skip to Step 2, this task creates the file.)

- [ ] **Step 2: Write the v3 event queue integration spec**

Write (or overwrite) `src/test/com/fulcrologic/statecharts/persistence/jdbc/event_queue_spec.clj`:

```clojure
(ns ^:integration com.fulcrologic.statecharts.persistence.jdbc.event-queue-spec
  "Integration tests for the v3 JDBC event queue."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.persistence.jdbc.coordinator :as coord]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.event-queue :as sut]
   [com.fulcrologic.statecharts.persistence.jdbc.fixtures :as fixtures :refer [*pool*]]
   [com.fulcrologic.statecharts.protocols :as sp]))

(use-fixtures :once fixtures/with-pool)
(use-fixtures :each fixtures/with-clean-tables)

(defn- new-queue []
  (let [c (coord/new-jdbc-coordinator *pool* "test-node")]
    (sut/new-queue c)))

(defn- events-for [pool target]
  (core/execute-sql! pool
    "SELECT * FROM statechart_events WHERE target_session_id = ? ORDER BY id"
    [(core/session-id->str target)]))

(defn- cancellations-for [pool event-id]
  (core/execute-sql! pool
    "SELECT * FROM statechart_event_cancellations WHERE event_id = ?" [event-id]))

(deftest ^:integration send-inserts-into-events-only-test
  (let [q (new-queue)]
    (sp/send! q {} {:event :hello :target :s-1})
    (let [rows (events-for *pool* :s-1)]
      (is (= 1 (count rows)))
      (is (= ":hello" (:event-name (first rows))))
      (testing "no claim row is created on send!"
        (is (nil? (core/execute-sql-one! *pool*
                    "SELECT 1 FROM statechart_event_claims WHERE event_id = ?"
                    [(:id (first rows))])))))))

(deftest ^:integration cancel-inserts-into-event-cancellations-not-delete-test
  (let [q (new-queue)]
    (sp/send! q {} {:event :delayed
                    :target :s-1
                    :source-session-id :s-1
                    :send-id "sid-1"
                    :delay 60000})
    (sp/cancel! q {} :s-1 "sid-1")
    (let [events (events-for *pool* :s-1)]
      (testing "event row still present (append-only)"
        (is (= 1 (count events))))
      (testing "cancellation row inserted"
        (is (seq (cancellations-for *pool* (:id (first events)))))))))

(deftest ^:integration receive-events-skips-cancelled-test
  (let [q (new-queue)]
    ;; Insert a deliverable event then cancel it BEFORE any claim.
    (sp/send! q {} {:event :doomed
                    :target :s-1
                    :source-session-id :s-1
                    :send-id "sid-x"
                    :delay 0})
    ;; deliver_at = now() so it IS deliverable by default — the cancel-window
    ;; predicate in cancel-events! excludes deliver_at <= now, so we need to
    ;; construct a delayed event and cancel before deliver_at.
    ;; Rebuild: insert a +1s-delayed event, cancel, verify not delivered.
    (core/execute-sql! *pool* "TRUNCATE statechart_events CASCADE")
    (sp/send! q {} {:event :doomed
                    :target :s-1
                    :source-session-id :s-1
                    :send-id "sid-y"
                    :delay 1000})
    (sp/cancel! q {} :s-1 "sid-y")
    (Thread/sleep 1100)
    (let [received (atom [])
          handler  (fn [_env event]
                     (swap! received conj (:name event)))]
      (sp/receive-events! q {} handler)
      (is (empty? @received) "cancelled event must not be delivered"))))

(deftest ^:integration per-session-fifo-blocks-concurrent-claim-test
  (let [q      (new-queue)
        e1-id  (do (sp/send! q {} {:event :a :target :s-1}) (:id (last (events-for *pool* :s-1))))
        e2-id  (do (sp/send! q {} {:event :b :target :s-1}) (:id (last (events-for *pool* :s-1))))]
    ;; Simulate another worker holding an unprocessed claim on e1.
    (core/execute-sql! *pool*
      "INSERT INTO statechart_event_claims (event_id, claimed_by) VALUES (?, ?)"
      [e1-id "other-node"])
    (let [received (atom [])]
      (sp/receive-events! q {} (fn [_ e] (swap! received conj (:name e))))
      (is (empty? @received)
          "FIFO guard prevents this node from claiming e2 while e1 is in flight on another node"))))
```

- [ ] **Step 3: Run to verify**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.event-queue-spec`

Expected: tests pass. If they don't, the failure explains whether the bug is in send/cancel/receive — fix the v3 code accordingly before continuing.

- [ ] **Step 4: Commit**

```bash
git add src/test/com/fulcrologic/statecharts/persistence/jdbc/event_queue_spec.clj
git commit -m "test(persistence): v3 event queue integration spec"
```

---

## Phase 4: Rewrite `working_memory_store.clj` for v3

### Task 4.1: Remove env-hook handling; route saves through the coordinator

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/working_memory_store.clj`

- [ ] **Step 1: Replace the file with the v3 shape**

Overwrite `src/main/com/fulcrologic/statecharts/persistence/jdbc/working_memory_store.clj`:

```clojure
(ns com.fulcrologic.statecharts.persistence.jdbc.working-memory-store
  "PostgreSQL-backed working-memory store (v3).

   Holds a `PersistenceCoordinator` reference. On save, if the env carries
   `::sc/current-event-handle`, the save routes through the coordinator's
   `commit-event-processing` so the WM write + event ACK commit atomically.
   Otherwise the save runs in its own tx (start! path, non-coordinated use)."
  (:require
   [clojure.edn :as edn]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.persistence.jdbc.coordinator :as coord]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.job-store :as job-store]
   [com.fulcrologic.statecharts.protocols :as sp]
   [taoensso.timbre :as log]))

;; -----------------------------------------------------------------------------
;; Optimistic lock failure
;; -----------------------------------------------------------------------------

(defn optimistic-lock-failure [session-id expected-version]
  (ex-info "Optimistic lock failure: session was modified by another process"
           {:type ::optimistic-lock-failure
            :session-id session-id
            :expected-version expected-version}))

(defn optimistic-lock-failure? [e]
  (and (instance? clojure.lang.ExceptionInfo e)
       (= ::optimistic-lock-failure (:type (ex-data e)))))

;; -----------------------------------------------------------------------------
;; Read / write helpers
;; -----------------------------------------------------------------------------

(defn- read-statechart-src [s]
  (try (edn/read-string s)
       (catch Exception e
         (log/error e "Malformed statechart_src in session row — falling back to raw string"
                    {:raw s})
         s)))

(defn- fetch-session [ds session-id]
  (when-let [row (core/execute-one! ds
                   {:select [:working-memory :version :statechart-src]
                    :from   [:statechart-sessions]
                    :where  [:= :session-id (core/session-id->str session-id)]})]
    (let [src (read-statechart-src (:statechart-src row))]
      (-> (:working-memory row)
          core/thaw
          (assoc ::sc/statechart-src src)
          (core/attach-version (:version row))))))

(defn- upsert-session!
  "Write WM on `conn` (DataSource OR tx connection). Runs a single statement;
   the caller controls transaction scope."
  [conn session-id wmem]
  (let [expected-version (core/get-version wmem)]
    (if expected-version
      (let [new-version (inc expected-version)
            result (core/execute! conn
                     {:update :statechart-sessions
                      :set {:working-memory (core/freeze wmem)
                            :version        new-version
                            :updated-at     [:now]}
                      :where [:and
                              [:= :session-id (core/session-id->str session-id)]
                              [:= :version    expected-version]]})]
        (when (zero? (core/affected-row-count result))
          (log/warn "Optimistic lock failure"
                    {:session-id session-id :expected-version expected-version})
          (throw (optimistic-lock-failure session-id expected-version)))
        true)
      (let [src (get wmem ::sc/statechart-src)]
        (core/execute! conn
          {:insert-into :statechart-sessions
           :values [{:session-id     (core/session-id->str session-id)
                     :statechart-src (pr-str src)
                     :working-memory (core/freeze wmem)
                     :version        1}]
           :on-conflict   [:session-id]
           :do-update-set {:working-memory :excluded.working-memory
                           :statechart-src :excluded.statechart-src
                           :version        [:+ :statechart-sessions.version 1]
                           :updated-at     [:now]}})
        true))))

(defn- delete-session!
  "Delete a session + cancel its durable jobs + purge its pending events.
   All three operations share one transaction."
  [ds session-id]
  (core/with-tx [tx ds]
    (job-store/cancel-by-session-in-tx! tx session-id)
    (core/execute-sql! tx
      (str "DELETE FROM statechart_events"
           " WHERE target_session_id = ?"
           "   AND NOT EXISTS ("
           "     SELECT 1 FROM statechart_event_claims c"
           "     WHERE c.event_id = statechart_events.id"
           "       AND c.processed_at IS NOT NULL)")
      [(core/session-id->str session-id)])
    (let [result (core/execute-sql! tx
                   "DELETE FROM statechart_sessions WHERE session_id = ?"
                   [(core/session-id->str session-id)])]
      (when (pos? (core/affected-row-count result))
        (log/debug "Session deleted" {:session-id session-id}))
      true)))

;; -----------------------------------------------------------------------------
;; WorkingMemoryStore record
;; -----------------------------------------------------------------------------

(defrecord JdbcWorkingMemoryStore [coordinator datasource]
  sp/WorkingMemoryStore
  (get-working-memory [_ _env session-id]
    (fetch-session datasource session-id))

  (save-working-memory! [_ env session-id wmem]
    (if-let [handle (::sc/current-event-handle env)]
      (coord/commit-event-processing
        (or (::sc/persistence-coordinator env) coordinator)
        handle
        (fn [tx] (upsert-session! tx session-id wmem)))
      (upsert-session! datasource session-id wmem)))

  (delete-working-memory! [_ _env session-id]
    (delete-session! datasource session-id)))

(defn new-store
  "Create a v3 working-memory store wired to a coordinator."
  [coordinator]
  (->JdbcWorkingMemoryStore coordinator (:datasource coordinator)))

;; -----------------------------------------------------------------------------
;; Retry helper (unchanged API from v2)
;; -----------------------------------------------------------------------------

(defn with-optimistic-retry
  ([f] (with-optimistic-retry {} f))
  ([{:keys [max-retries backoff-ms] :or {max-retries 3 backoff-ms 50}} f]
   (loop [attempt 1]
     (let [result (try
                    {:ok (f)}
                    (catch clojure.lang.ExceptionInfo e
                      (if (and (optimistic-lock-failure? e)
                               (< attempt max-retries))
                        (let [backoff (* backoff-ms (long (Math/pow 2 (dec attempt))))]
                          (log/debug "Retrying after optimistic lock failure"
                                     {:attempt attempt :max-retries max-retries
                                      :backoff-ms backoff
                                      :session-id (:session-id (ex-data e))})
                          {:retry true :backoff backoff})
                        (throw e))))]
       (if (:retry result)
         (do (Thread/sleep (:backoff result))
             (recur (inc attempt)))
         (:ok result))))))
```

- [ ] **Step 2: Verify compilation**

Run: `clojure -M:clj-tests unit --focus com.fulcrologic.statecharts.persistence.jdbc`

Expected: compiles. Integration tests will break until job_store is rewritten too (next phase).

- [ ] **Step 3: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/working_memory_store.clj
git commit -m "refactor(persistence): WM store routes through coordinator in v3"
```

---

## Phase 5: Rewrite `job_store.clj` for v3

This is the biggest phase — the job store touches all four coordination tables (`statechart_jobs`, `statechart_job_leases`, `statechart_job_results`, `statechart_terminal_dispatches`). We TDD each operation against live PG.

### Task 5.1: `create-job!` and supporting helpers

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj`
- Test: `src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj`

- [ ] **Step 1: Replace `job_store.clj` with v3 skeleton + create-job!**

Overwrite `src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj`:

```clojure
(ns com.fulcrologic.statecharts.persistence.jdbc.job-store
  "PostgreSQL-backed durable job store (v3).

   Jobs, leases, results, and terminal dispatches live in separate relations.
   `statechart_jobs.terminated_at` is explicitly denormalised from
   `statechart_job_results.completed_at` so partial unique indexes work; writes
   to both happen in the same transaction."
  (:require
   [com.fulcrologic.statecharts.events :as evts]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [taoensso.timbre :as log])
  (:import
   [java.time OffsetDateTime Duration]))

;; -----------------------------------------------------------------------------
;; Invokeid serialisation (unchanged from v2)
;; -----------------------------------------------------------------------------

(defn invokeid->str [invokeid]
  (core/encode-id invokeid {:uuid-shape    :tagged
                            :keyword-shape :bare-unless-numeric
                            :symbol-shape  :bare-unless-numeric}))

(defn str->invokeid [s]
  (core/decode-id s {:legacy-fallback :keyword}))

;; -----------------------------------------------------------------------------
;; Helpers
;; -----------------------------------------------------------------------------

(defn- backoff-seconds [attempt]
  (min 60 (long (Math/pow 2 attempt))))

(defn- hydrate-job-row
  "Decode persisted columns. v3 rows from statechart_jobs carry only domain
   columns; coordination fields are on side tables joined at query time."
  [row]
  (cond-> row
    (:session-id row) (update :session-id core/str->session-id)
    (:invokeid row)   (update :invokeid str->invokeid)
    (:payload row)    (update :payload core/thaw)
    (:result row)     (update :result core/thaw)
    (:error row)      (update :error core/thaw)
    (:metadata row)   (update :metadata core/thaw)))

;; -----------------------------------------------------------------------------
;; create-job!
;; -----------------------------------------------------------------------------

(defn create-job!
  "Create a new durable job + its initial lease. Idempotent via the partial
   unique index `idx_jobs_active_per_invoke` on (session_id, invokeid) WHERE
   terminated_at IS NULL. If an active job exists, return its id; otherwise
   insert and return the supplied id.

   Unlike v2, there's no race-retry loop — the partial-unique constraint
   plus one short tx gives deterministic dedup."
  [ds {:keys [id session-id invokeid job-type payload max-attempts]
       :or   {max-attempts 3}}]
  (let [sid-str (core/session-id->str session-id)
        iid-str (invokeid->str invokeid)]
    (core/with-tx [tx ds]
      (let [rows (core/execute-sql! tx
                   (str "INSERT INTO statechart_jobs (id, session_id, invokeid, job_type, payload, max_attempts)"
                        " VALUES (?, ?, ?, ?, ?, ?)"
                        " ON CONFLICT (session_id, invokeid) WHERE terminated_at IS NULL"
                        " DO NOTHING"
                        " RETURNING id")
                   [id sid-str iid-str job-type (core/freeze payload) max-attempts])
            inserted? (seq rows)]
        (if inserted?
          (do
            (core/execute-sql! tx
              (str "INSERT INTO statechart_job_leases (job_id, attempt, next_run_at)"
                   " VALUES (?, 0, now())")
              [id])
            id)
          (:id (first (core/execute-sql! tx
                        (str "SELECT id FROM statechart_jobs"
                             " WHERE session_id = ? AND invokeid = ?"
                             "   AND terminated_at IS NULL")
                        [sid-str iid-str]))))))))
```

- [ ] **Step 2: Overwrite `job_store_spec.clj` with v3 tests**

This spec is large (v3 has many operations). Write the full v3 spec now; subsequent tasks add more tests. Start with create-job tests:

Replace `src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj` with:

```clojure
(ns ^:integration com.fulcrologic.statecharts.persistence.jdbc.job-store-spec
  "Integration tests for the v3 PostgreSQL job store."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.fixtures :as fixtures :refer [*pool*]]
   [com.fulcrologic.statecharts.persistence.jdbc.job-store :as sut])
  (:import
   [java.time OffsetDateTime Duration]
   [java.util UUID]))

(use-fixtures :once fixtures/with-pool)
(use-fixtures :each fixtures/with-clean-tables)

(defn- make-job-params
  ([] (make-job-params {}))
  ([overrides]
   (merge {:id (UUID/randomUUID)
           :session-id :test-session
           :invokeid :invoke-1
           :job-type "http"
           :payload {:url "https://example.com"}
           :max-attempts 3}
          overrides)))

(defn- job-row [pool job-id]
  (core/execute-sql-one! pool
    "SELECT * FROM statechart_jobs WHERE id = ?" [job-id]))

(defn- lease-row [pool job-id]
  (core/execute-sql-one! pool
    "SELECT * FROM statechart_job_leases WHERE job_id = ?" [job-id]))

(defn- results-row [pool job-id]
  (core/execute-sql-one! pool
    "SELECT * FROM statechart_job_results WHERE job_id = ?" [job-id]))

(defn- dispatch-row [pool job-id]
  (core/execute-sql-one! pool
    "SELECT * FROM statechart_terminal_dispatches WHERE job_id = ?" [job-id]))

;; -----------------------------------------------------------------------------
;; 1. create-job!
;; -----------------------------------------------------------------------------

(deftest ^:integration create-job-inserts-job-and-lease-test
  (testing "create-job! returns the job id and writes a lease row"
    (let [params (make-job-params)
          returned (sut/create-job! *pool* params)]
      (is (= (:id params) returned))
      (is (some? (job-row *pool* (:id params))))
      (is (some? (lease-row *pool* (:id params)))
          "a lease row accompanies every active job")
      (is (nil? (:terminated-at (job-row *pool* (:id params))))
          "new jobs are not terminated"))))

(deftest ^:integration create-job-is-idempotent-per-session-invokeid-test
  (testing "duplicate (session, invoke) returns the existing active job id"
    (let [p1 (make-job-params)
          p2 (assoc (make-job-params) :id (UUID/randomUUID))  ; same session + invoke, different UUID
          r1 (sut/create-job! *pool* p1)
          r2 (sut/create-job! *pool* p2)]
      (is (= r1 r2) "second create returns the first job's id")
      (is (= (:id p1) r1))
      (testing "only one job row exists for this (session, invoke)"
        (is (= 1 (count (core/execute-sql! *pool*
                          "SELECT id FROM statechart_jobs WHERE session_id = ? AND invokeid = ?"
                          [(core/session-id->str (:session-id p1)) (sut/invokeid->str (:invokeid p1))]))))))))

(deftest ^:integration create-job-after-termination-inserts-new-row-test
  (testing "if the previous job is terminated, a new (session, invoke) job is allowed"
    (let [p1 (make-job-params)
          r1 (sut/create-job! *pool* p1)]
      ;; Mark as terminated
      (core/execute-sql! *pool*
        "UPDATE statechart_jobs SET terminated_at = now() WHERE id = ?" [r1])
      (core/execute-sql! *pool*
        "DELETE FROM statechart_job_leases WHERE job_id = ?" [r1])
      ;; Now create a new job with the same (session, invoke)
      (let [p2 (assoc (make-job-params) :id (UUID/randomUUID))
            r2 (sut/create-job! *pool* p2)]
        (is (not= r1 r2) "new job id is distinct from terminated job id")
        (is (= 2 (count (core/execute-sql! *pool*
                          "SELECT id FROM statechart_jobs WHERE session_id = ? AND invokeid = ?"
                          [(core/session-id->str (:session-id p1))
                           (sut/invokeid->str (:invokeid p1))]))))))))
```

- [ ] **Step 3: Run and verify all three tests pass**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.job-store-spec`

Expected: tests pass. Other tests in this spec (for claim/complete/fail/cancel/dispatch) don't exist yet — those tasks follow.

- [ ] **Step 4: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj \
        src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj
git commit -m "feat(persistence): v3 create-job! via partial unique index + spec"
```

### Task 5.2: `claim-jobs!` and `heartbeat!`

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj`
- Modify: `src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj`

- [ ] **Step 1: Append tests to the spec**

Append to `src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj`:

```clojure
;; -----------------------------------------------------------------------------
;; 2. claim-jobs!
;; -----------------------------------------------------------------------------

(deftest ^:integration claim-jobs-marks-lease-running-and-increments-attempt-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)
        claimed (sut/claim-jobs! *pool* {:owner-id "worker-a" :limit 5})]
    (is (= 1 (count claimed)))
    (let [row (lease-row *pool* (:id p))]
      (is (= "worker-a" (:lease-owner row)))
      (is (= 1         (:attempt row)))
      (is (some? (:lease-expires-at row))))))

(deftest ^:integration claim-jobs-skips-already-claimed-with-live-lease-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)]
    (sut/claim-jobs! *pool* {:owner-id "worker-a" :limit 5 :lease-duration-seconds 60})
    (let [second-worker (sut/claim-jobs! *pool* {:owner-id "worker-b" :limit 5 :lease-duration-seconds 60})]
      (is (empty? second-worker) "worker-b cannot claim an actively-leased job"))))

(deftest ^:integration claim-jobs-reclaims-expired-lease-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)]
    (sut/claim-jobs! *pool* {:owner-id "worker-a" :limit 5 :lease-duration-seconds 60})
    ;; Expire the lease
    (core/execute-sql! *pool*
      "UPDATE statechart_job_leases SET lease_expires_at = now() - interval '1 second' WHERE job_id = ?"
      [(:id p)])
    (let [reclaimed (sut/claim-jobs! *pool* {:owner-id "worker-b" :limit 5 :lease-duration-seconds 60})]
      (is (= 1 (count reclaimed)))
      (is (= "worker-b" (:lease-owner (lease-row *pool* (:id p))))))))

(deftest ^:integration claim-jobs-respects-next-run-at-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)]
    ;; Push next_run_at into the future
    (core/execute-sql! *pool*
      "UPDATE statechart_job_leases SET next_run_at = now() + interval '1 hour' WHERE job_id = ?"
      [(:id p)])
    (let [claimed (sut/claim-jobs! *pool* {:owner-id "worker-a" :limit 5})]
      (is (empty? claimed)))))

;; -----------------------------------------------------------------------------
;; 3. heartbeat!
;; -----------------------------------------------------------------------------

(deftest ^:integration heartbeat-extends-lease-for-owner-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)
        _ (sut/claim-jobs! *pool* {:owner-id "worker-a" :limit 5 :lease-duration-seconds 1})
        initial (:lease-expires-at (lease-row *pool* (:id p)))]
    (Thread/sleep 50)
    (is (sut/heartbeat! *pool* (:id p) "worker-a" 30))
    (let [after (:lease-expires-at (lease-row *pool* (:id p)))]
      (is (pos? (compare after initial))
          "lease_expires_at moved forward"))))

(deftest ^:integration heartbeat-returns-false-for-wrong-owner-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)
        _ (sut/claim-jobs! *pool* {:owner-id "worker-a" :limit 5})]
    (is (false? (sut/heartbeat! *pool* (:id p) "worker-b" 30)))))
```

- [ ] **Step 2: Run to verify failure (not yet implemented)**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.job-store-spec`

Expected: new tests fail with "Unable to resolve symbol: claim-jobs!" etc.

- [ ] **Step 3: Append implementation to job_store.clj**

Append to `src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj`:

```clojure
;; -----------------------------------------------------------------------------
;; claim-jobs!
;; -----------------------------------------------------------------------------

(defn claim-jobs!
  "Claim up to `limit` jobs for this worker.

   A lease is claimable iff:
   - next_run_at <= now()
   - (lease_owner IS NULL) OR (lease_expires_at < now())  -- unclaimed or expired

   lease_expires_at is computed server-side so app clock skew can't buy extra
   ownership. Returns claimed job rows with thawed payload."
  [ds {:keys [owner-id lease-duration-seconds limit]
       :or   {lease-duration-seconds 60 limit 5}}]
  (let [limit      (long limit)
        lease-secs (long lease-duration-seconds)]
    (core/with-tx [tx ds]
      (let [claimed-ids (->> (core/execute-sql! tx
                               (str "UPDATE statechart_job_leases"
                                    " SET lease_owner      = ?,"
                                    "     lease_expires_at = now() + interval '" lease-secs " seconds',"
                                    "     attempt          = attempt + 1,"
                                    "     updated_at       = now()"
                                    " WHERE job_id IN ("
                                    "   SELECT l.job_id FROM statechart_job_leases l"
                                    "   WHERE l.next_run_at <= now()"
                                    "     AND (l.lease_owner IS NULL OR l.lease_expires_at < now())"
                                    "   ORDER BY l.next_run_at"
                                    "   LIMIT " limit
                                    "   FOR UPDATE SKIP LOCKED)"
                                    " RETURNING job_id")
                               [owner-id])
                             (map :job-id))]
        (if (seq claimed-ids)
          (let [placeholders (clojure.string/join ", " (repeat (count claimed-ids) "?"))]
            (->> (core/execute-sql! tx
                   (str "SELECT j.*, l.attempt, l.next_run_at, l.lease_owner, l.lease_expires_at"
                        "  FROM statechart_jobs j"
                        "  JOIN statechart_job_leases l ON l.job_id = j.id"
                        " WHERE j.id IN (" placeholders ")"
                        " ORDER BY l.next_run_at, j.id")
                   (vec claimed-ids))
                 (mapv hydrate-job-row)))
          [])))))

;; -----------------------------------------------------------------------------
;; heartbeat!
;; -----------------------------------------------------------------------------

(defn heartbeat!
  "Extend the lease for a job owned by this worker. Returns true if extended,
   false otherwise."
  [ds job-id owner-id lease-duration-seconds]
  (let [lease-secs (long lease-duration-seconds)
        result (core/execute-sql! ds
                 (str "UPDATE statechart_job_leases"
                      " SET lease_expires_at = now() + interval '" lease-secs " seconds',"
                      "     updated_at       = now()"
                      " WHERE job_id = ? AND lease_owner = ?"
                      " RETURNING job_id")
                 [job-id owner-id])]
    (pos? (core/affected-row-count result))))
```

You must also import `java.util.UUID` in the ns form. Update the existing `(:import ...)` clause to:

```clojure
  (:import
   [java.time OffsetDateTime Duration]
   [java.util UUID])
```

- [ ] **Step 4: Run to verify all tests pass**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.job-store-spec`

Expected: all 7 tests pass (3 create + 4 claim + 2 heartbeat).

- [ ] **Step 5: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj \
        src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj
git commit -m "feat(persistence): v3 claim-jobs! / heartbeat! against job_leases"
```

### Task 5.3: `complete!` and `fail!`

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj`
- Modify: `src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj`

- [ ] **Step 1: Append tests**

Append to `src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj`:

```clojure
;; -----------------------------------------------------------------------------
;; 4. complete!
;; -----------------------------------------------------------------------------

(deftest ^:integration complete-writes-succeeded-result-and-terminates-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)
        _ (sut/claim-jobs! *pool* {:owner-id "w" :limit 5})
        ok? (sut/complete! *pool* (:id p) "w" {:ok 42})]
    (is ok?)
    (let [jr (results-row *pool* (:id p))
          jj (job-row     *pool* (:id p))
          jl (lease-row   *pool* (:id p))]
      (is (= "succeeded" (:status jr)))
      (is (= {:ok 42}   (core/thaw (:result jr))))
      (is (some? (:terminated-at jj)))
      (is (nil? jl) "lease row is deleted on completion"))))

(deftest ^:integration complete-rejects-non-owner-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)
        _ (sut/claim-jobs! *pool* {:owner-id "w" :limit 5})
        ok? (sut/complete! *pool* (:id p) "other-worker" {:ok 42})]
    (is (false? ok?))
    (is (nil? (results-row *pool* (:id p))) "no result row")
    (is (nil? (:terminated-at (job-row *pool* (:id p))))
        "job remains active")))

;; -----------------------------------------------------------------------------
;; 5. fail! — retry
;; -----------------------------------------------------------------------------

(deftest ^:integration fail-reschedules-when-attempts-remain-test
  (let [p (make-job-params {:max-attempts 3})
        _ (sut/create-job! *pool* p)
        _ (sut/claim-jobs! *pool* {:owner-id "w" :limit 5})]
    (is (= :retry-scheduled
           (sut/fail! *pool* (:id p) "w" 1 3 {:error "boom"})))
    (let [jl (lease-row *pool* (:id p))
          jr (results-row *pool* (:id p))]
      (is (nil? jr) "no result row yet — still retrying")
      (is (nil? (:lease-owner jl)) "lease is released for re-claim")
      (is (some? (:attempt-error jl)) "retry error stored in lease"))))

(deftest ^:integration fail-terminates-when-attempts-exhausted-test
  (let [p (make-job-params {:max-attempts 1})
        _ (sut/create-job! *pool* p)
        _ (sut/claim-jobs! *pool* {:owner-id "w" :limit 5})]
    (is (= :failed
           (sut/fail! *pool* (:id p) "w" 1 1 {:error "final"})))
    (let [jl (lease-row *pool* (:id p))
          jr (results-row *pool* (:id p))
          jj (job-row *pool* (:id p))]
      (is (= "failed" (:status jr)))
      (is (= {:error "final"} (core/thaw (:error jr))))
      (is (some? (:terminated-at jj)))
      (is (nil? jl) "lease row deleted on terminal failure"))))
```

- [ ] **Step 2: Run to verify failures**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.job-store-spec`

Expected: new tests fail with undefined `complete!` / `fail!`.

- [ ] **Step 3: Append implementation**

Append to `src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj`:

```clojure
;; -----------------------------------------------------------------------------
;; complete!
;; -----------------------------------------------------------------------------

(defn- write-result-and-terminate!
  "Shared helper: insert job_results, update terminated_at, delete lease.
   Caller supplies an already-open tx."
  [tx job-id status result-payload error-payload metadata-payload]
  (core/execute-sql! tx
    (str "INSERT INTO statechart_job_results (job_id, status, result, error, metadata)"
         " VALUES (?, ?, ?, ?, ?)")
    [job-id status
     (some-> result-payload core/freeze)
     (some-> error-payload core/freeze)
     (some-> metadata-payload core/freeze)])
  (core/execute-sql! tx
    "UPDATE statechart_jobs SET terminated_at = now() WHERE id = ?"
    [job-id])
  (core/execute-sql! tx
    "DELETE FROM statechart_job_leases WHERE job_id = ?"
    [job-id]))

(defn complete!
  "Mark a job as succeeded. Writes job_results, sets terminated_at, deletes
   the lease. All in one tx. Returns true on success, false if owner check
   fails or the job is already terminated.

   If owner-id is nil, owner is not checked (maintenance callers only)."
  ([ds job-id result]
   (complete! ds job-id nil result))
  ([ds job-id owner-id result]
   (core/with-tx [tx ds]
     (let [where-sql  (if owner-id
                        "WHERE l.job_id = ? AND l.lease_owner = ? AND j.terminated_at IS NULL"
                        "WHERE l.job_id = ? AND j.terminated_at IS NULL")
           params     (cond-> [job-id] owner-id (conj owner-id))
           locked (core/execute-sql! tx
                    (str "SELECT j.id FROM statechart_jobs j"
                         " JOIN statechart_job_leases l ON l.job_id = j.id "
                         where-sql
                         " FOR UPDATE")
                    params)]
       (if (seq locked)
         (do (write-result-and-terminate! tx job-id "succeeded" result nil nil)
             true)
         false)))))

;; -----------------------------------------------------------------------------
;; fail!
;; -----------------------------------------------------------------------------

(defn fail!
  "Handle job failure. If attempts remain, update the lease for retry. If
   exhausted, write the terminal failure. Returns :retry-scheduled, :failed,
   or :ignored."
  ([ds job-id attempt max-attempts error]
   (fail! ds job-id nil attempt max-attempts error))
  ([ds job-id owner-id attempt max-attempts error]
   (core/with-tx [tx ds]
     (if (< attempt max-attempts)
       ;; Retry
       (let [delay-secs (backoff-seconds attempt)
             next-run   (.plus (OffsetDateTime/now) (Duration/ofSeconds delay-secs))
             where-sql  (if owner-id
                          " WHERE job_id = ? AND lease_owner = ?"
                          " WHERE job_id = ?")
             params     (cond-> [job-id] owner-id (conj owner-id))
             rows (core/execute-sql! tx
                    (str "UPDATE statechart_job_leases"
                         " SET next_run_at      = ?,"
                         "     lease_owner      = NULL,"
                         "     lease_expires_at = NULL,"
                         "     attempt_error    = ?,"
                         "     updated_at       = now()"
                         where-sql
                         " RETURNING job_id")
                    (into [next-run (core/freeze error)] params))]
         (if (pos? (core/affected-row-count rows))
           (do (log/info "Job failed, scheduling retry"
                         {:job-id job-id :attempt attempt :max-attempts max-attempts
                          :next-run-in-seconds delay-secs})
               :retry-scheduled)
           :ignored))
       ;; Terminal failure
       (let [where-sql  (if owner-id
                          "WHERE l.job_id = ? AND l.lease_owner = ?"
                          "WHERE l.job_id = ?")
             params     (cond-> [job-id] owner-id (conj owner-id))
             locked (core/execute-sql! tx
                      (str "SELECT l.job_id FROM statechart_job_leases l "
                           where-sql
                           " FOR UPDATE")
                      params)]
         (if (seq locked)
           (do (write-result-and-terminate! tx job-id "failed" nil error nil)
               (log/warn "Job failed permanently"
                         {:job-id job-id :attempt attempt :max-attempts max-attempts})
               :failed)
           :ignored))))))
```

- [ ] **Step 4: Run and verify all tests pass**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.job-store-spec`

Expected: 11 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj \
        src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj
git commit -m "feat(persistence): v3 complete! + fail! with atomic terminate"
```

### Task 5.4: `cancel!` and `cancel-by-session!`

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj`
- Modify: `src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj`

- [ ] **Step 1: Append tests**

Append to the spec:

```clojure
;; -----------------------------------------------------------------------------
;; 6. cancel!
;; -----------------------------------------------------------------------------

(deftest ^:integration cancel-writes-cancelled-result-with-metadata-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)]
    (is (= 1 (sut/cancel! *pool* (:session-id p) (:invokeid p))))
    (let [jr (results-row *pool* (:id p))
          jj (job-row *pool* (:id p))]
      (is (= "cancelled" (:status jr)))
      (is (= {:reason :cancelled} (core/thaw (:metadata jr))))
      (is (some? (:terminated-at jj)))
      (is (nil? (lease-row *pool* (:id p)))))))

(deftest ^:integration cancel-returns-zero-for-already-terminated-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)]
    (sut/cancel! *pool* (:session-id p) (:invokeid p))
    (is (= 0 (sut/cancel! *pool* (:session-id p) (:invokeid p)))
        "second cancel is a no-op")))

(deftest ^:integration cancel-by-session-cancels-multiple-jobs-test
  (let [p1 (make-job-params {:invokeid :i-1})
        p2 (make-job-params {:invokeid :i-2})
        _ (sut/create-job! *pool* p1)
        _ (sut/create-job! *pool* p2)]
    (is (= 2 (sut/cancel-by-session! *pool* :test-session)))
    (is (= "cancelled" (:status (results-row *pool* (:id p1)))))
    (is (= "cancelled" (:status (results-row *pool* (:id p2)))))))
```

- [ ] **Step 2: Run to verify failures**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.job-store-spec`

Expected: new tests fail.

- [ ] **Step 3: Append implementation**

Append to `job_store.clj`:

```clojure
;; -----------------------------------------------------------------------------
;; cancel! / cancel-by-session!
;; -----------------------------------------------------------------------------

(def ^:private cancellation-metadata
  {:reason :cancelled})

(defn cancel!
  "Cancel an active job by (session-id, invokeid). Writes a cancelled result +
   terminated_at + deletes lease. Returns 1 on cancel, 0 if no active job."
  [ds session-id invokeid]
  (let [sid-str (core/session-id->str session-id)
        iid-str (invokeid->str invokeid)]
    (core/with-tx [tx ds]
      (let [job-id (:id (first (core/execute-sql! tx
                                  (str "SELECT id FROM statechart_jobs"
                                       " WHERE session_id = ? AND invokeid = ?"
                                       "   AND terminated_at IS NULL"
                                       " FOR UPDATE")
                                  [sid-str iid-str])))]
        (if job-id
          (do (write-result-and-terminate! tx job-id "cancelled" nil nil cancellation-metadata)
              1)
          0)))))

(defn cancel-by-session-in-tx!
  "Cancel all active jobs for a session within a caller's open tx. See
   `working_memory_store/delete-session!` for the intended use."
  [tx session-id]
  (let [sid-str (core/session-id->str session-id)
        ids (->> (core/execute-sql! tx
                   (str "SELECT id FROM statechart_jobs"
                        " WHERE session_id = ? AND terminated_at IS NULL"
                        " FOR UPDATE")
                   [sid-str])
                 (map :id))]
    (reduce
      (fn [n job-id]
        (write-result-and-terminate! tx job-id "cancelled" nil nil cancellation-metadata)
        (inc n))
      0
      ids)))

(defn cancel-by-session!
  "Cancel all active jobs for a session in its own tx."
  [ds session-id]
  (core/with-tx [tx ds]
    (cancel-by-session-in-tx! tx session-id)))
```

- [ ] **Step 4: Run and verify**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.job-store-spec`

Expected: 14 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj \
        src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj
git commit -m "feat(persistence): v3 cancel! + cancel-by-session! via results insert"
```

### Task 5.5: Terminal-dispatch claim and reconciler helpers

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj`
- Modify: `src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj`

- [ ] **Step 1: Append tests**

```clojure
;; -----------------------------------------------------------------------------
;; 7. Terminal dispatch
;; -----------------------------------------------------------------------------

(deftest ^:integration claim-terminal-dispatch-is-idempotent-first-wins-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)
        _ (sut/claim-jobs! *pool* {:owner-id "w" :limit 5})
        _ (sut/complete! *pool* (:id p) "w" {:ok 1})]
    (is (sut/claim-terminal-dispatch! *pool* (:id p)))
    (is (false? (sut/claim-terminal-dispatch! *pool* (:id p))))
    (is (some? (dispatch-row *pool* (:id p))))))

(deftest ^:integration release-terminal-dispatch-claim-clears-dispatched-at-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)
        _ (sut/claim-jobs! *pool* {:owner-id "w" :limit 5})
        _ (sut/complete! *pool* (:id p) "w" {:ok 1})]
    (sut/claim-terminal-dispatch! *pool* (:id p))
    (sut/release-terminal-dispatch-claim! *pool* (:id p))
    (is (nil? (dispatch-row *pool* (:id p))) "released row is deleted")))

(deftest ^:integration get-undispatched-terminal-jobs-returns-terminated-not-yet-dispatched-test
  (let [p1 (make-job-params {:invokeid :i-1})
        p2 (make-job-params {:invokeid :i-2})]
    (sut/create-job! *pool* p1)
    (sut/create-job! *pool* p2)
    (sut/claim-jobs! *pool* {:owner-id "w" :limit 5})
    (sut/complete! *pool* (:id p1) "w" {:r 1})
    (sut/complete! *pool* (:id p2) "w" {:r 2})
    ;; Mark p2 as dispatched
    (sut/claim-terminal-dispatch! *pool* (:id p2))
    (let [pending (sut/get-undispatched-terminal-jobs *pool* 10)
          ids     (set (map :id pending))]
      (is (contains? ids (:id p1)))
      (is (not (contains? ids (:id p2)))))))

(deftest ^:integration terminal-event-name-for-succeeded-test
  (is (= (pr-str :done.invoke.my-invoke)
         (sut/terminal-event-name :succeeded :my-invoke))))

(deftest ^:integration terminal-event-name-for-failed-and-cancelled-test
  (is (= (pr-str :error.invoke.my-invoke)
         (sut/terminal-event-name :failed :my-invoke)))
  (is (= (pr-str :error.invoke.my-invoke)
         (sut/terminal-event-name :cancelled :my-invoke))))
```

- [ ] **Step 2: Run to verify failures**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.job-store-spec`

Expected: new tests fail.

- [ ] **Step 3: Append implementation**

Append to `job_store.clj`:

```clojure
;; -----------------------------------------------------------------------------
;; Terminal-dispatch claim
;; -----------------------------------------------------------------------------

(defn claim-terminal-dispatch!
  "Atomically claim the terminal-event dispatch slot for `job-id`. Returns
   true if this caller won the claim, false if another did. ON CONFLICT handles
   concurrent claims.

   Callers must release on send failure via `release-terminal-dispatch-claim!`."
  [ds job-id]
  (let [rows (core/execute-sql! ds
               (str "INSERT INTO statechart_terminal_dispatches (job_id)"
                    " VALUES (?)"
                    " ON CONFLICT (job_id) DO NOTHING"
                    " RETURNING job_id")
               [job-id])]
    (pos? (core/affected-row-count rows))))

(defn release-terminal-dispatch-claim!
  "Roll back a terminal-dispatch claim (send failed). Next reconciler pass
   retries."
  [ds job-id]
  (core/execute-sql! ds
    "DELETE FROM statechart_terminal_dispatches WHERE job_id = ?" [job-id]))

(defn get-undispatched-terminal-jobs
  "Fetch terminated jobs whose terminal event has not yet been dispatched.
   Returns hydrated rows including the original invokeid and the result/
   error/metadata needed to compute the terminal event."
  [ds limit]
  (let [limit (long limit)
        rows (core/execute-sql! ds
               (str "SELECT j.id, j.session_id, j.invokeid, j.job_type, j.payload,"
                    "       jr.status, jr.result, jr.error, jr.metadata, jr.completed_at"
                    "  FROM statechart_job_results jr"
                    "  JOIN statechart_jobs j ON j.id = jr.job_id"
                    "  LEFT JOIN statechart_terminal_dispatches td ON td.job_id = j.id"
                    " WHERE td.job_id IS NULL"
                    " ORDER BY jr.completed_at"
                    " LIMIT " limit))]
    (mapv hydrate-job-row rows)))

;; -----------------------------------------------------------------------------
;; Terminal event computation (derived, not stored)
;; -----------------------------------------------------------------------------

(defn terminal-event-name
  "Compute the SCXML terminal event name for a job's status + invokeid.
   Shape matches the pr-str-of-keyword form the event queue consumes."
  [status invokeid]
  (case status
    :succeeded (pr-str (evts/invoke-done-event invokeid))
    :failed    (pr-str (evts/invoke-error-event invokeid))
    :cancelled (pr-str (evts/invoke-error-event invokeid))))

(defn terminal-event-data
  "Compute the data payload for a job's terminal event."
  [status result error metadata]
  (case status
    :succeeded result
    :failed    error
    :cancelled metadata))
```

- [ ] **Step 4: Run and verify**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.job-store-spec`

Expected: 19 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj \
        src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj
git commit -m "feat(persistence): v3 terminal-dispatch claim + derived event name/data"
```

### Task 5.6: `get-active-job` and `job-cancelled?`

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj`
- Modify: `src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj`

- [ ] **Step 1: Append tests**

```clojure
;; -----------------------------------------------------------------------------
;; 8. Queries
;; -----------------------------------------------------------------------------

(deftest ^:integration get-active-job-returns-non-terminated-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)]
    (let [j (sut/get-active-job *pool* (:session-id p) (:invokeid p))]
      (is (some? j))
      (is (= (:id p) (:id j))))
    (sut/cancel! *pool* (:session-id p) (:invokeid p))
    (is (nil? (sut/get-active-job *pool* (:session-id p) (:invokeid p)))
        "cancelled jobs are not active")))

(deftest ^:integration job-cancelled?-reads-result-status-test
  (let [p (make-job-params)
        _ (sut/create-job! *pool* p)]
    (is (false? (sut/job-cancelled? *pool* (:id p)))
        "live job is not cancelled")
    (sut/cancel! *pool* (:session-id p) (:invokeid p))
    (is (true? (sut/job-cancelled? *pool* (:id p))))))
```

- [ ] **Step 2: Run to verify failures**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.job-store-spec`

Expected: new tests fail.

- [ ] **Step 3: Append implementation**

```clojure
;; -----------------------------------------------------------------------------
;; Queries
;; -----------------------------------------------------------------------------

(defn get-active-job
  "Return the active (non-terminated) job for (session-id, invokeid), or nil."
  [ds session-id invokeid]
  (let [rows (core/execute-sql! ds
               (str "SELECT j.*, l.attempt, l.next_run_at, l.lease_owner, l.lease_expires_at"
                    "  FROM statechart_jobs j"
                    "  JOIN statechart_job_leases l ON l.job_id = j.id"
                    " WHERE j.session_id = ? AND j.invokeid = ?"
                    "   AND j.terminated_at IS NULL")
               [(core/session-id->str session-id) (invokeid->str invokeid)])]
    (when-let [row (first rows)]
      (hydrate-job-row row))))

(defn job-cancelled?
  "True if the job has a cancelled result row."
  [ds job-id]
  (let [row (core/execute-sql-one! ds
              "SELECT status FROM statechart_job_results WHERE job_id = ?" [job-id])]
    (= "cancelled" (:status row))))
```

- [ ] **Step 4: Run and verify**

Expected: 21 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/job_store.clj \
        src/test/com/fulcrologic/statecharts/persistence/jdbc/job_store_spec.clj
git commit -m "feat(persistence): v3 get-active-job + job-cancelled?"
```

---

## Phase 6: `jdbc.clj` wiring + retention

### Task 6.1: Rewrite `pg-env` to use the coordinator, update retention functions

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc.clj`

- [ ] **Step 1: Replace the file**

Overwrite `src/main/com/fulcrologic/statecharts/persistence/jdbc.clj`:

```clojure
(ns com.fulcrologic.statecharts.persistence.jdbc
  "JDBC-backed (PostgreSQL) persistence layer (v3).

   Top-level API. `pg-env` constructs a coordinator, event queue, and WM store
   all wired together. See `docs/ai/specs/persistence-v3-coordinator.md` for
   the full design.

   Consumers pass a `javax.sql.DataSource` (HikariCP is the standard choice)."
  (:require
   [clojure.string :as str]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.algorithms.v20150901 :as alg]
   [com.fulcrologic.statecharts.algorithms.v20150901-validation :as v]
   [com.fulcrologic.statecharts.data-model.working-memory-data-model :as wmdm]
   [com.fulcrologic.statecharts.event-queue.event-processing :as ep]
   [com.fulcrologic.statecharts.execution-model.lambda :as lambda]
   [com.fulcrologic.statecharts.invocation.future :as i.future]
   [com.fulcrologic.statecharts.invocation.statechart :as i.statechart]
   [com.fulcrologic.statecharts.persistence.jdbc.coordinator :as coord]
   [com.fulcrologic.statecharts.persistence.jdbc.event-queue :as pg-eq]
   [com.fulcrologic.statecharts.persistence.jdbc.schema :as schema]
   [com.fulcrologic.statecharts.persistence.jdbc.working-memory-store :as pg-wms]
   [com.fulcrologic.statecharts.protocols :as sp]
   [com.fulcrologic.statecharts.registry.local-memory-registry :as mem-reg]
   [com.fulcrologic.statecharts.util :refer [new-uuid]]
   [taoensso.timbre :as log]))

;; -----------------------------------------------------------------------------
;; Schema
;; -----------------------------------------------------------------------------

(defn create-tables! [datasource] (schema/create-tables! datasource))
(defn drop-tables!   [datasource] (schema/drop-tables!   datasource))

;; -----------------------------------------------------------------------------
;; Environment
;; -----------------------------------------------------------------------------

(defn pg-env
  "Create a statechart environment backed by v3 PostgreSQL persistence.

   Options:
   - :datasource - javax.sql.DataSource (REQUIRED).
   - :node-id    - Unique identifier for this worker node. MUST be globally
                   unique. Default is `(str (random-uuid))`. Collisions break
                   stale-claim recovery.
   - :data-model, :execution-model, :invocation-processors, :processor —
     as in v2; processor must match execution-model async-ness.

   Returns an `::sc/env` map with a live `PersistenceCoordinator` under
   `::sc/persistence-coordinator` and `::coord` (for maintenance callers)."
  [{:keys [datasource node-id data-model execution-model invocation-processors processor]
    :or {node-id (str (random-uuid))}}]
  (assert datasource "A javax.sql.DataSource is required under :datasource")
  (when processor
    (assert (satisfies? sp/Processor processor)
            ":processor must implement com.fulcrologic.statecharts.protocols/Processor"))
  (let [coord      (coord/new-jdbc-coordinator datasource node-id)
        dm         (or data-model (wmdm/new-flat-model))
        q          (pg-eq/new-queue coord)
        ex         (or execution-model (lambda/new-execution-model dm q))
        registry   (mem-reg/new-registry)
        wmstore    (pg-wms/new-store coord)
        inv-procs  (or invocation-processors
                       [(i.statechart/new-invocation-processor)
                        (i.future/new-future-processor)])]
    {::sc/statechart-registry       registry
     ::sc/data-model                dm
     ::sc/event-queue               q
     ::sc/working-memory-store      wmstore
     ::sc/processor                 (or processor (alg/new-processor))
     ::sc/invocation-processors     inv-procs
     ::sc/execution-model           ex
     ::sc/persistence-coordinator   coord   ; consumed by WM store on save
     ::coord                        coord   ; maintenance functions
     ::datasource                   datasource
     ::node-id                      node-id}))

;; -----------------------------------------------------------------------------
;; Convenience (unchanged behaviour)
;; -----------------------------------------------------------------------------

(defn register!
  "Register a chart in the env's registry. Throws on validation errors."
  [{::sc/keys [statechart-registry]} chart-key chart]
  (let [problems  (v/problems chart)
        errors?   (boolean (some #(= :error (:level %)) problems))
        warnings? (boolean (some #(= :warn (:level %)) problems))]
    (cond
      errors?   (throw (ex-info "Cannot register invalid chart"
                                {:chart-key chart-key :problems (vec problems)}))
      warnings? (log/warn "Chart" chart-key "has problems:"
                          (str/join "," (map (fn [{:keys [element message]}]
                                               (str element ": " message))
                                             problems))))
    (sp/register-statechart! statechart-registry chart-key chart))
  true)

(defn start!
  ([env chart-src] (start! env chart-src (new-uuid)))
  ([{::sc/keys [processor working-memory-store statechart-registry] :as env}
    chart-src session-id]
   (assert statechart-registry "There must be a statechart registry in env")
   (assert working-memory-store "There must be a working memory store in env")
   (assert (sp/get-statechart statechart-registry chart-src)
           (str "A chart must be registered under " chart-src))
   (let [s0 (sp/start! processor env chart-src {::sc/session-id session-id})]
     (sp/save-working-memory! working-memory-store env session-id s0)
     (log/info "Statechart session started"
               {:session-id session-id
                :statechart-src chart-src
                :initial-configuration (::sc/configuration s0)}))
   true))

(defn send!
  "Send an event. If an event loop is running on THIS env, wake it up."
  [{::sc/keys [event-queue] ::keys [coord] :as env} event-or-request]
  (let [result   (sp/send! event-queue env event-or-request)
        delayed? (and (map? event-or-request)
                      (some? (:delay event-or-request))
                      (pos? (:delay event-or-request)))]
    (when (and coord (not delayed?))
      (coord/wake coord))
    result))

;; -----------------------------------------------------------------------------
;; Event loop
;; -----------------------------------------------------------------------------

(def ^:private ^java.lang.reflect.Method isClosed-method-for-class
  (memoize
    (fn [cls]
      (try (.getMethod ^Class cls "isClosed" (into-array Class []))
           (catch NoSuchMethodException _ nil)
           (catch Exception _ nil)))))

(defn- datasource-closed? [ds]
  (boolean
    (when ds
      (when-let [m (isClosed-method-for-class (class ds))]
        (try (.invoke ^java.lang.reflect.Method m ds (object-array 0))
             (catch Exception _ false))))))

(defn start-event-loop!
  ([env] (start-event-loop! env 100))
  ([env poll-interval-ms] (start-event-loop! env poll-interval-ms {}))
  ([{::sc/keys [event-queue] ::keys [node-id datasource coord] :as env}
    poll-interval-ms options]
   (let [running     (atom true)
         wake-signal (java.util.concurrent.LinkedBlockingQueue.)
         coord-live  (coord/attach-wake-signal coord wake-signal)
         env'        (-> env
                         (assoc ::coord coord-live)
                         (assoc ::sc/persistence-coordinator coord-live))
         handler     (fn [env event]
                       (ep/standard-statechart-event-handler env event))
         loop-fn (fn []
                   (log/info "Event loop started"
                             {:node-id node-id :poll-interval-ms poll-interval-ms
                              :session-filter (:session-id options)})
                   (while @running
                     (try
                       (if (datasource-closed? datasource)
                         (do (log/info "Datasource closed, stopping event loop" {:node-id node-id})
                             (reset! running false))
                         (sp/receive-events! event-queue env' handler options))
                       (catch Exception e
                         (log/error e "Event loop error" {:node-id node-id})))
                     (.poll wake-signal poll-interval-ms java.util.concurrent.TimeUnit/MILLISECONDS))
                   (log/info "Event loop stopped" {:node-id node-id}))]
     (future (loop-fn))
     {:env   env'
      :stop! (fn [] (reset! running false) (.offer wake-signal :stop))
      :wake! (fn [] (.offer wake-signal :wake))})))

;; -----------------------------------------------------------------------------
;; Maintenance
;; -----------------------------------------------------------------------------

(defn recover-stale-claims!
  ([env] (recover-stale-claims! env 30))
  ([{::keys [coord]} timeout-seconds]
   (coord/recover-stale-claims! coord timeout-seconds)))

(defn purge-processed-events!
  ([env] (purge-processed-events! env 7))
  ([env retention-days]
   (pg-eq/purge-processed-events! (::datasource env) retention-days)))

(defn purge-terminated-jobs!
  "Delete jobs terminated more than `retention-days` days ago whose terminal
   event has been dispatched. Cascades to job_results, job_leases (empty by
   then), terminal_dispatches."
  ([env] (purge-terminated-jobs! env 7))
  ([env retention-days]
   (let [retention-days (long retention-days)
         result (com.fulcrologic.statecharts.persistence.jdbc.core/execute-sql!
                  (::datasource env)
                  (str "DELETE FROM statechart_jobs"
                       " WHERE terminated_at < (now() - interval '" retention-days " days')"
                       "   AND EXISTS ("
                       "     SELECT 1 FROM statechart_terminal_dispatches td"
                       "     WHERE td.job_id = statechart_jobs.id)"))]
     (com.fulcrologic.statecharts.persistence.jdbc.core/affected-row-count result))))

(defn queue-depth
  ([env] (queue-depth env {}))
  ([env options]
   (pg-eq/queue-depth (::datasource env) options)))

;; No-op legacy cache fns (kept for back-compat)
(defn preload-registry-cache! [_env] nil)
(defn clear-registry-cache!   [_env] nil)
```

- [ ] **Step 2: Run full integration suite**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc`

Expected: most integration tests pass. `integration_test.clj` / `chaos_test.clj` / `regression_test.clj` likely fail because they haven't been updated yet — that's Phase 8.

- [ ] **Step 3: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc.clj
git commit -m "refactor(persistence): jdbc.clj wires coordinator + v3 retention fns"
```

---

## Phase 7: Migration function (v2 → v3)

### Task 7.1: Scaffold the migration namespace

**Files:**
- Create: `src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj`

- [ ] **Step 1: Create the scaffolding**

Write `src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj`:

```clojure
(ns com.fulcrologic.statecharts.persistence.jdbc.migrations.v2-to-v3
  "One-way migration from v2 to v3 schema.

   Operational contract: **workers MUST be quiesced** before calling `migrate!`.
   Not zero-downtime. Not dual-write-safe.

   All work runs in a single PostgreSQL transaction. Partial failure rolls
   back to untouched v2 state. The function is idempotent — safe to rerun
   after success (no-op) or after a crash (resumes). Each step uses `IF NOT
   EXISTS` / `ON CONFLICT DO NOTHING` / `DROP IF EXISTS` so re-running can't
   corrupt a half-migrated state.

   See docs/ai/specs/persistence-v3-coordinator.md for the design rationale."
  (:require
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.schema :as schema]
   [taoensso.timbre :as log]))

;; Declare step fns up front so `migrate!` below compiles. Each step fn lands
;; in a subsequent task.
(declare step-1-create-v3-tables!
         step-2-add-terminated-at!
         step-3-migrate-claims!
         step-4-migrate-leases!
         step-5-migrate-results!
         step-6-backfill-terminated-at!
         step-7-migrate-dispatches!
         step-8-drop-v2-columns!
         step-9-rebuild-indexes!)

(defn migrate!
  "Migrate a v2 schema to v3. Idempotent. See ns docstring."
  [datasource]
  (core/with-tx [tx datasource]
    (step-1-create-v3-tables!   tx)
    (step-2-add-terminated-at!  tx)
    (step-3-migrate-claims!     tx)
    (step-4-migrate-leases!     tx)
    (step-5-migrate-results!    tx)
    (step-6-backfill-terminated-at! tx)
    (step-7-migrate-dispatches! tx)
    (step-8-drop-v2-columns!    tx)
    (step-9-rebuild-indexes!    tx)
    (log/info "v2→v3 migration complete")
    true))
```

- [ ] **Step 2: Verify compile**

Run: `clojure -M:clj-tests unit --focus com.fulcrologic.statecharts.persistence.jdbc`

Expected: compiles. `migrate!` will throw if called because step fns are declared-but-undefined; we implement them in the next tasks.

- [ ] **Step 3: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj
git commit -m "scaffold(persistence): v2→v3 migration namespace"
```

### Task 7.2: Steps 1, 2, 3 (create tables, add terminated_at, migrate claims)

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj`
- Test: `src/test/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3_spec.clj` (new)

- [ ] **Step 1: Create the migration test with v2 fixtures**

Write `src/test/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3_spec.clj`:

```clojure
(ns ^:integration com.fulcrologic.statecharts.persistence.jdbc.migrations.v2-to-v3-spec
  "Integration tests for the v2→v3 schema migration.

   Seeds a v2-shaped database (no v3 tables), runs migrate!, asserts the v3
   shape is correct and data round-trips through v3 queries."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.fixtures :as fixtures :refer [*pool*]]
   [com.fulcrologic.statecharts.persistence.jdbc.migrations.v2-to-v3 :as migration])
  (:import
   [java.util UUID]))

(use-fixtures :once fixtures/with-pool)

(def ^:private v2-sessions-ddl
  "CREATE TABLE IF NOT EXISTS statechart_sessions (
    session_id     TEXT PRIMARY KEY,
    statechart_src TEXT NOT NULL,
    working_memory BYTEA NOT NULL,
    version        BIGINT NOT NULL DEFAULT 1,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now())")

(def ^:private v2-events-ddl
  "CREATE TABLE IF NOT EXISTS statechart_events (
    id                BIGSERIAL PRIMARY KEY,
    target_session_id TEXT NOT NULL,
    source_session_id TEXT,
    send_id           TEXT,
    invoke_id         TEXT,
    event_name        TEXT NOT NULL,
    event_type        TEXT DEFAULT 'external',
    event_data        BYTEA,
    deliver_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at        TIMESTAMPTZ,
    claimed_by        TEXT,
    processed_at      TIMESTAMPTZ)")

(def ^:private v2-jobs-ddl
  "CREATE TABLE IF NOT EXISTS statechart_jobs (
    id                           UUID PRIMARY KEY,
    session_id                   TEXT NOT NULL,
    invokeid                     TEXT NOT NULL,
    job_type                     TEXT NOT NULL,
    status                       TEXT NOT NULL DEFAULT 'pending',
    payload                      BYTEA NOT NULL,
    attempt                      INT NOT NULL DEFAULT 0,
    max_attempts                 INT NOT NULL DEFAULT 3,
    next_run_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner                  TEXT,
    lease_expires_at             TIMESTAMPTZ,
    result                       BYTEA,
    error                        BYTEA,
    terminal_event_name          TEXT,
    terminal_event_data          BYTEA,
    terminal_event_dispatched_at TIMESTAMPTZ,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now())")

(defn- drop-everything! [pool]
  (core/execute-sql! pool "DROP TABLE IF EXISTS statechart_terminal_dispatches CASCADE")
  (core/execute-sql! pool "DROP TABLE IF EXISTS statechart_job_results         CASCADE")
  (core/execute-sql! pool "DROP TABLE IF EXISTS statechart_job_leases          CASCADE")
  (core/execute-sql! pool "DROP TABLE IF EXISTS statechart_jobs                CASCADE")
  (core/execute-sql! pool "DROP TABLE IF EXISTS statechart_event_cancellations CASCADE")
  (core/execute-sql! pool "DROP TABLE IF EXISTS statechart_event_claims        CASCADE")
  (core/execute-sql! pool "DROP TABLE IF EXISTS statechart_events              CASCADE")
  (core/execute-sql! pool "DROP TABLE IF EXISTS statechart_sessions            CASCADE"))

(defn- seed-v2! [pool]
  (drop-everything! pool)
  (core/execute-sql! pool v2-sessions-ddl)
  (core/execute-sql! pool v2-events-ddl)
  (core/execute-sql! pool v2-jobs-ddl))

(defn- table-exists? [pool table]
  (some? (core/execute-sql-one! pool
           (str "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=?")
           [table])))

(defn- column-exists? [pool table col]
  (some? (core/execute-sql-one! pool
           (str "SELECT 1 FROM information_schema.columns"
                " WHERE table_schema='public' AND table_name=? AND column_name=?")
           [table col])))

(deftest ^:integration migrate-creates-all-v3-tables-test
  (seed-v2! *pool*)
  (migration/migrate! *pool*)
  (is (table-exists? *pool* "statechart_event_claims"))
  (is (table-exists? *pool* "statechart_event_cancellations"))
  (is (table-exists? *pool* "statechart_job_leases"))
  (is (table-exists? *pool* "statechart_job_results"))
  (is (table-exists? *pool* "statechart_terminal_dispatches"))
  (is (column-exists? *pool* "statechart_jobs" "terminated_at")))

(deftest ^:integration migrate-is-idempotent-test
  (seed-v2! *pool*)
  (migration/migrate! *pool*)
  (testing "second invocation is a no-op"
    (migration/migrate! *pool*))
  (is (table-exists? *pool* "statechart_event_claims")))

(deftest ^:integration migrate-splits-v2-event-claims-into-side-table-test
  (seed-v2! *pool*)
  (let [eid (:id (core/execute-sql-one! *pool*
                   (str "INSERT INTO statechart_events (target_session_id, event_name,"
                        "                               claimed_at, claimed_by, processed_at)"
                        " VALUES ('\"s-1\"', ':ping', now() - interval '5 seconds',"
                        "         'worker-a', now() - interval '2 seconds')"
                        " RETURNING id")))]
    (migration/migrate! *pool*)
    (let [claim (core/execute-sql-one! *pool*
                  "SELECT * FROM statechart_event_claims WHERE event_id = ?" [eid])]
      (is (= "worker-a" (:claimed-by claim)))
      (is (some? (:claimed-at claim)))
      (is (some? (:processed-at claim))))
    (testing "v2 claim columns are gone from statechart_events"
      (is (not (column-exists? *pool* "statechart_events" "claimed_at")))
      (is (not (column-exists? *pool* "statechart_events" "claimed_by")))
      (is (not (column-exists? *pool* "statechart_events" "processed_at"))))))
```

- [ ] **Step 2: Run to verify failures**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.migrations`

Expected: three tests fail (step fns throw because they're undefined).

- [ ] **Step 3: Implement steps 1, 2, 3**

Append to `migrations/v2_to_v3.clj`:

```clojure
;; -----------------------------------------------------------------------------
;; Step 1: Create v3 tables (idempotent)
;; -----------------------------------------------------------------------------

(defn step-1-create-v3-tables! [tx]
  (core/execute-sql! tx
    "CREATE TABLE IF NOT EXISTS statechart_event_claims (
       event_id     BIGINT PRIMARY KEY REFERENCES statechart_events(id) ON DELETE CASCADE,
       claimed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
       claimed_by   TEXT NOT NULL,
       processed_at TIMESTAMPTZ)")
  (core/execute-sql! tx
    "CREATE TABLE IF NOT EXISTS statechart_event_cancellations (
       event_id     BIGINT PRIMARY KEY REFERENCES statechart_events(id) ON DELETE CASCADE,
       cancelled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
       cancelled_by TEXT)")
  (core/execute-sql! tx
    "CREATE TABLE IF NOT EXISTS statechart_job_leases (
       job_id           UUID PRIMARY KEY REFERENCES statechart_jobs(id) ON DELETE CASCADE,
       attempt          INT NOT NULL DEFAULT 0,
       next_run_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
       lease_owner      TEXT,
       lease_expires_at TIMESTAMPTZ,
       attempt_error    BYTEA,
       updated_at       TIMESTAMPTZ NOT NULL DEFAULT now())")
  (core/execute-sql! tx
    "CREATE TABLE IF NOT EXISTS statechart_job_results (
       job_id       UUID PRIMARY KEY REFERENCES statechart_jobs(id) ON DELETE CASCADE,
       status       TEXT NOT NULL CHECK (status IN ('succeeded', 'failed', 'cancelled')),
       result       BYTEA,
       error        BYTEA,
       metadata     BYTEA,
       completed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
  (core/execute-sql! tx
    "CREATE TABLE IF NOT EXISTS statechart_terminal_dispatches (
       job_id        UUID PRIMARY KEY REFERENCES statechart_jobs(id) ON DELETE CASCADE,
       dispatched_at TIMESTAMPTZ NOT NULL DEFAULT now())"))

;; -----------------------------------------------------------------------------
;; Step 2: Add terminated_at column to statechart_jobs
;; -----------------------------------------------------------------------------

(defn step-2-add-terminated-at! [tx]
  (core/execute-sql! tx
    "ALTER TABLE statechart_jobs ADD COLUMN IF NOT EXISTS terminated_at TIMESTAMPTZ"))

;; -----------------------------------------------------------------------------
;; Step 3: Migrate v2 event claims into the side table
;; -----------------------------------------------------------------------------

(defn step-3-migrate-claims! [tx]
  (when (core/execute-sql-one! tx
          "SELECT 1 FROM information_schema.columns
           WHERE table_schema='public' AND table_name='statechart_events'
             AND column_name='claimed_at'")
    (core/execute-sql! tx
      "INSERT INTO statechart_event_claims (event_id, claimed_at, claimed_by, processed_at)
       SELECT id, claimed_at, COALESCE(claimed_by, 'legacy-migration'), processed_at
         FROM statechart_events
        WHERE claimed_at IS NOT NULL
       ON CONFLICT (event_id) DO NOTHING")))
```

- [ ] **Step 4: Run and verify the 3 migration tests pass**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.migrations`

Expected: all three tests pass. The column-dropping assertion in `migrate-splits-v2-event-claims-into-side-table-test` needs steps 8+9 — mark those tests `^:migration-steps-8-9` pending for now OR keep them failing and fix in Task 7.5.

If the third test fails because v2 event columns still exist: move on — this is expected until Task 7.5.

- [ ] **Step 5: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj \
        src/test/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3_spec.clj
git commit -m "feat(persistence): migration steps 1-3 (create tables, migrate event claims)"
```

### Task 7.3: Steps 4, 5, 6 (migrate job leases, results, terminated_at)

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj`
- Modify: `src/test/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3_spec.clj`

- [ ] **Step 1: Append tests**

Append to the migration spec:

```clojure
(deftest ^:integration migrate-splits-v2-running-jobs-into-leases-test
  (seed-v2! *pool*)
  (let [job-id (UUID/randomUUID)]
    (core/execute-sql! *pool*
      (str "INSERT INTO statechart_jobs"
           " (id, session_id, invokeid, job_type, status, payload,"
           "  attempt, max_attempts, next_run_at, lease_owner, lease_expires_at)"
           " VALUES (?, ?, ?, 'http', 'running', ?,"
           "  2, 3, now(), 'worker-a', now() + interval '30 seconds')")
      [job-id "\"s-1\"" "i-1" (core/freeze {:url "x"})])
    (migration/migrate! *pool*)
    (let [lease (core/execute-sql-one! *pool*
                  "SELECT * FROM statechart_job_leases WHERE job_id = ?" [job-id])]
      (is (= 2 (:attempt lease)))
      (is (= "worker-a" (:lease-owner lease))))))

(deftest ^:integration migrate-splits-v2-succeeded-jobs-into-results-test
  (seed-v2! *pool*)
  (let [job-id (UUID/randomUUID)]
    (core/execute-sql! *pool*
      (str "INSERT INTO statechart_jobs"
           " (id, session_id, invokeid, job_type, status, payload,"
           "  attempt, max_attempts, result, updated_at)"
           " VALUES (?, ?, ?, 'http', 'succeeded', ?,"
           "  1, 3, ?, now() - interval '10 seconds')")
      [job-id "\"s-1\"" "i-1" (core/freeze {}) (core/freeze {:ok 42})])
    (migration/migrate! *pool*)
    (let [jr (core/execute-sql-one! *pool*
               "SELECT * FROM statechart_job_results WHERE job_id = ?" [job-id])
          jj (core/execute-sql-one! *pool*
               "SELECT * FROM statechart_jobs WHERE id = ?" [job-id])]
      (is (= "succeeded" (:status jr)))
      (is (= {:ok 42} (core/thaw (:result jr))))
      (is (some? (:terminated-at jj)) "terminated_at backfilled"))))

(deftest ^:integration migrate-splits-v2-cancelled-jobs-with-metadata-test
  (seed-v2! *pool*)
  (let [job-id (UUID/randomUUID)]
    (core/execute-sql! *pool*
      (str "INSERT INTO statechart_jobs"
           " (id, session_id, invokeid, job_type, status, payload, updated_at)"
           " VALUES (?, ?, ?, 'http', 'cancelled', ?, now() - interval '5 seconds')")
      [job-id "\"s-1\"" "i-1" (core/freeze {})])
    (migration/migrate! *pool*)
    (let [jr (core/execute-sql-one! *pool*
               "SELECT * FROM statechart_job_results WHERE job_id = ?" [job-id])]
      (is (= "cancelled" (:status jr)))
      (is (= {:reason :cancelled} (core/thaw (:metadata jr)))))))
```

- [ ] **Step 2: Run to verify failures**

Expected: new tests fail.

- [ ] **Step 3: Append implementation**

```clojure
;; -----------------------------------------------------------------------------
;; Step 4: Migrate pending/running v2 jobs into statechart_job_leases
;; -----------------------------------------------------------------------------

(defn step-4-migrate-leases! [tx]
  (when (core/execute-sql-one! tx
          "SELECT 1 FROM information_schema.columns
           WHERE table_schema='public' AND table_name='statechart_jobs'
             AND column_name='status'")
    (core/execute-sql! tx
      "INSERT INTO statechart_job_leases
           (job_id, attempt, next_run_at, lease_owner, lease_expires_at, attempt_error, updated_at)
       SELECT id, attempt, next_run_at, lease_owner, lease_expires_at, error, updated_at
         FROM statechart_jobs
        WHERE status IN ('pending', 'running')
       ON CONFLICT (job_id) DO NOTHING")))

;; -----------------------------------------------------------------------------
;; Step 5: Migrate terminal v2 jobs into statechart_job_results
;; -----------------------------------------------------------------------------

(def ^:private cancelled-metadata-bytes
  (delay (core/freeze {:reason :cancelled})))

(defn step-5-migrate-results! [tx]
  (when (core/execute-sql-one! tx
          "SELECT 1 FROM information_schema.columns
           WHERE table_schema='public' AND table_name='statechart_jobs'
             AND column_name='status'")
    ;; One row at a time because Clojure-side nippy is needed for the metadata
    ;; column on cancelled rows. Volumes here are at most the size of the
    ;; terminal-job table, bounded by `purge-terminated-jobs!` retention.
    (let [rows (core/execute-sql! tx
                 "SELECT id, status, result, error, updated_at
                    FROM statechart_jobs
                   WHERE status IN ('succeeded', 'failed', 'cancelled')")]
      (doseq [{:keys [id status result error updated-at]} rows]
        (core/execute-sql! tx
          (str "INSERT INTO statechart_job_results"
               " (job_id, status, result, error, metadata, completed_at)"
               " VALUES (?, ?, ?, ?, ?, ?)"
               " ON CONFLICT (job_id) DO NOTHING")
          [id status result error
           (when (= status "cancelled") @cancelled-metadata-bytes)
           updated-at])))))

;; -----------------------------------------------------------------------------
;; Step 6: Backfill terminated_at on statechart_jobs
;; -----------------------------------------------------------------------------

(defn step-6-backfill-terminated-at! [tx]
  (when (core/execute-sql-one! tx
          "SELECT 1 FROM information_schema.columns
           WHERE table_schema='public' AND table_name='statechart_jobs'
             AND column_name='status'")
    (core/execute-sql! tx
      "UPDATE statechart_jobs
          SET terminated_at = updated_at
        WHERE status IN ('succeeded', 'failed', 'cancelled')
          AND terminated_at IS NULL")))
```

- [ ] **Step 4: Run and verify all migration tests pass (except column-drop expectations)**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.migrations`

Expected: tests for leases/results pass. Tests that check `not (column-exists?)` still fail — that's step 8 in Task 7.5.

- [ ] **Step 5: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj \
        src/test/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3_spec.clj
git commit -m "feat(persistence): migration steps 4-6 (leases, results, terminated_at)"
```

### Task 7.4: Step 7 — terminal dispatches + golden-file check

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj`
- Modify: `src/test/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3_spec.clj`

- [ ] **Step 1: Append tests including golden-file check**

```clojure
(deftest ^:integration migrate-carries-over-terminal-dispatched-at-test
  (seed-v2! *pool*)
  (let [job-id (UUID/randomUUID)]
    (core/execute-sql! *pool*
      (str "INSERT INTO statechart_jobs"
           " (id, session_id, invokeid, job_type, status, payload,"
           "  terminal_event_dispatched_at)"
           " VALUES (?, ?, ?, 'http', 'succeeded', ?, now() - interval '1 minute')")
      [job-id "\"s-1\"" "i-1" (core/freeze {})])
    (migration/migrate! *pool*)
    (is (some? (core/execute-sql-one! *pool*
                 "SELECT 1 FROM statechart_terminal_dispatches WHERE job_id = ?"
                 [job-id])))))

(deftest ^:integration migrate-golden-file-terminal-event-shape-test
  (testing "for every v2 terminal job, v3's computed terminal event matches what v2 stored"
    (seed-v2! *pool*)
    (let [j1 (UUID/randomUUID)
          j2 (UUID/randomUUID)
          j3 (UUID/randomUUID)]
      (doseq [[id status invokeid payload-shape stored-name stored-data]
              [[j1 "succeeded" ":invoke-1" {:r 42} ":done.invoke.invoke-1"  {:r 42}]
               [j2 "failed"    ":invoke-2" {:e "x"} ":error.invoke.invoke-2" {:e "x"}]
               [j3 "cancelled" ":invoke-3" {}     ":error.invoke.invoke-3" {:reason :cancelled}]]]
        (core/execute-sql! *pool*
          (str "INSERT INTO statechart_jobs"
               " (id, session_id, invokeid, job_type, status, payload,"
               "  result, error, terminal_event_name, terminal_event_data)"
               " VALUES (?, ?, ?, 'http', ?, ?, ?, ?, ?, ?)")
          [id "\"s-1\"" invokeid status (core/freeze {})
           (when (= status "succeeded") (core/freeze payload-shape))
           (when (= status "failed")    (core/freeze payload-shape))
           stored-name
           (core/freeze stored-data)]))
      ;; Snapshot the v2 stored values before migration.
      (let [pre (core/execute-sql! *pool*
                  "SELECT id, terminal_event_name, terminal_event_data FROM statechart_jobs ORDER BY id")]
        (migration/migrate! *pool*)
        (doseq [{:keys [id terminal-event-name terminal-event-data]} pre]
          (let [jr (core/execute-sql-one! *pool*
                     "SELECT jr.*, j.invokeid FROM statechart_job_results jr
                        JOIN statechart_jobs j ON j.id = jr.job_id
                       WHERE j.id = ?" [id])
                v3-name (com.fulcrologic.statecharts.persistence.jdbc.job-store/terminal-event-name
                          (keyword (:status jr))
                          (com.fulcrologic.statecharts.persistence.jdbc.job-store/str->invokeid (:invokeid jr)))
                v3-data (com.fulcrologic.statecharts.persistence.jdbc.job-store/terminal-event-data
                          (keyword (:status jr))
                          (some-> (:result jr) core/thaw)
                          (some-> (:error jr) core/thaw)
                          (some-> (:metadata jr) core/thaw))]
            (is (= terminal-event-name v3-name)
                (str "terminal event name mismatch for job " id))
            (is (= (core/thaw terminal-event-data) v3-data)
                (str "terminal event data mismatch for job " id))))))))
```

- [ ] **Step 2: Run to verify failures**

Expected: golden-file and dispatches tests fail.

- [ ] **Step 3: Implement step 7**

```clojure
;; -----------------------------------------------------------------------------
;; Step 7: Carry over terminal_event_dispatched_at
;; -----------------------------------------------------------------------------

(defn step-7-migrate-dispatches! [tx]
  (when (core/execute-sql-one! tx
          "SELECT 1 FROM information_schema.columns
           WHERE table_schema='public' AND table_name='statechart_jobs'
             AND column_name='terminal_event_dispatched_at'")
    (core/execute-sql! tx
      "INSERT INTO statechart_terminal_dispatches (job_id, dispatched_at)
       SELECT id, terminal_event_dispatched_at
         FROM statechart_jobs
        WHERE terminal_event_dispatched_at IS NOT NULL
       ON CONFLICT (job_id) DO NOTHING")))
```

- [ ] **Step 4: Run**

Expected: dispatches test passes. Golden-file test should also pass because `job-store/terminal-event-name` and `terminal-event-data` were defined in Task 5.5 and match v2's stored shape for all three statuses.

- [ ] **Step 5: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj \
        src/test/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3_spec.clj
git commit -m "feat(persistence): migration step 7 + golden-file check for terminal events"
```

### Task 7.5: Steps 8, 9 (drop v2 columns, rebuild indexes)

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj`

- [ ] **Step 1: Implement steps 8 and 9**

Append to `migrations/v2_to_v3.clj`:

```clojure
;; -----------------------------------------------------------------------------
;; Step 8: Drop v2 columns
;; -----------------------------------------------------------------------------

(defn step-8-drop-v2-columns! [tx]
  (doseq [col ["claimed_at" "claimed_by" "processed_at"]]
    (core/execute-sql! tx
      (str "ALTER TABLE statechart_events DROP COLUMN IF EXISTS " col)))
  (doseq [col ["status" "attempt" "next_run_at" "lease_owner" "lease_expires_at"
               "result" "error" "terminal_event_name" "terminal_event_data"
               "terminal_event_dispatched_at" "updated_at"]]
    (core/execute-sql! tx
      (str "ALTER TABLE statechart_jobs DROP COLUMN IF EXISTS " col))))

;; -----------------------------------------------------------------------------
;; Step 9: Drop v2 indexes, create v3 indexes
;; -----------------------------------------------------------------------------

(defn step-9-rebuild-indexes! [tx]
  (doseq [idx ["idx_events_target_deliver"
               "idx_events_cancel"
               "idx_events_claimed"
               "idx_events_unclaimed_deliver"
               "idx_jobs_active_per_invoke"  ;; v2 version — v3 redefines
               "idx_jobs_claimable"
               "idx_jobs_session"
               "idx_jobs_undispatched"]]
    (core/execute-sql! tx (str "DROP INDEX IF EXISTS " idx)))
  (doseq [ddl ["CREATE INDEX IF NOT EXISTS idx_events_deliver_at ON statechart_events(deliver_at, id)"
               "CREATE INDEX IF NOT EXISTS idx_events_cancel     ON statechart_events(source_session_id, send_id, deliver_at)"
               "CREATE INDEX IF NOT EXISTS idx_events_target     ON statechart_events(target_session_id, deliver_at)"
               "CREATE INDEX IF NOT EXISTS idx_event_claims_recovery
                  ON statechart_event_claims(claimed_at)
                  WHERE processed_at IS NULL"
               "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_per_invoke
                  ON statechart_jobs(session_id, invokeid)
                  WHERE terminated_at IS NULL"
               "CREATE INDEX IF NOT EXISTS idx_jobs_session
                  ON statechart_jobs(session_id, created_at DESC)"
               "CREATE INDEX IF NOT EXISTS idx_leases_claimable
                  ON statechart_job_leases(next_run_at, lease_expires_at)"
               "CREATE INDEX IF NOT EXISTS idx_results_completed_at
                  ON statechart_job_results(completed_at)"]]
    (core/execute-sql! tx ddl)))
```

- [ ] **Step 2: Run full migration spec**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.migrations`

Expected: all migration tests pass, including the earlier "column-exists" negations in `migrate-splits-v2-event-claims-into-side-table-test`.

- [ ] **Step 3: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/migrations/v2_to_v3.clj
git commit -m "feat(persistence): migration steps 8-9 (drop v2 columns, rebuild indexes)"
```

---

## Phase 8: Update existing tests

### Task 8.1: Update `core_spec.clj`

**Files:** `src/test/com/fulcrologic/statecharts/persistence/jdbc/core_spec.clj`

- [ ] **Step 1: Run the spec and note failures**

Run: `clojure -M:clj-tests unit --focus com.fulcrologic.statecharts.persistence.jdbc.core-spec`

- [ ] **Step 2: The spec tests encode-id/decode-id/freeze/thaw/version metadata — none of which changed in v3.**

If any test fails, the failure is most likely about an integration-style test (`SQL Execution Functions` section, which uses a live DB) that was formerly in the unit spec. Move any `^:integration` deftests to `integration_test.clj` or mark them correctly.

If the spec passes as-is, no changes needed.

- [ ] **Step 3: If changes are needed, commit them**

```bash
git add src/test/com/fulcrologic/statecharts/persistence/jdbc/core_spec.clj
git commit -m "test(persistence): adjust core_spec for v3 schema"
```

### Task 8.2: Update `integration_test.clj`

**Files:** `src/test/com/fulcrologic/statecharts/persistence/jdbc/integration_test.clj`

- [ ] **Step 1: Run it**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.integration-test`

- [ ] **Step 2: Remove any helper that names v2-only columns**

Grep for these patterns in the test file and replace them:

```bash
grep -n "claimed_at\|claimed_by\|processed_at\|lease_owner\|terminal_event_name\|terminal_event_data" \
  src/test/com/fulcrologic/statecharts/persistence/jdbc/integration_test.clj
```

Typical replacements:
- Reads of `statechart_events.claimed_at` → join on `statechart_event_claims`.
- Reads of `statechart_jobs.status` → derived (see `job-store/get-active-job` or compute via LEFT JOIN).
- Direct writes to `statechart_jobs.lease_owner` → update `statechart_job_leases.lease_owner`.

- [ ] **Step 3: Rerun until green**

Fix each failure individually. The test intent stays the same; only the schema shape of the assertions/setups changes.

- [ ] **Step 4: Commit**

```bash
git add src/test/com/fulcrologic/statecharts/persistence/jdbc/integration_test.clj
git commit -m "test(persistence): integration_test adapted to v3 schema"
```

### Task 8.3: Update `chaos_test.clj`

**Files:** `src/test/com/fulcrologic/statecharts/persistence/jdbc/chaos_test.clj`

- [ ] **Step 1: Run, identify failures**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.chaos-test`

- [ ] **Step 2: Replace schema references**

The chaos test stresses concurrent claim/ack. Its setup probably sets `claimed_at`/`claimed_by`/`processed_at` directly on `statechart_events`. Move those to `statechart_event_claims` inserts.

- [ ] **Step 3: Rerun until green; commit**

```bash
git add src/test/com/fulcrologic/statecharts/persistence/jdbc/chaos_test.clj
git commit -m "test(persistence): chaos_test adapted to v3 schema"
```

### Task 8.4: Update `regression_test.clj`

**Files:** `src/test/com/fulcrologic/statecharts/persistence/jdbc/regression_test.clj`

- [ ] **Step 1: Run, identify failures**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.regression-test`

- [ ] **Step 2: For each failing test, decide:**

- **Regression still expressible in v3** → rewrite the setup/assertion using v3 schema.
- **Regression tied to v2 column that no longer exists** (e.g., `terminal_event_name`) → remove the test and add a line to CHANGELOG noting the test was removed.

- [ ] **Step 3: Commit**

```bash
git add src/test/com/fulcrologic/statecharts/persistence/jdbc/regression_test.clj
git commit -m "test(persistence): regression_test adapted to v3; v2-specific tests removed"
```

### Task 8.5: Update `id_property_test.clj` and `fixtures.clj`

**Files:**
- `src/test/com/fulcrologic/statecharts/persistence/jdbc/id_property_test.clj`
- `src/test/com/fulcrologic/statecharts/persistence/jdbc/fixtures.clj`

- [ ] **Step 1: Run both**

Run: `clojure -M:clj-tests integration --focus com.fulcrologic.statecharts.persistence.jdbc.id-property-test` and `clojure -M:clj-tests unit` separately.

- [ ] **Step 2: `id_property_test` exercises id encoding, which v3 did not change.** Expect it to pass unchanged.

- [ ] **Step 3: `fixtures.clj` references `schema/create-tables!` and `schema/truncate-tables!` which are unchanged in public API.** Expect it to work unchanged.

- [ ] **Step 4: If anything broke, commit the fix**

```bash
git add src/test/com/fulcrologic/statecharts/persistence/jdbc/{id_property_test.clj,fixtures.clj}
git commit -m "test(persistence): fixtures + id-property adjust for v3 (if needed)"
```

### Task 8.6: Full test-suite green run

- [ ] **Step 1: Run unit tests**

Run: `clojure -M:clj-tests unit`

Expected: all pass.

- [ ] **Step 2: Run integration tests**

Run: `clojure -M:clj-tests integration`

Expected: all pass (requires live PG).

- [ ] **Step 3: If anything fails, address it before proceeding to Phase 9.**

This is a checkpoint, not a commit task. Proceed only when green.

---

## Phase 9: Docs + release

### Task 9.1: Update persistence README with v3 usage and migration guide

**Files:**
- Modify: `src/main/com/fulcrologic/statecharts/persistence/jdbc/README.md`

- [ ] **Step 1: Read the current README**

Run: `wc -l src/main/com/fulcrologic/statecharts/persistence/jdbc/README.md`

Read it fully (it's ~19k; the high-level structure is what matters).

- [ ] **Step 2: Add a v3 section at the top**

Prepend to the README a section with:

```markdown
## v3 (3.0.0): FRP-aligned schema

Version 3 separates coordination state from domain state. The key differences
from v2:

- Events (`statechart_events`) are append-only. Claims live in
  `statechart_event_claims`; cancellations in `statechart_event_cancellations`.
- Jobs (`statechart_jobs`) are append-only; coordination moves to
  `statechart_job_leases` (in-flight), `statechart_job_results` (terminal), and
  `statechart_terminal_dispatches` (dispatch claim).
- A new `PersistenceCoordinator` protocol owns the ACK-on-save transactional
  invariant; replaces v2's env-threaded `::sc/on-save-hooks` / `::sc/save-
  attempted?` / `::wake-signal`.

### Migrating from 2.x

v2 → v3 is a **one-shot, quiesce-required** migration. Not zero-downtime.

    (require '[com.fulcrologic.statecharts.persistence.jdbc.migrations.v2-to-v3 :as m])
    ;; 1. Quiesce all workers (stop event loops).
    ;; 2. Run:
    (m/migrate! datasource)
    ;; 3. Deploy v3 code and restart workers.

The migration runs in one transaction; partial failure rolls back. Idempotent,
so rerunning after a crash resumes cleanly. See
`docs/ai/specs/persistence-v3-coordinator.md` for the full design.

### Behavior changes worth noting

- `<cancel>` inserts into `statechart_event_cancellations` instead of
  deleting the event row. Cancelled events persist in `statechart_events`
  until `purge-processed-events!` retention trims them (after their claim
  cycle). Operators querying `statechart_events` should filter via
  cancellation join if "only live events" is the goal.
- Per-attempt retry errors live in `statechart_job_leases.attempt_error`
  (same overwrite-on-retry behavior as v2 `statechart_jobs.error`).
- `terminal_event_name` and `terminal_event_data` are no longer stored;
  computed at dispatch time from `invokeid` + `statechart_job_results.status`
  and payload columns.
- `create-job!` no longer has a race-retry loop. The partial-unique index
  plus one tx gives deterministic dedup.
```

- [ ] **Step 3: Commit**

```bash
git add src/main/com/fulcrologic/statecharts/persistence/jdbc/README.md
git commit -m "docs(persistence): README v3 overview + migration guide"
```

### Task 9.2: CHANGELOG entry for 3.0.0

**Files:**
- Modify: `CHANGELOG`

- [ ] **Step 1: Prepend a 3.0.0 entry**

Prepend to the `CHANGELOG` file:

```
3.0.0
-----

**BREAKING**: Persistence layer schema v3 replaces v2. See
`docs/ai/specs/persistence-v3-coordinator.md` and
`src/main/com/fulcrologic/statecharts/persistence/jdbc/README.md#migrating-from-2x`
for the migration path.

Structural changes:

* **Events are append-only**. Claim/processing state moves from columns on
  `statechart_events` (`claimed_at`/`claimed_by`/`processed_at`) to a
  side table `statechart_event_claims`.
* **Cancellation is a fact** in `statechart_event_cancellations`
  (instead of v2's `DELETE`). SCXML semantics unchanged; cancelled rows
  persist until retention trims them.
* **Jobs are append-only**. Coordination splits into `statechart_job_leases`
  (in-flight/retry), `statechart_job_results` (terminal outcomes),
  `statechart_terminal_dispatches` (dispatch claim). `statechart_jobs`
  keeps only domain columns plus a denormalized `terminated_at` column
  (written in the same transaction as the matching `job_results` row) so
  partial unique indexes work.
* **`PersistenceCoordinator`** — new protocol replacing v2's
  `::sc/on-save-hooks` / `::sc/save-attempted?` / `::wake-signal` env
  keys. Threaded on env as `::sc/persistence-coordinator`. `EventQueue`
  and `WorkingMemoryStore` protocol signatures are unchanged; only env
  keys differ.
* **Derived values no longer stored**: `terminal_event_name` and
  `terminal_event_data` on jobs are computed at dispatch time from
  `invokeid` + `job_results.status`/`.result`/`.error`/`.metadata`.
* **Race-retry loop in `create-job!` removed**. The v2 five-attempt
  (INSERT → SELECT) retry loop under adversarial concurrency is gone;
  the partial-unique index plus one tx gives deterministic dedup.

Migration:

* `com.fulcrologic.statecharts.persistence.jdbc.migrations.v2-to-v3/migrate!`
  performs a one-shot migration in a single transaction. Idempotent and
  safe to rerun. **Requires workers to be quiesced first**; not dual-write
  compatible.

Non-JDBC backends (in-memory working-memory store, manually-polled queue,
core.async event loop) are unchanged.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG
git commit -m "docs: CHANGELOG entry for 3.0.0 (v3 persistence)"
```

### Task 9.3: Final smoke test and tag

- [ ] **Step 1: Full test suite**

```bash
clojure -M:clj-tests unit && clojure -M:clj-tests integration
```

Expected: all pass.

- [ ] **Step 2: Inspect the diff vs main**

```bash
git log main..HEAD --oneline
git diff main..HEAD --stat
```

Expected: only files listed in the "File layout" section of the spec are touched.

- [ ] **Step 3: (Optional — coordinate with maintainer before tagging)**

Tagging is a release action — do NOT perform it as part of plan execution unless the maintainer has explicitly requested it. If requested:

```bash
git tag -a 3.0.0 -m "Persistence v3 release"
git push origin v3-persistence
git push origin 3.0.0
```

Otherwise stop here and hand off the branch for review/PR.

---

## Self-review

**Spec coverage check (manual walk):**
- ✅ Schema v3 with all 8 tables (sessions, events, event_claims, event_cancellations, jobs, job_leases, job_results, terminal_dispatches) — Task 1.1, pinned by Task 1.2.
- ✅ Derived-state discipline including `terminated_at` denormalization — Task 1.1 + Task 5.3 (write-result-and-terminate! writes both in one tx).
- ✅ PersistenceCoordinator protocol + NoopCoordinator + JdbcCoordinator — Tasks 2.1–2.3.
- ✅ Event queue rewrite with v3 claim/cancel/receive — Tasks 3.1–3.2.
- ✅ WM store rewrite routing through coordinator — Task 4.1.
- ✅ Job store rewrite: create/claim/heartbeat/complete/fail/cancel/cancel-by-session/dispatch-claim/derived terminal event name — Tasks 5.1–5.6.
- ✅ jdbc.clj wiring + `purge-terminated-jobs!` retention — Task 6.1.
- ✅ Migration function with 9 steps + golden-file check — Tasks 7.1–7.5.
- ✅ Test migrations across 7 specs — Tasks 8.1–8.5.
- ✅ README v3 section + CHANGELOG 3.0.0 — Tasks 9.1–9.2.
- ✅ SCXML conformance analysis — documented in the spec; no code required.

**Placeholder scan:** none of "TBD", "TODO", "implement later", "fill in details", "add appropriate error handling", "similar to Task N" appear in the task steps. All code blocks are complete (not abbreviated).

**Type consistency check:**
- `coord/new-jdbc-coordinator` used in Tasks 2.3, 3.2, 4.1, 5.x, 6.1 — consistent.
- `coord/new-noop-coordinator` used in Task 2.2 — consistent.
- `sut/invokeid->str` / `sut/str->invokeid` (in spec) references match `job-store/invokeid->str` / `str->invokeid` (in src) — Task 5.1 defines them.
- `sut/terminal-event-name` / `sut/terminal-event-data` — Task 5.5 defines them with these names.
- `write-result-and-terminate!` — defined in Task 5.3, called by 5.4's `cancel!` and `cancel-by-session-in-tx!`. Consistent.
- The migration's `step-N-*!` fn names match between `declare` (Task 7.1) and `defn` (Tasks 7.2–7.5).
- `::sc/persistence-coordinator` and `::sc/current-event-handle` env keys are consistent across event_queue (Task 3.1), WM store (Task 4.1), and jdbc.clj (Task 6.1).
