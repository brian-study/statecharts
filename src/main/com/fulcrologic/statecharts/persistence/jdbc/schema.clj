(ns com.fulcrologic.statecharts.persistence.jdbc.schema
  "Database schema DDL for statechart persistence.

   Provides functions to create and drop the required tables."
  (:require
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]))

;; -----------------------------------------------------------------------------
;; Table Creation DDL
;; -----------------------------------------------------------------------------

(def ^:private sessions-ddl
  "CREATE TABLE IF NOT EXISTS statechart_sessions (
    session_id       TEXT PRIMARY KEY,
    statechart_src   TEXT NOT NULL,
    working_memory   BYTEA NOT NULL,
    version          BIGINT NOT NULL DEFAULT 1,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
  )")

(def ^:private sessions-indexes-ddl
  ["CREATE INDEX IF NOT EXISTS idx_sessions_statechart_src ON statechart_sessions(statechart_src)"
   "CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON statechart_sessions(updated_at)"])

(def ^:private events-ddl
  "CREATE TABLE IF NOT EXISTS statechart_events (
    id                  BIGSERIAL PRIMARY KEY,
    target_session_id   TEXT NOT NULL,
    source_session_id   TEXT,
    send_id             TEXT,
    invoke_id           TEXT,
    event_name          TEXT NOT NULL,
    event_type          TEXT DEFAULT 'external',
    event_data          BYTEA,
    deliver_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at          TIMESTAMPTZ,
    claimed_by          TEXT,
    processed_at        TIMESTAMPTZ
  )")

(def ^:private events-indexes-ddl
  ["CREATE INDEX IF NOT EXISTS idx_events_target_deliver ON statechart_events(target_session_id, deliver_at) WHERE processed_at IS NULL"
   "CREATE INDEX IF NOT EXISTS idx_events_cancel ON statechart_events(source_session_id, send_id, deliver_at) WHERE processed_at IS NULL"
   "CREATE INDEX IF NOT EXISTS idx_events_claimed ON statechart_events(claimed_at) WHERE claimed_at IS NOT NULL AND processed_at IS NULL"
   ;; Supports the unfiltered `claim-events!` path (no :session-id option).
   ;; `idx_events_target_deliver` leads with `target_session_id`, so a
   ;; worker polling without a session filter has to scan it; a partial
   ;; index on `(deliver_at, id)` over unclaimed pending rows lets the
   ;; order-by run as an index scan.
   "CREATE INDEX IF NOT EXISTS idx_events_unclaimed_deliver ON statechart_events(deliver_at, id) WHERE processed_at IS NULL AND claimed_at IS NULL"])

(def ^:private jobs-ddl
  "CREATE TABLE IF NOT EXISTS statechart_jobs (
    id                          UUID PRIMARY KEY,
    session_id                  TEXT NOT NULL,
    invokeid                    TEXT NOT NULL,
    job_type                    TEXT NOT NULL,
    status                      TEXT NOT NULL DEFAULT 'pending',
    payload                     BYTEA NOT NULL,
    attempt                     INT NOT NULL DEFAULT 0,
    max_attempts                INT NOT NULL DEFAULT 3,
    next_run_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner                 TEXT,
    lease_expires_at            TIMESTAMPTZ,
    result                      BYTEA,
    error                       BYTEA,
    terminal_event_name         TEXT,
    terminal_event_data         BYTEA,
    terminal_event_dispatched_at TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
  )")

(def ^:private jobs-indexes-ddl
  ["CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_per_invoke
      ON statechart_jobs (session_id, invokeid)
      WHERE status IN ('pending', 'running')"
   "CREATE INDEX IF NOT EXISTS idx_jobs_claimable
      ON statechart_jobs (status, next_run_at, lease_expires_at)
      WHERE status IN ('pending', 'running')"
   "CREATE INDEX IF NOT EXISTS idx_jobs_session
      ON statechart_jobs (session_id, created_at DESC)"
   "CREATE INDEX IF NOT EXISTS idx_jobs_undispatched
      ON statechart_jobs (status)
      WHERE terminal_event_dispatched_at IS NULL AND status IN ('succeeded', 'failed', 'cancelled')"])

;; -----------------------------------------------------------------------------
;; Table Drop DDL
;; -----------------------------------------------------------------------------

(def ^:private drop-sessions-ddl "DROP TABLE IF EXISTS statechart_sessions CASCADE")
(def ^:private drop-events-ddl "DROP TABLE IF EXISTS statechart_events CASCADE")
(def ^:private drop-jobs-ddl "DROP TABLE IF EXISTS statechart_jobs CASCADE")
;; Legacy table kept out of create-tables! — JdbcStatechartRegistry was
;; deleted in 2.0.10. drop/truncate still name it so existing deployments
;; can clean up without a manual migration.
(def ^:private drop-legacy-definitions-ddl "DROP TABLE IF EXISTS statechart_definitions CASCADE")

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn create-tables!
  "Create all statechart persistence tables and indexes.
   Safe to call multiple times (uses IF NOT EXISTS)."
  [ds-or-conn]
  (core/execute-sql! ds-or-conn sessions-ddl)
  (doseq [idx sessions-indexes-ddl]
    (core/execute-sql! ds-or-conn idx))
  (core/execute-sql! ds-or-conn events-ddl)
  (doseq [idx events-indexes-ddl]
    (core/execute-sql! ds-or-conn idx))
  (core/execute-sql! ds-or-conn jobs-ddl)
  (doseq [idx jobs-indexes-ddl]
    (core/execute-sql! ds-or-conn idx))
  true)

(defn drop-tables!
  "Drop all statechart persistence tables.
   WARNING: This will delete all data! Also drops the legacy
   `statechart_definitions` table from pre-2.0.10 deployments for cleanup."
  [ds-or-conn]
  (core/execute-sql! ds-or-conn drop-jobs-ddl)
  (core/execute-sql! ds-or-conn drop-events-ddl)
  (core/execute-sql! ds-or-conn drop-sessions-ddl)
  (core/execute-sql! ds-or-conn drop-legacy-definitions-ddl)
  true)

(defn truncate-tables!
  "Truncate all statechart persistence tables.
   Removes all data but keeps table structure."
  [ds-or-conn]
  (core/execute-sql! ds-or-conn
    "TRUNCATE statechart_events, statechart_sessions, statechart_jobs RESTART IDENTITY CASCADE")
  true)
