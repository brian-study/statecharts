(ns com.fulcrologic.statecharts.persistence.pg.schema
  "Database schema DDL for statechart persistence.

   Provides functions to create and drop the required tables."
  (:require
   [com.fulcrologic.statecharts.persistence.pg.core :as core]))

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
   "CREATE INDEX IF NOT EXISTS idx_events_cancel ON statechart_events(source_session_id, send_id) WHERE processed_at IS NULL AND deliver_at > now()"
   "CREATE INDEX IF NOT EXISTS idx_events_claimed ON statechart_events(claimed_at) WHERE claimed_at IS NOT NULL AND processed_at IS NULL"])

(def ^:private definitions-ddl
  "CREATE TABLE IF NOT EXISTS statechart_definitions (
    src              TEXT PRIMARY KEY,
    definition       BYTEA NOT NULL,
    version          BIGINT NOT NULL DEFAULT 1,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
  )")

;; -----------------------------------------------------------------------------
;; Table Drop DDL
;; -----------------------------------------------------------------------------

(def ^:private drop-sessions-ddl "DROP TABLE IF EXISTS statechart_sessions CASCADE")
(def ^:private drop-events-ddl "DROP TABLE IF EXISTS statechart_events CASCADE")
(def ^:private drop-definitions-ddl "DROP TABLE IF EXISTS statechart_definitions CASCADE")

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn create-tables!
  "Create all statechart persistence tables and indexes.
   Safe to call multiple times (uses IF NOT EXISTS)."
  [conn-or-pool]
  (core/execute! conn-or-pool {:raw sessions-ddl})
  (doseq [idx sessions-indexes-ddl]
    (core/execute! conn-or-pool {:raw idx}))
  (core/execute! conn-or-pool {:raw events-ddl})
  (doseq [idx events-indexes-ddl]
    (core/execute! conn-or-pool {:raw idx}))
  (core/execute! conn-or-pool {:raw definitions-ddl})
  true)

(defn drop-tables!
  "Drop all statechart persistence tables.
   WARNING: This will delete all data!"
  [conn-or-pool]
  (core/execute! conn-or-pool {:raw drop-events-ddl})
  (core/execute! conn-or-pool {:raw drop-sessions-ddl})
  (core/execute! conn-or-pool {:raw drop-definitions-ddl})
  true)

(defn truncate-tables!
  "Truncate all statechart persistence tables.
   Removes all data but keeps table structure."
  [conn-or-pool]
  (core/execute! conn-or-pool {:raw "TRUNCATE statechart_events, statechart_sessions, statechart_definitions RESTART IDENTITY CASCADE"})
  true)
