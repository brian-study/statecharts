(ns com.fulcrologic.statecharts.persistence.pg.schema-spec
  "Tests for PostgreSQL schema DDL.

   These tests verify the DDL structure and function existence
   without requiring a database."
  (:require
   [com.fulcrologic.statecharts.persistence.pg.schema :as schema]
   [fulcro-spec.core :refer [=> assertions behavior component specification]]))

;; -----------------------------------------------------------------------------
;; DDL Content Tests
;; -----------------------------------------------------------------------------

(specification "Sessions Table DDL"
  (component "table structure"
    (let [ddl @#'schema/sessions-ddl]
      (behavior "contains CREATE TABLE"
        (assertions
          (clojure.string/includes? ddl "CREATE TABLE") => true))

      (behavior "creates statechart_sessions table"
        (assertions
          (clojure.string/includes? ddl "statechart_sessions") => true))

      (behavior "has session_id primary key"
        (assertions
          (clojure.string/includes? ddl "session_id") => true
          (clojure.string/includes? ddl "PRIMARY KEY") => true))

      (behavior "has statechart_src column"
        (assertions
          (clojure.string/includes? ddl "statechart_src") => true))

      (behavior "has working_memory JSONB column"
        (assertions
          (clojure.string/includes? ddl "working_memory") => true
          (clojure.string/includes? ddl "JSONB") => true))

      (behavior "has version column for optimistic locking"
        (assertions
          (clojure.string/includes? ddl "version") => true
          (clojure.string/includes? ddl "BIGINT") => true))

      (behavior "has timestamp columns"
        (assertions
          (clojure.string/includes? ddl "created_at") => true
          (clojure.string/includes? ddl "updated_at") => true
          (clojure.string/includes? ddl "TIMESTAMPTZ") => true))

      (behavior "uses IF NOT EXISTS for idempotency"
        (assertions
          (clojure.string/includes? ddl "IF NOT EXISTS") => true))))

  (component "indexes"
    (let [indexes @#'schema/sessions-indexes-ddl]
      (behavior "creates index on statechart_src"
        (assertions
          (some #(clojure.string/includes? % "statechart_src") indexes) => true))

      (behavior "creates index on updated_at"
        (assertions
          (some #(clojure.string/includes? % "updated_at") indexes) => true)))))

(specification "Events Table DDL"
  (component "table structure"
    (let [ddl @#'schema/events-ddl]
      (behavior "contains CREATE TABLE"
        (assertions
          (clojure.string/includes? ddl "CREATE TABLE") => true))

      (behavior "creates statechart_events table"
        (assertions
          (clojure.string/includes? ddl "statechart_events") => true))

      (behavior "has BIGSERIAL primary key"
        (assertions
          (clojure.string/includes? ddl "BIGSERIAL PRIMARY KEY") => true))

      (behavior "has target_session_id column"
        (assertions
          (clojure.string/includes? ddl "target_session_id") => true))

      (behavior "has source_session_id column"
        (assertions
          (clojure.string/includes? ddl "source_session_id") => true))

      (behavior "has send_id for cancellation"
        (assertions
          (clojure.string/includes? ddl "send_id") => true))

      (behavior "has invoke_id for invocation tracking"
        (assertions
          (clojure.string/includes? ddl "invoke_id") => true))

      (behavior "has event_name column"
        (assertions
          (clojure.string/includes? ddl "event_name") => true))

      (behavior "has event_type column"
        (assertions
          (clojure.string/includes? ddl "event_type") => true))

      (behavior "has event_data JSONB column"
        (assertions
          (clojure.string/includes? ddl "event_data") => true
          (clojure.string/includes? ddl "JSONB") => true))

      (behavior "has deliver_at for delayed events"
        (assertions
          (clojure.string/includes? ddl "deliver_at") => true))

      (behavior "has claimed_at for exactly-once processing"
        (assertions
          (clojure.string/includes? ddl "claimed_at") => true))

      (behavior "has claimed_by for worker tracking"
        (assertions
          (clojure.string/includes? ddl "claimed_by") => true))

      (behavior "has processed_at for completion tracking"
        (assertions
          (clojure.string/includes? ddl "processed_at") => true))))

  (component "indexes"
    (let [indexes @#'schema/events-indexes-ddl]
      (behavior "has index for delivery lookup"
        (assertions
          (some #(and (clojure.string/includes? % "target_session_id")
                      (clojure.string/includes? % "deliver_at")) indexes) => true))

      (behavior "has index for cancellation"
        (assertions
          (some #(and (clojure.string/includes? % "source_session_id")
                      (clojure.string/includes? % "send_id")) indexes) => true))

      (behavior "has index for stale claim recovery"
        (assertions
          (some #(clojure.string/includes? % "claimed_at") indexes) => true))

      (behavior "indexes use partial index WHERE clauses"
        (assertions
          (some #(clojure.string/includes? % "WHERE") indexes) => true)))))

(specification "Definitions Table DDL"
  (component "table structure"
    (let [ddl @#'schema/definitions-ddl]
      (behavior "contains CREATE TABLE"
        (assertions
          (clojure.string/includes? ddl "CREATE TABLE") => true))

      (behavior "creates statechart_definitions table"
        (assertions
          (clojure.string/includes? ddl "statechart_definitions") => true))

      (behavior "has src primary key"
        (assertions
          (clojure.string/includes? ddl "src") => true
          (clojure.string/includes? ddl "PRIMARY KEY") => true))

      (behavior "has definition JSONB column"
        (assertions
          (clojure.string/includes? ddl "definition") => true
          (clojure.string/includes? ddl "JSONB") => true))

      (behavior "has version column"
        (assertions
          (clojure.string/includes? ddl "version") => true)))))

;; -----------------------------------------------------------------------------
;; Drop DDL Tests
;; -----------------------------------------------------------------------------

(specification "Drop Table DDL"
  (component "drop statements"
    (behavior "drop sessions uses CASCADE"
      (let [ddl @#'schema/drop-sessions-ddl]
        (assertions
          (clojure.string/includes? ddl "DROP TABLE") => true
          (clojure.string/includes? ddl "IF EXISTS") => true
          (clojure.string/includes? ddl "CASCADE") => true
          (clojure.string/includes? ddl "statechart_sessions") => true)))

    (behavior "drop events uses CASCADE"
      (let [ddl @#'schema/drop-events-ddl]
        (assertions
          (clojure.string/includes? ddl "DROP TABLE") => true
          (clojure.string/includes? ddl "CASCADE") => true
          (clojure.string/includes? ddl "statechart_events") => true)))

    (behavior "drop definitions uses CASCADE"
      (let [ddl @#'schema/drop-definitions-ddl]
        (assertions
          (clojure.string/includes? ddl "DROP TABLE") => true
          (clojure.string/includes? ddl "CASCADE") => true
          (clojure.string/includes? ddl "statechart_definitions") => true)))))

;; -----------------------------------------------------------------------------
;; Public API Tests
;; -----------------------------------------------------------------------------

(specification "Schema Public API"
  (component "function existence"
    (behavior "create-tables! exists"
      (assertions
        (fn? schema/create-tables!) => true))

    (behavior "drop-tables! exists"
      (assertions
        (fn? schema/drop-tables!) => true))

    (behavior "truncate-tables! exists"
      (assertions
        (fn? schema/truncate-tables!) => true))))
