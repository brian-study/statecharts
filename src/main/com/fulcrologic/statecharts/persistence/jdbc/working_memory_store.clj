(ns com.fulcrologic.statecharts.persistence.jdbc.working-memory-store
  "A PostgreSQL-backed working memory store with optimistic locking.

   Working memory is stored as BYTEA (via nippy) with a version column for
   concurrent write detection."
  (:require
   [clojure.edn :as edn]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.job-store :as job-store]
   [com.fulcrologic.statecharts.protocols :as sp]
   [taoensso.timbre :as log]))

;; -----------------------------------------------------------------------------
;; Optimistic Lock Exception
;; -----------------------------------------------------------------------------

(defn optimistic-lock-failure
  "Create an exception for optimistic lock failure."
  [session-id expected-version]
  (ex-info "Optimistic lock failure: session was modified by another process"
           {:type ::optimistic-lock-failure
            :session-id session-id
            :expected-version expected-version}))

(defn optimistic-lock-failure?
  "Check if an exception is an optimistic lock failure."
  [e]
  (and (instance? clojure.lang.ExceptionInfo e)
       (= ::optimistic-lock-failure (:type (ex-data e)))))

;; -----------------------------------------------------------------------------
;; Internal Helpers
;; -----------------------------------------------------------------------------

(defn- read-statechart-src
  "Decode the stored `statechart_src` column back to its original shape.

   Written via `pr-str` (see `upsert-session!` below), so a keyword like
   `::my-chart` round-trips as a qualified keyword, a UUID as a `#uuid`
   tagged literal, etc. Callers must tolerate a string fallback when the
   row on disk is malformed — a corrupted or partially-written row
   shouldn't wedge the entire session. The fallback preserves the raw
   string so callers can surface the corruption rather than silently
   dropping it."
  [s]
  (try
    (edn/read-string s)
    (catch Exception e
      (log/error e "Malformed statechart_src in session row — falling back to raw string"
                 {:raw s})
      s)))

(defn- fetch-session
  "Fetch a session by ID, returning working memory with version metadata."
  [ds session-id]
  (when-let [row (core/execute-one! ds
                                    {:select [:working-memory :version :statechart-src]
                                     :from [:statechart-sessions]
                                     :where [:= :session-id (core/session-id->str session-id)]})]
    (let [src (read-statechart-src (:statechart-src row))]
      (-> (:working-memory row)
          core/thaw
          (assoc ::sc/statechart-src src)
          (core/attach-version (:version row))))))

(defn- update-session!
  "Update an existing session with optimistic locking.
   Throws on version mismatch."
  [ds session-id wmem expected-version]
  (let [new-version (inc expected-version)
        result (core/execute! ds
                              {:update :statechart-sessions
                               :set {:working-memory (core/freeze wmem)
                                     :version new-version
                                     :updated-at [:now]}
                               :where [:and
                                       [:= :session-id (core/session-id->str session-id)]
                                       [:= :version expected-version]]})]
    (when (zero? (core/affected-row-count result))
      (log/warn "Optimistic lock failure"
                {:session-id session-id
                 :expected-version expected-version})
      (throw (optimistic-lock-failure session-id expected-version)))
    (log/trace "Session updated"
               {:session-id session-id
                :version new-version})
    true))

(defn- upsert-session!
  "Insert or update a session with proper version handling.

   - With version metadata: optimistic-lock update via `update-session!`.
   - Without version metadata (initial save from `start!`): INSERT with
     `ON CONFLICT DO UPDATE` so concurrent starts of the same session-id
     deterministically end in the last writer's state rather than one of
     them surfacing a duplicate-key error.

   When `on-save-hooks` is non-empty, the save runs inside a transaction
   and each hook is invoked with the tx connection before commit. If any
   hook throws, the WM write rolls back with it — this is how callers
   (e.g. the JDBC event queue) piggy-back atomic ACK-on-save semantics."
  [ds session-id wmem on-save-hooks]
  (let [expected-version (core/get-version wmem)
        write!           (fn [conn]
                           (if expected-version
                             (update-session! conn session-id wmem expected-version)
                             (let [src (get wmem ::sc/statechart-src)]
                               (core/execute! conn
                                              {:insert-into :statechart-sessions
                                               :values [{:session-id (core/session-id->str session-id)
                                                         :statechart-src (pr-str src)
                                                         :working-memory (core/freeze wmem)
                                                         :version 1}]
                                               :on-conflict [:session-id]
                                               :do-update-set {:working-memory :excluded.working-memory
                                                               :statechart-src :excluded.statechart-src
                                                               :version [:+ :statechart-sessions.version 1]
                                                               :updated-at [:now]}})
                               true)))]
    (if (seq on-save-hooks)
      (core/with-tx [tx ds]
        (write! tx)
        (doseq [hook on-save-hooks]
          (hook tx))
        true)
      (write! ds))))

(defn- delete-session!
  "Delete a session by ID, cancel any durable jobs it owns, and purge
   pending events targeted at it.

   Cancellation writes a terminal event per job so the reconciler can
   dispatch `error.invoke.<invokeid>` for observers. The event purge
   removes queued rows whose `target_session_id` points at the deleted
   session (both immediate and delayed / already-claimed rows) — without
   it, workers keep claiming zombie events against a missing WM, waste
   cycles, and pollute logs until the fallback ACK path eventually clears
   them. All three operations share one transaction so either everything
   commits or nothing does; a mid-delete failure must not leave jobs
   cancelled but the session row intact, and must not leave queued events
   pointing at a session that's about to be deleted.

   Calls `job-store/cancel-by-session-in-tx!` directly (NOT the
   tx-opening `cancel-by-session!` variant) because nested
   `core/with-tx` is forbidden in this codebase — `next.jdbc`'s
   default `:nested-tx :commit` would cause the inner cancellation's
   UPDATEs to commit independently, so a failing outer session DELETE
   would leave jobs cancelled but the session row intact."
  [ds session-id]
  (core/with-tx [tx ds]
    (job-store/cancel-by-session-in-tx! tx session-id)
    (core/execute! tx
                   {:delete-from :statechart-events
                    :where [:and
                            [:= :target-session-id (core/session-id->str session-id)]
                            [:is :processed-at nil]]})
    (let [result (core/execute! tx
                                {:delete-from :statechart-sessions
                                 :where [:= :session-id (core/session-id->str session-id)]})]
      (when (pos? (core/affected-row-count result))
        (log/debug "Session deleted"
                   {:session-id session-id}))
      true)))

;; -----------------------------------------------------------------------------
;; WorkingMemoryStore Implementation
;; -----------------------------------------------------------------------------

(defrecord JdbcWorkingMemoryStore [datasource]
  sp/WorkingMemoryStore
  (get-working-memory [_ _env session-id]
    (fetch-session datasource session-id))

  (save-working-memory! [_ env session-id wmem]
    ;; `::sc/on-save-hooks` in env lets callers (e.g. the JDBC event queue)
    ;; participate in the save's transaction so ACK-on-save is atomic.
    ;;
    ;; `::sc/save-attempted?` is an atom the event queue installs to
    ;; distinguish "handler never tried to save" (fallback ACK is safe —
    ;; session gone / terminated) from "handler tried to save but the
    ;; tx rolled back" (fallback ACK is a bug — would silently lose the
    ;; event). Flipped BEFORE the upsert runs so a rollback still counts
    ;; as "attempted".
    (when-let [a (::sc/save-attempted? env)]
      (reset! a true))
    (upsert-session! datasource session-id wmem (::sc/on-save-hooks env)))

  (delete-working-memory! [_ _env session-id]
    (delete-session! datasource session-id)))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn new-store
  "Create a new JDBC-backed working memory store.

   datasource - a javax.sql.DataSource (HikariCP is the standard choice) or a
                java.sql.Connection."
  [datasource]
  (->JdbcWorkingMemoryStore datasource))

;; -----------------------------------------------------------------------------
;; Retry Helper
;; -----------------------------------------------------------------------------

(defn with-optimistic-retry
  "Execute f with automatic retry on optimistic lock failure.

   Options:
   - :max-retries - Maximum number of retries (default 3)
   - :backoff-ms - Initial backoff in ms (default 50, doubles each retry)"
  ([f] (with-optimistic-retry {} f))
  ([{:keys [max-retries backoff-ms]
     :or {max-retries 3
          backoff-ms 50}} f]
   (loop [attempt 1]
     (let [result (try
                    {:ok (f)}
                    (catch clojure.lang.ExceptionInfo e
                      (if (and (optimistic-lock-failure? e)
                               (< attempt max-retries))
                        (let [backoff (* backoff-ms (long (Math/pow 2 (dec attempt))))]
                          (log/debug "Retrying after optimistic lock failure"
                                     {:attempt attempt
                                      :max-retries max-retries
                                      :backoff-ms backoff
                                      :session-id (:session-id (ex-data e))})
                          {:retry true :backoff backoff})
                        (throw e))))]
       (if (:retry result)
         (do
           (Thread/sleep (:backoff result))
           (recur (inc attempt)))
         (:ok result))))))
