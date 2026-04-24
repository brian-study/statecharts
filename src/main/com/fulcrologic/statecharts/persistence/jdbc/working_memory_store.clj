(ns com.fulcrologic.statecharts.persistence.jdbc.working-memory-store
  "A PostgreSQL-backed working memory store with optimistic locking.

   Working memory is stored as BYTEA (via nippy) with a version column for
   concurrent write detection."
  (:require
   [clojure.edn :as edn]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
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

(defn- fetch-session
  "Fetch a session by ID, returning working memory with version metadata."
  [ds session-id]
  (when-let [row (core/execute-one! ds
                                    {:select [:working-memory :version :statechart-src]
                                     :from [:statechart-sessions]
                                     :where [:= :session-id (core/session-id->str session-id)]})]
    (let [src (edn/read-string (:statechart-src row))]
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
     them surfacing a duplicate-key error."
  [ds session-id wmem]
  (let [expected-version (core/get-version wmem)]
    (if expected-version
      (update-session! ds session-id wmem expected-version)
      (let [src (get wmem ::sc/statechart-src)]
        (core/execute! ds
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
        true))))

(defn- delete-session!
  "Delete a session by ID."
  [ds session-id]
  (let [result (core/execute! ds
                              {:delete-from :statechart-sessions
                               :where [:= :session-id (core/session-id->str session-id)]})]
    (when (pos? (core/affected-row-count result))
      (log/debug "Session deleted"
                 {:session-id session-id}))
    true))

;; -----------------------------------------------------------------------------
;; WorkingMemoryStore Implementation
;; -----------------------------------------------------------------------------

(defrecord JdbcWorkingMemoryStore [datasource]
  sp/WorkingMemoryStore
  (get-working-memory [_ _env session-id]
    (fetch-session datasource session-id))

  (save-working-memory! [_ _env session-id wmem]
    (upsert-session! datasource session-id wmem))

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
