(ns com.fulcrologic.statecharts.persistence.jdbc.job-store
  "PostgreSQL-backed durable job store for statechart invocations.

   Jobs are created by DurableJobInvocationProcessor when entering an invoke state,
   and claimed by the worker for execution. Designed for restart safety with
   lease-based ownership and idempotent operations."
  (:require
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [taoensso.timbre :as log])
  (:import
   [java.time OffsetDateTime Duration]))

;; -----------------------------------------------------------------------------
;; Invokeid Serialization
;; -----------------------------------------------------------------------------

(defn invokeid->str
  "Serialize an invokeid keyword to a string, preserving namespace.
   Simple keywords: :content-generation → \"content-generation\"
   Qualified keywords: :my-ns/gen → \"my-ns/gen\""
  [invokeid]
  (subs (str invokeid) 1))

(defn str->invokeid
  "Deserialize a string back to an invokeid keyword.
   \"content-generation\" → :content-generation
   \"my-ns/gen\" → :my-ns/gen"
  [s]
  (keyword s))

;; -----------------------------------------------------------------------------
;; Internal Helpers
;; -----------------------------------------------------------------------------

(defn- backoff-seconds
  "Exponential backoff: 2^attempt seconds, capped at 60s."
  [attempt]
  (min 60 (long (Math/pow 2 attempt))))

(defn- guarded-where-clause
  "Build a status/lease-guarded WHERE clause for terminal updates.
   If owner-id is present, only the current lease owner may update.
   Uses `?` placeholders; extra-params are appended to the caller's param vector
   in the order they appear in the returned SQL."
  [owner-id]
  (if owner-id
    {:sql " WHERE id = ? AND status = 'running' AND lease_owner = ?"
     :extra-params [owner-id]}
    {:sql " WHERE id = ? AND status IN ('pending', 'running')"
     :extra-params []}))

(defn- hydrate-job-row
  "Decode persisted fields and restore session-id shape for runtime use."
  [row]
  (-> row
      (update :session-id core/str->session-id)
      (update :invokeid str->invokeid)
      (update :payload core/thaw)
      (cond->
        (:result row) (update :result core/thaw)
        (:error row) (update :error core/thaw)
        (:terminal-event-data row) (update :terminal-event-data core/thaw))))

;; -----------------------------------------------------------------------------
;; Job CRUD
;; -----------------------------------------------------------------------------

(def ^:private create-job-max-race-attempts
  "Upper bound on (try-insert! → find-active) retries under adversarial races
   where the active job flips to terminal between INSERT-no-op and SELECT and
   a new active job appears before the retry. Exhaustion throws — stranding an
   invocation is preferable to silently returning nil."
  5)

(defn create-job!
  "Create a new job, returning the job-id (UUID).

   Idempotent (I1): if an active job already exists for this session+invokeid,
   returns the existing job-id instead of creating a duplicate.
   Uses partial unique index on (session_id, invokeid) WHERE status IN ('pending','running').

   Under adversarial concurrency, the (INSERT-no-op → SELECT) sequence can observe
   a terminal transition and miss the active job. We loop up to
   `create-job-max-race-attempts` times and throw on exhaustion rather than
   returning nil (which callers would stringify to an empty job-id)."
  [ds {:keys [id session-id invokeid job-type payload max-attempts]
       :or {max-attempts 3}}]
  (let [sid-str (core/session-id->str session-id)
        iid-str (invokeid->str invokeid)
        insert-sql (str "INSERT INTO statechart_jobs (id, session_id, invokeid, job_type, payload, max_attempts)"
                        " VALUES (?, ?, ?, ?, ?, ?)"
                        " ON CONFLICT (session_id, invokeid) WHERE status IN ('pending', 'running')"
                        " DO NOTHING"
                        " RETURNING id")
        insert-params [id sid-str iid-str job-type (core/freeze payload) max-attempts]
        try-insert! (fn []
                      (let [result (core/execute-sql! ds insert-sql insert-params)]
                        (or (some-> (when (sequential? result) (first result)) :id)
                            (when (pos? (core/affected-row-count result)) id))))
        find-active (fn []
                      (:id (first (core/execute-sql! ds
                                    (str "SELECT id FROM statechart_jobs"
                                         " WHERE session_id = ? AND invokeid = ?"
                                         " AND status IN ('pending', 'running')")
                                    [sid-str iid-str]))))]
    (loop [attempt 0]
      (if-let [job-id (or (try-insert!) (find-active))]
        job-id
        (if (< attempt create-job-max-race-attempts)
          (do
            (log/debug "create-job! lost a race, retrying"
                       {:session-id session-id :invokeid invokeid :attempt attempt})
            (recur (inc attempt)))
          (throw (ex-info "create-job! exhausted race retries"
                          {:session-id     session-id
                           :invokeid       invokeid
                           :attempts       attempt
                           :max-race-attempts create-job-max-race-attempts})))))))

(defn claim-jobs!
  "Claim up to `limit` claimable jobs for this worker.

   A job is claimable if:
   - status is 'pending' and next_run_at <= now, OR
   - status is 'running' and lease has expired (stale worker recovery)

   Uses SELECT FOR UPDATE SKIP LOCKED to prevent concurrent claims.
   Sets status='running', increments attempt, sets lease.

   Returns claimed job rows with thawed payload."
  [ds {:keys [owner-id lease-duration-seconds limit]
       :or {lease-duration-seconds 60 limit 5}}]
  (core/with-tx [tx ds]
    (let [limit (long limit) ;; ensure numeric
          now (OffsetDateTime/now)
          lease-until (.plus now (Duration/ofSeconds lease-duration-seconds))
          rows (core/execute-sql! tx
                 (str "UPDATE statechart_jobs"
                      " SET status = 'running',"
                      "     attempt = attempt + 1,"
                      "     lease_owner = ?,"
                      "     lease_expires_at = ?,"
                      "     updated_at = now()"
                      " WHERE id IN ("
                      "   SELECT id FROM statechart_jobs"
                      "   WHERE (status = 'pending' AND next_run_at <= now())"
                      "      OR (status = 'running' AND lease_expires_at < now())"
                      "   ORDER BY next_run_at"
                      "   LIMIT " limit
                      "   FOR UPDATE SKIP LOCKED"
                      " )"
                      " RETURNING *")
                 [owner-id lease-until])]
      (->> rows
           ;; UPDATE ... RETURNING does not guarantee row order in all plans.
           ;; Re-apply deterministic ordering for predictable claims/tests.
           (sort-by (juxt :next-run-at :id))
           (mapv hydrate-job-row)))))

(defn heartbeat!
  "Extend the lease for a running job owned by this worker.

   Returns true if lease was extended (we still own it).
   Returns false if lease was taken over by another worker (I8) or job is
   no longer running. On false, the worker must abandon execution immediately."
  [ds job-id owner-id lease-duration-seconds]
  (let [lease-until (.plus (OffsetDateTime/now) (Duration/ofSeconds lease-duration-seconds))
        result (core/execute-sql! ds
                 (str "UPDATE statechart_jobs"
                      " SET lease_expires_at = ?, updated_at = now()"
                      " WHERE id = ? AND lease_owner = ? AND status = 'running'"
                      " RETURNING id")
                 [lease-until job-id owner-id])]
    (pos? (core/affected-row-count result))))

(defn complete!
  "Mark a job as succeeded and store the result.
   Also stores the terminal event name and data for reconciliation.
   Returns true when the row was updated, false otherwise."
  ([ds job-id result terminal-event-name terminal-event-data]
   (complete! ds job-id nil result terminal-event-name terminal-event-data))
  ([ds job-id owner-id result terminal-event-name terminal-event-data]
   (let [{:keys [sql extra-params]} (guarded-where-clause owner-id)
         rows (core/execute-sql! ds
                (str "UPDATE statechart_jobs"
                     " SET status = 'succeeded',"
                     "     result = ?,"
                     "     terminal_event_name = ?,"
                     "     terminal_event_data = ?,"
                     "     lease_owner = NULL,"
                     "     lease_expires_at = NULL,"
                     "     updated_at = now()"
                     sql
                     " RETURNING id")
                (into [(core/freeze result)
                       terminal-event-name
                       (core/freeze terminal-event-data)
                       job-id]
                      extra-params))]
     (pos? (core/affected-row-count rows)))))

(defn fail!
  "Handle job failure. If attempts remain, re-enqueue with backoff.
   If exhausted, mark failed and store the terminal event for dispatch.
   Returns one of:
   - :retry-scheduled
   - :failed
   - :ignored (job no longer active/owned)."
  ([ds job-id attempt max-attempts error terminal-event-name terminal-event-data]
   (fail! ds job-id nil attempt max-attempts error terminal-event-name terminal-event-data))
  ([ds job-id owner-id attempt max-attempts error terminal-event-name terminal-event-data]
   (if (< attempt max-attempts)
     ;; Retryable — re-enqueue with backoff
     (let [delay-secs (backoff-seconds attempt)
           next-run (.plus (OffsetDateTime/now) (Duration/ofSeconds delay-secs))
           {:keys [sql extra-params]} (guarded-where-clause owner-id)
           rows (core/execute-sql! ds
                  (str "UPDATE statechart_jobs"
                       " SET status = 'pending',"
                       "     next_run_at = ?,"
                       "     lease_owner = NULL,"
                       "     lease_expires_at = NULL,"
                       "     error = ?,"
                       "     updated_at = now()"
                       sql
                       " RETURNING id")
                  (into [next-run (core/freeze error) job-id]
                        extra-params))]
       (if (pos? (core/affected-row-count rows))
         (do
           (log/info "Job failed, scheduling retry"
                     {:job-id job-id :attempt attempt :max-attempts max-attempts
                      :next-run-in-seconds delay-secs})
           :retry-scheduled)
         :ignored))
     ;; Exhausted — terminal failure
     (let [{:keys [sql extra-params]} (guarded-where-clause owner-id)
           rows (core/execute-sql! ds
                  (str "UPDATE statechart_jobs"
                       " SET status = 'failed',"
                       "     error = ?,"
                       "     terminal_event_name = ?,"
                       "     terminal_event_data = ?,"
                       "     lease_owner = NULL,"
                       "     lease_expires_at = NULL,"
                       "     updated_at = now()"
                       sql
                       " RETURNING id")
                  (into [(core/freeze error)
                         terminal-event-name
                         (core/freeze terminal-event-data)
                         job-id]
                        extra-params))]
       (if (pos? (core/affected-row-count rows))
         (do
           (log/warn "Job failed permanently"
                     {:job-id job-id :attempt attempt :max-attempts max-attempts})
           :failed)
         :ignored)))))

(def ^:private cancelled-event-data
  (delay (core/freeze {:reason :cancelled})))

(defn cancel!
  "Cancel a job for a specific session+invokeid.
   Status-conditional (I7): only cancels pending/running jobs.

   Also writes a terminal event (`error.invoke.<invokeid>` with data
   `{:reason :cancelled}`) so the reconciler can dispatch it to the parent
   session — otherwise an in-flight worker and the parent would deadlock
   waiting for each other.

   Returns the number of rows affected."
  [ds session-id invokeid]
  (let [iid-str (invokeid->str invokeid)
        terminal-event-name (str \: "error.invoke." iid-str)
        result (core/execute-sql! ds
                 (str "UPDATE statechart_jobs"
                      " SET status = 'cancelled',"
                      "     lease_owner = NULL,"
                      "     lease_expires_at = NULL,"
                      "     terminal_event_name = ?,"
                      "     terminal_event_data = ?,"
                      "     updated_at = now()"
                      " WHERE session_id = ? AND invokeid = ?"
                      " AND status IN ('pending', 'running')"
                      " RETURNING id")
                 [terminal-event-name
                  @cancelled-event-data
                  (core/session-id->str session-id)
                  iid-str])]
    (core/affected-row-count result)))

(defn cancel-by-session!
  "Cancel all active jobs for a session (I6).
   Used when session is being deleted or reset.

   Writes a terminal event per job so the reconciler dispatches
   `error.invoke.<invokeid>` to the parent. `invokeid` is concatenated
   server-side (safe: `invokeid` is library-serialised via `invokeid->str`,
   never user input)."
  [ds session-id]
  (let [result (core/execute-sql! ds
                 (str "UPDATE statechart_jobs"
                      " SET status = 'cancelled',"
                      "     lease_owner = NULL,"
                      "     lease_expires_at = NULL,"
                      "     terminal_event_name = ':error.invoke.' || invokeid,"
                      "     terminal_event_data = ?,"
                      "     updated_at = now()"
                      " WHERE session_id = ?"
                      " AND status IN ('pending', 'running')"
                      " RETURNING id")
                 [@cancelled-event-data
                  (core/session-id->str session-id)])]
    (core/affected-row-count result)))

(defn get-active-job
  "Get the active (pending/running) job for a session+invokeid, or nil."
  [ds session-id invokeid]
  (let [rows (core/execute-sql! ds
               (str "SELECT * FROM statechart_jobs"
                    " WHERE session_id = ? AND invokeid = ?"
                    " AND status IN ('pending', 'running')")
               [(core/session-id->str session-id)
                (invokeid->str invokeid)])]
    (when-let [row (first rows)]
      (hydrate-job-row row))))

(defn job-cancelled?
  "Check if a job has been cancelled. Used by worker to poll during execution (I6)."
  [ds job-id]
  (let [row (core/execute-sql-one! ds
              "SELECT status FROM statechart_jobs WHERE id = ?"
              [job-id])]
    (= "cancelled" (:status row))))

(defn get-undispatched-terminal-jobs
  "Get jobs in a terminal state whose terminal event hasn't been dispatched yet.
   Used by the reconciliation loop.

   Includes succeeded, failed, and cancelled jobs (the latter always have a
   `terminal_event_name` written by `cancel!` / `cancel-by-session!`)."
  [ds limit]
  (let [limit (long limit) ;; ensure numeric — prevent SQL injection
        rows (core/execute-sql! ds
               (str "SELECT * FROM statechart_jobs"
                    " WHERE status IN ('succeeded', 'failed', 'cancelled')"
                    " AND terminal_event_dispatched_at IS NULL"
                    " AND terminal_event_name IS NOT NULL"
                    " ORDER BY updated_at"
                    " LIMIT " limit))]
    (mapv hydrate-job-row rows)))

(defn mark-terminal-event-dispatched!
  "Mark a job's terminal event as dispatched (reconciliation complete)."
  [ds job-id]
  (core/execute! ds
    {:update :statechart-jobs
     :set {:terminal-event-dispatched-at [:now]}
     :where [:= :id job-id]}))

(defn store-partial-result!
  "Store intermediate result for idempotent retry (I9).
   Non-streaming job handlers call this after entity creation but before completion,
   so retries can skip entity creation."
  [ds job-id result]
  (core/execute! ds
    {:update :statechart-jobs
     :set {:result (core/freeze result)
           :updated-at [:now]}
     :where [:= :id job-id]}))
