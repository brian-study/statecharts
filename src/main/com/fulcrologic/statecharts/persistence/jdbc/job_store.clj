(ns com.fulcrologic.statecharts.persistence.jdbc.job-store
  "PostgreSQL-backed durable job store for statechart invocations.

   Jobs are created by DurableJobInvocationProcessor when entering an invoke state,
   and claimed by the worker for execution. Designed for restart safety with
   lease-based ownership and idempotent operations."
  (:require
   [com.fulcrologic.statecharts.events :as evts]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [taoensso.timbre :as log])
  (:import
   [java.time OffsetDateTime Duration]))

;; -----------------------------------------------------------------------------
;; Invokeid Serialization
;; -----------------------------------------------------------------------------
;;
;; Thin wrappers over `core/encode-id` / `core/decode-id` since 2.0.17.
;; Durable-job rows use `:uuid-shape :tagged` (UUIDs stored via pr-str as
;; `#uuid \"…\"`) and `:keyword-shape / :symbol-shape :bare-unless-numeric`
;; for back-compat with pre-2.0.8 rows that wrote keyword invoke-ids via
;; `(name x)`. Decoder uses `:legacy-fallback :keyword` so a bare legacy
;; row decodes as a keyword rather than a string.

(defn invokeid->str
  "Serialize an invokeid to a string for DB storage, preserving type
   information so `str->invokeid` can reconstruct the original value.

   - Keywords/symbols with non-numeric names are stored bare (`:kw` →
     `\"kw\"`, `:my-ns/k` → `\"my-ns/k\"`) — back-compat with rows
     written pre-2.0.8.
   - Keywords whose name would parse as a number (`:42`, `:3.14`,
     `:-dashy`) are stored with the leading `:` via `pr-str` so the
     decoder can tell them apart from real numbers.
   - Everything else (string, UUID, Long, Double, BigInt, BigDecimal,
     Ratio, …) is stored via `pr-str` so the leading marker
     distinguishes it from a bare keyword.

   Why it matters: `handle-external-invocations!` matches terminal
   events to `<invoke>` elements by `=` against the original
   `idlocation` value. A string invokeid silently coerced to a keyword
   on readback (or a `:42` keyword silently coerced to the Long 42)
   misses finalize/autoforward even though the terminal event name
   looks right."
  [invokeid]
  (core/encode-id invokeid {:uuid-shape    :tagged
                            :keyword-shape :bare-unless-numeric
                            :symbol-shape  :bare-unless-numeric}))

(defn str->invokeid
  "Deserialize an invokeid from the DB back to its original type.

   Shape-aware inverse of `invokeid->str`. Symbols round-trip as
   keywords — the bare form can't be distinguished from a keyword's
   bare form without a type marker."
  [s]
  (core/decode-id s {:legacy-fallback :keyword}))

(defn job-type->str
  "Serialize a job-type to a string for DB storage.

   Mirrors `invokeid->str`'s scheme so the on-disk form is
   self-describing: keywords with non-numeric names are stored bare
   (`:video-url-validation` → `\"video-url-validation\"`), namespaced
   keywords preserve their namespace (`:my.app/x` → `\"my.app/x\"`),
   and anything else (numeric-named keyword, UUID, string, number)
   is stored via `pr-str` with a type marker the decoder can read
   back.

   Before 2.0.22, callers hand-rolled this encoding — typically
   `(name src)`, which silently dropped namespaces. Consolidating
   here makes the lib the sole owner of the job-type shape and
   fixes the namespace bug by construction."
  [job-type]
  (core/encode-id job-type {:uuid-shape    :tagged
                            :keyword-shape :bare-unless-numeric
                            :symbol-shape  :bare-unless-numeric}))

(defn str->job-type
  "Deserialize a stored job_type back into its original type.

   Shape-aware inverse of `job-type->str`. Legacy-fallback is
   `:keyword` so pre-2.0.22 rows (bare name without a type marker,
   e.g. `\"video-url-validation\"`) continue to decode to the matching
   keyword for handler lookup. Rolling upgrades are safe: old rows
   keep working, new rows carry the marker."
  [s]
  (core/decode-id s {:legacy-fallback :keyword}))

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
  "Decode persisted fields and restore session-id / job-type shape for
   runtime use."
  [row]
  (-> row
      (update :session-id core/str->session-id)
      (update :invokeid str->invokeid)
      (update :job-type str->job-type)
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

(defn create-job-result
  "Create a new job and return a result map — `{:ok <job-id>}` on
   success, `{:error <ex-info>}` on race-exhaustion.

   Idempotent (I1): if an active job already exists for this
   session+invokeid, returns `{:ok <existing-job-id>}` instead of
   creating a duplicate. Uses a partial unique index on
   (session_id, invokeid) WHERE status IN ('pending','running').

   Under adversarial concurrency, the (INSERT-no-op → SELECT)
   sequence can observe a terminal transition and miss the active
   job. The loop retries up to `create-job-max-race-attempts`
   times; if all attempts lose the race, returns
   `{:error <ex-info>}` rather than throwing. Callers that want
   exception semantics should use `create-job!` (which is a
   thin wrapper around this fn).

   Exceptions from the JDBC driver or connection layer are NOT
   caught here — those propagate normally."
  [ds {:keys [id session-id invokeid job-type payload max-attempts]
       :or {max-attempts 3}}]
  (let [sid-str (core/session-id->str session-id)
        iid-str (invokeid->str invokeid)
        jt-str (job-type->str job-type)
        insert-sql (str "INSERT INTO statechart_jobs (id, session_id, invokeid, job_type, payload, max_attempts)"
                        " VALUES (?, ?, ?, ?, ?, ?)"
                        " ON CONFLICT (session_id, invokeid) WHERE status IN ('pending', 'running')"
                        " DO NOTHING"
                        " RETURNING id")
        insert-params [id sid-str iid-str jt-str (core/freeze payload) max-attempts]
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
        {:ok job-id}
        (if (< attempt create-job-max-race-attempts)
          (do
            (log/debug "create-job! lost a race, retrying"
                       {:session-id session-id :invokeid invokeid :attempt attempt})
            (recur (inc attempt)))
          {:error (ex-info "create-job! exhausted race retries"
                           {:session-id        session-id
                            :invokeid          invokeid
                            :attempts          attempt
                            :max-race-attempts create-job-max-race-attempts})})))))

(defn create-job!
  "Create a new job, returning the job-id (UUID).

   Backward-compatible wrapper over `create-job-result`: unwraps
   `{:ok id}` to `id`, throws the captured `ex-info` on `{:error}`.
   Exception-as-control-flow is sometimes what callers want (fail
   the enclosing transition outright); when they prefer to branch
   on the failure — e.g. emit `:error.platform` to the parent
   session and keep going — use `create-job-result` directly."
  [ds params]
  (let [r (create-job-result ds params)]
    (if-let [id (:ok r)]
      id
      (throw (:error r)))))

(defn claim-jobs!
  "Claim up to `limit` claimable jobs for this worker.

   A job is claimable if:
   - status is 'pending' and next_run_at <= now, OR
   - status is 'running' and lease has expired (stale worker recovery)

   Uses SELECT FOR UPDATE SKIP LOCKED to prevent concurrent claims.
   Sets status='running', increments attempt, sets lease.

   `lease_expires_at` is computed server-side via `now() + interval`
   rather than app-side so application clock skew against the DB server
   cannot stretch or compress the lease window.

   Returns claimed job rows with thawed payload."
  [ds {:keys [owner-id lease-duration-seconds limit]
       :or {lease-duration-seconds 60 limit 5}}]
  (core/with-tx [tx ds]
    (let [limit (long limit) ;; ensure numeric
          lease-secs (long lease-duration-seconds) ;; numeric — prevent SQL injection
          rows (core/execute-sql! tx
                 (str "UPDATE statechart_jobs"
                      " SET status = 'running',"
                      "     attempt = attempt + 1,"
                      "     lease_owner = ?,"
                      "     lease_expires_at = now() + interval '" lease-secs " seconds',"
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
                 [owner-id])]
      (->> rows
           ;; UPDATE ... RETURNING does not guarantee row order in all plans.
           ;; Re-apply deterministic ordering for predictable claims/tests.
           (sort-by (juxt :next-run-at :id))
           (mapv hydrate-job-row)))))

(defn heartbeat!
  "Extend the lease for a running job owned by this worker.

   `lease_expires_at` is computed server-side (`now() + interval`) so
   application clock skew against the DB server cannot extend the
   lease past its intended deadline — a skew of +5s on the worker
   would otherwise buy it 5s of extra ownership it shouldn't have.
   `lease-duration-seconds` = 0 (or negative) yields a lease that has
   already expired server-side, which is the intended 'retire' path.

   Returns true if lease was extended (we still own it).
   Returns false if lease was taken over by another worker (I8) or job is
   no longer running. On false, the worker must abandon execution immediately."
  [ds job-id owner-id lease-duration-seconds]
  (let [lease-secs (long lease-duration-seconds) ;; numeric — prevent SQL injection
        result (core/execute-sql! ds
                 (str "UPDATE statechart_jobs"
                      " SET lease_expires_at = now() + interval '" lease-secs " seconds',"
                      "     updated_at = now()"
                      " WHERE id = ? AND lease_owner = ? AND status = 'running'"
                      " RETURNING id")
                 [job-id owner-id])]
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

(defn- terminal-event-name-for
  "Build the `error.invoke.<invokeid>` terminal event name in the same EDN
   form the worker's reconciliation path expects (`pr-str`'d keyword)."
  [invokeid]
  (pr-str (evts/invoke-error-event invokeid)))

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
        terminal-event-name (terminal-event-name-for invokeid)
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

(defn cancel-by-session-in-tx!
  "Internal helper: SELECT pending/running jobs for `session-id` and
   issue a guarded UPDATE per row to cancel them, writing a terminal
   event per job so the reconciler dispatches
   `error.invoke.<invokeid>` to observers.

   Runs against an already-open `Connection` (`tx`). Use this when you
   need cancellation to participate in a caller's existing transaction
   — `delete-session!` in `working_memory_store.clj` is the load-bearing
   example: session delete + job cancel must be atomic, but the
   top-level `with-tx` lives in `delete-session!`.

   Nested `core/with-tx` is forbidden in this codebase; the `-in-tx`
   variant is how helpers cooperate with a caller's open transaction.

   Since invokeid storage is typed (keyword/UUID/number/string via
   `invokeid->str` / `str->invokeid`), we can't reconstruct the terminal
   event name server-side via SQL concat — we fetch each row,
   reconstruct the original invokeid in Clojure, and issue a guarded
   UPDATE per row."
  [tx session-id]
  (let [sid-str        (core/session-id->str session-id)
        cancelled-data @cancelled-event-data
        rows           (core/execute-sql! tx
                         (str "SELECT id, invokeid FROM statechart_jobs"
                              " WHERE session_id = ?"
                              " AND status IN ('pending', 'running')")
                         [sid-str])]
    (reduce
      (fn [n {:keys [id invokeid]}]
        (let [orig-invokeid (str->invokeid invokeid)
              terminal-event-name (terminal-event-name-for orig-invokeid)
              result (core/execute-sql! tx
                       (str "UPDATE statechart_jobs"
                            " SET status = 'cancelled',"
                            "     lease_owner = NULL,"
                            "     lease_expires_at = NULL,"
                            "     terminal_event_name = ?,"
                            "     terminal_event_data = ?,"
                            "     updated_at = now()"
                            " WHERE id = ?"
                            " AND status IN ('pending', 'running')"
                            " RETURNING id")
                       [terminal-event-name cancelled-data id])]
          (+ n (core/affected-row-count result))))
      0
      rows)))

(defn cancel-by-session!
  "Cancel all active jobs for a session (I6).
   Used when session is being deleted or reset.

   Writes a terminal event per job so the reconciler dispatches
   `error.invoke.<invokeid>` to the parent. Opens its own transaction
   so cancellation is atomic: either every pending/running job in the
   session is cancelled or none of them are.

   If you need cancellation to participate in a caller's existing
   transaction, call `cancel-by-session-in-tx!` directly instead —
   nested `core/with-tx` is forbidden."
  [ds session-id]
  (core/with-tx [tx ds]
    (cancel-by-session-in-tx! tx session-id)))

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
  "Mark a job's terminal event as dispatched (reconciliation complete).

   Prefer `claim-terminal-dispatch!` which is atomic — this helper is
   kept for callers that have other reasons to set the timestamp
   (e.g. backfill scripts)."
  [ds job-id]
  (core/execute! ds
    {:update :statechart-jobs
     :set {:terminal-event-dispatched-at [:now]}
     :where [:= :id job-id]}))

(defn claim-terminal-dispatch!
  "Atomically claim a terminal job for dispatch. Returns `true` if this
   caller won the claim and should `send!` the terminal event; `false`
   if another worker or reconciler has already claimed the slot.

   The claim commits `terminal_event_dispatched_at = now()` conditionally
   on `terminal_event_dispatched_at IS NULL`, so simultaneous callers
   serialize at the row and only the first wins. This eliminates
   duplicate `done.invoke.*` / `error.invoke.*` events across concurrent
   reconcilers and the gap between a worker's `succeed!` / `fail!` and
   its own `mark-terminal-event-dispatched!`.

   **Callers must check session readiness before claiming.** If the
   parent session is not yet ready to receive the terminal event, the
   caller should skip the claim entirely and let a subsequent
   reconciliation pass retry. Claiming then finding the session not
   ready would permanently lose the event. See
   `worker/reconcile-undispatched!` for the intended pattern.

   **If `send!` throws after a successful claim**, call
   `release-terminal-dispatch-claim!` to roll back the claim so the
   next reconciliation pass retries. Without a release, the event
   would be silently lost.

   Trade-off: a crash between the claim committing and the subsequent
   `send!` commit still loses the terminal event (previously that
   same crash produced a duplicate). The loss is auditable — query
   for rows with `terminal_event_dispatched_at IS NOT NULL` but no
   matching entry in `statechart_events`. Duplicate dispatch drove
   divergent parent transitions and was the greater harm."
  [ds job-id]
  (pos? (core/affected-row-count
          (core/execute! ds
            {:update :statechart-jobs
             :set {:terminal-event-dispatched-at [:now]
                   :updated-at                   [:now]}
             :where [:and
                     [:= :id job-id]
                     [:= :terminal-event-dispatched-at nil]]}))))

(defn release-terminal-dispatch-claim!
  "Reverse a `claim-terminal-dispatch!` after a failed send so the next
   reconciliation pass retries. Guarded so the UPDATE only hits rows
   whose `terminal_event_dispatched_at` is actually set — a stray
   release call against an unclaimed row (double-release, wrong call
   path) is a 0-row no-op rather than a silent rewrite that could
   shadow the claim state from other callers.

   Intended usage:

       (when (claim-terminal-dispatch! ds job-id)
         (try
           (send-terminal-event! …)
           (catch Exception e
             (release-terminal-dispatch-claim! ds job-id)
             (throw e))))"
  [ds job-id]
  (core/execute! ds
    {:update :statechart-jobs
     :set {:terminal-event-dispatched-at nil
           :updated-at                   [:now]}
     :where [:and
             [:= :id job-id]
             [:is-not :terminal-event-dispatched-at nil]]}))

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
