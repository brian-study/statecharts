(ns com.fulcrologic.statecharts.jobs.worker
  "Background worker for executing durable jobs.

   The worker claims jobs from the job store, executes registered handler functions,
   and dispatches terminal events (done/error) back to the statechart session via
   the event queue.

   Features:
   - Lease-based ownership prevents duplicate execution
   - Heartbeat keeps lease alive during long-running jobs
   - continue-fn lets handlers check for cancellation and lease validity
   - Optimistic retry wrapper for data model updates (I5)
   - Reconciliation loop for crash-safe terminal event dispatch
   - Exponential backoff for retryable failures"
  (:require
   [com.fulcrologic.statecharts.events :as evts]
   [com.fulcrologic.statecharts.persistence.pg.job-store :as job-store]
   [com.fulcrologic.statecharts.protocols :as sp]
   [taoensso.timbre :as log])
  (:import
   [java.util.concurrent Callable ExecutorService Executors LinkedBlockingQueue RejectedExecutionException Semaphore TimeUnit]))

;; -----------------------------------------------------------------------------
;; Optimistic Retry (I5)
;; -----------------------------------------------------------------------------

(defn- with-optimistic-retry
  "Wrap a function with optimistic retry for working memory save conflicts.
   Retries up to max-retries times with a brief sleep between attempts."
  [f max-retries]
  (loop [attempt 0]
    (let [result (try
                   {:ok (f)}
                   (catch clojure.lang.ExceptionInfo e
                     (if (and (< attempt max-retries)
                              (re-find #"(?i)optimistic lock" (or (.getMessage e) "")))
                       (do
                         (log/debug "Optimistic lock conflict, retrying"
                                    {:attempt attempt :max-retries max-retries})
                         (Thread/sleep (min 100 (* 10 (inc attempt))))
                         {:retry true})
                       (throw e))))]
      (if (:retry result)
        (recur (inc attempt))
        (:ok result)))))

;; -----------------------------------------------------------------------------
;; Handler Context
;; -----------------------------------------------------------------------------

(defn- make-update-fn
  "Create an update function that wraps data model updates with optimistic retry.
   The update-fn takes a session-id and a map of data updates."
  [update-data-by-id-fn max-retries]
  (fn [session-id data-updates]
    (with-optimistic-retry
      #(update-data-by-id-fn session-id data-updates)
      max-retries)))

(defn- make-continue-fn
  "Create a continue function that checks both lease ownership and cancellation.
   Returns true only if the worker still owns the lease AND the job hasn't been cancelled.
   On false, handler must stop immediately."
  [pool job-id owner-id lease-duration-seconds]
  (fn []
    (try
      (and (job-store/heartbeat! pool job-id owner-id lease-duration-seconds)
           (not (job-store/job-cancelled? pool job-id)))
      (catch Exception e
        (log/warn e "continue-fn failed (treating as lease lost)"
                  {:job-id job-id :owner-id owner-id})
        false))))

;; -----------------------------------------------------------------------------
;; Terminal Event Dispatch
;; -----------------------------------------------------------------------------

(defn- dispatch-terminal-event!
  "Dispatch a terminal event via the event queue.
   Verifies the parent session is in the expected state with matching job-id
   before dispatching (I3, I4).

   Returns true if dispatched, false if session not ready (will be retried by reconciler)."
  [event-queue env job {:keys [get-session-state-fn wake-event-loop-fn]}]
  (let [{:keys [id session-id invokeid terminal-event-name terminal-event-data]} job
        event-name (clojure.edn/read-string terminal-event-name)
        do-send! (fn []
                   (sp/send! event-queue env
                     {:event event-name
                      :target session-id
                      :data (or terminal-event-data {})})
                   (when wake-event-loop-fn (wake-event-loop-fn)))]
    (if-let [check-result (when get-session-state-fn
                            (get-session-state-fn session-id invokeid (str id)))]
      (if (:ready check-result)
        (do
          (do-send!)
          (log/info "Terminal event dispatched"
                    {:job-id id :event terminal-event-name :session-id session-id})
          true)
        (do
          (log/debug "Session not ready for terminal event"
                     {:job-id id :session-id session-id :reason (:reason check-result)})
          false))
      ;; No session state checker — dispatch unconditionally
      (do
        (do-send!)
        (log/info "Terminal event dispatched (no session check)"
                  {:job-id id :event terminal-event-name :session-id session-id})
        true))))

;; -----------------------------------------------------------------------------
;; Job Execution
;; -----------------------------------------------------------------------------

(defn- execute-job!
  "Execute a single claimed job. Calls the handler and handles terminal event storage."
  [pool event-queue env job handler-fn owner-id
   {:keys [lease-duration-seconds update-data-by-id-fn
           max-update-retries]
    :or {lease-duration-seconds 60 max-update-retries 5}
    :as exec-opts}]
  (let [{:keys [id session-id invokeid payload attempt max-attempts]} job
        update-fn (make-update-fn update-data-by-id-fn max-update-retries)
        continue-fn (make-continue-fn pool id owner-id lease-duration-seconds)
        done-event-name (pr-str (evts/invoke-done-event invokeid))
        error-event-name (pr-str (evts/invoke-error-event invokeid))]
    (try
      (let [result (handler-fn {:job-id      (str id)
                                :params      payload
                                :session-id  session-id
                                :update-fn   update-fn
                                :continue-fn continue-fn})]
        ;; Final continue check before terminal write (I8)
        (if (continue-fn)
          (let [event-data (merge (when (map? result) result)
                                  {:job-id (str id)})]
            ;; Store terminal state + event in one write
            (if (job-store/complete! pool id owner-id result done-event-name event-data)
              ;; Try immediate dispatch
              (when (dispatch-terminal-event! event-queue env
                      (assoc job :terminal-event-name done-event-name
                                 :terminal-event-data event-data)
                      exec-opts)
                (job-store/mark-terminal-event-dispatched! pool id))
              (log/info "Skipping completion write; job no longer active/owned"
                        {:job-id id :session-id session-id :owner-id owner-id})))
          (log/info "Job lease lost after execution, abandoning"
                    {:job-id id :session-id session-id})))
      (catch Exception e
        (log/error e "Job handler failed"
                   {:job-id id :session-id session-id :invokeid invokeid
                    :attempt attempt :max-attempts max-attempts})
        (let [error {:message (or (.getMessage e) "Unknown error")
                     :type (str (type e))}
              event-data (merge error {:job-id (str id)})]
          (case (job-store/fail! pool id owner-id attempt max-attempts error
                                 error-event-name event-data)
            :failed
            ;; If permanently failed, try immediate dispatch
            (when (dispatch-terminal-event! event-queue env
                    (assoc job :terminal-event-name error-event-name
                               :terminal-event-data event-data)
                    exec-opts)
              (job-store/mark-terminal-event-dispatched! pool id))
            :retry-scheduled nil
            :ignored
            (log/info "Skipping fail update; job no longer active/owned"
                      {:job-id id :session-id session-id :owner-id owner-id})))))))

;; -----------------------------------------------------------------------------
;; Reconciliation Loop
;; -----------------------------------------------------------------------------

(defn- reconcile-undispatched!
  "Find jobs that completed/failed but whose terminal event wasn't dispatched,
   and dispatch them. Handles the complete-then-crash gap."
  [pool event-queue env {:keys [limit] :or {limit 10} :as exec-opts}]
  (try
    (let [jobs (job-store/get-undispatched-terminal-jobs pool limit)]
      (doseq [job jobs]
        (try
          (when (dispatch-terminal-event! event-queue env job exec-opts)
            (job-store/mark-terminal-event-dispatched! pool (:id job)))
          (catch Exception e
            (log/warn e "Failed to reconcile terminal event"
                      {:job-id (:id job)})))))
    (catch Exception e
      (log/warn e "Reconciliation loop error"))))

(defn- execute-claimed-job!
  [pool event-queue env handlers owner-id exec-opts job]
  (let [job-type (:job-type job)
        handler-fn (or (get handlers (keyword job-type))
                       (get handlers job-type))]
    (if handler-fn
      (execute-job! pool event-queue env job handler-fn owner-id exec-opts)
      (do
        (log/error "No handler registered for job type"
                   {:job-type job-type :job-id (:id job)})
        (let [error {:message (str "No handler for job type: " job-type)}
              invokeid (:invokeid job)
              error-event-name (pr-str (evts/invoke-error-event invokeid))
              event-data (merge error {:job-id (str (:id job))})]
          (when (= :failed
                   (job-store/fail! pool (:id job) owner-id
                                    (:max-attempts job)
                                    (:max-attempts job)
                                    error error-event-name event-data))
            (when (dispatch-terminal-event! event-queue env
                    (assoc job :terminal-event-name error-event-name
                               :terminal-event-data event-data)
                    exec-opts)
              (job-store/mark-terminal-event-dispatched! pool (:id job)))))))))

(defn- shutdown-executor!
  [^ExecutorService executor owner-id shutdown-timeout-ms]
  (when executor
    (.shutdown executor)
    (try
      (when-not (.awaitTermination executor shutdown-timeout-ms TimeUnit/MILLISECONDS)
        (log/warn "Worker executor did not stop in time, forcing shutdownNow"
                  {:owner-id owner-id :shutdown-timeout-ms shutdown-timeout-ms})
        (.shutdownNow executor)
        (when-not (.awaitTermination executor shutdown-timeout-ms TimeUnit/MILLISECONDS)
          (log/warn "Worker executor still running after shutdownNow"
                    {:owner-id owner-id :shutdown-timeout-ms shutdown-timeout-ms})))
      (catch InterruptedException e
        (.shutdownNow executor)
        (.interrupt (Thread/currentThread))
        (log/warn e "Interrupted while stopping worker executor"
                  {:owner-id owner-id})))))

;; -----------------------------------------------------------------------------
;; Worker Lifecycle
;; -----------------------------------------------------------------------------

(defn start-worker!
  "Start a background job worker with demand-driven claiming.

   The worker tracks available concurrency slots. As soon as a job finishes,
   the coordinator claims more work to fill the freed slot. No batch wait.

   Options:
   - :pool - pg2 connection pool (REQUIRED)
   - :event-queue - Event queue for terminal event dispatch (REQUIRED)
   - :env - Statechart env map (REQUIRED, used for event dispatch)
   - :handlers - Map of job-type keyword to handler fn (REQUIRED)
   - :owner-id - Unique worker identifier (optional, auto-generated)
   - :poll-interval-ms - How often to check for jobs when idle (default 1000)
   - :claim-limit - Max jobs to claim per poll (default 5). Clamped to concurrency
     internally: effective-claim-limit = min(claim-limit, concurrency)
   - :concurrency - Max concurrent job executions (default claim-limit)
   - :lease-duration-seconds - How long a lease lasts (default 60)
   - :update-data-by-id-fn - Function to update data model by session-id (REQUIRED)
   - :get-session-state-fn - Function to check session state for I3/I4 (optional)
   - :wake-event-loop-fn - Function to wake event loop after terminal dispatch (optional)
   - :max-update-retries - Max retries for optimistic lock conflicts (default 5)
   - :reconcile-interval-polls - Run reconciliation every N polls (default 10)
   - :shutdown-timeout-ms - Wait timeout for executor shutdown (default 5000)

   Handlers receive: {:keys [job-id params session-id update-fn continue-fn]}
   - update-fn: (fn [session-id data-map]) - update data model with retry
   - continue-fn: (fn []) - returns true if worker should continue, false to stop

   Returns a map with :stop! and :wake! functions.
   `stop!` blocks for up to ~4x shutdown-timeout-ms (2x for coordinator drain + 2x for executor shutdown).

   NOTE: Dynamic bindings active at start-worker! call time are captured via bound-fn
   and propagated to all handler invocations on executor threads. If your handlers rely
   on dynamic vars (e.g., *conn*, *env*), ensure they are bound when calling start-worker!."
  [{:keys [pool event-queue env handlers owner-id
           poll-interval-ms claim-limit concurrency
           lease-duration-seconds
           update-data-by-id-fn get-session-state-fn wake-event-loop-fn
           max-update-retries reconcile-interval-polls shutdown-timeout-ms]
    :or {owner-id (str "worker-" (random-uuid))
         poll-interval-ms 1000
         claim-limit 5
         lease-duration-seconds 60
         max-update-retries 5
         reconcile-interval-polls 10
         shutdown-timeout-ms 5000}}]
  (assert pool "pool is required")
  (assert event-queue "event-queue is required")
  (assert env "env is required")
  (assert handlers "handlers map is required")
  (assert update-data-by-id-fn "update-data-by-id-fn is required")
  (let [running (atom true)
        stopping? (atom false)
        wake-signal (LinkedBlockingQueue. 1)
        concurrency (max 1 (long (or concurrency claim-limit)))
        claim-limit (max 1 (long claim-limit))
        effective-claim-limit (min claim-limit concurrency)
        executor (Executors/newFixedThreadPool (int concurrency))
        sem (Semaphore. concurrency)
        exec-opts {:lease-duration-seconds lease-duration-seconds
                   :update-data-by-id-fn update-data-by-id-fn
                   :get-session-state-fn get-session-state-fn
                   :wake-event-loop-fn wake-event-loop-fn
                   :max-update-retries max-update-retries}
        ;; Preserve dynamic bindings from start-worker! caller so handlers that
        ;; rely on dynamic vars keep working under concurrent execution.
        ;; Note: no @running/@stopping? guard here — once a job is claimed from
        ;; the DB and submitted to the executor, it MUST execute. The executor's
        ;; awaitTermination handles draining on shutdown. Skipping would orphan
        ;; the claimed job until its lease expires.
        execute-one-job! (bound-fn [job]
                           (execute-claimed-job! pool event-queue env handlers owner-id exec-opts job))
        submit-job! (fn [job]
                      (try
                        (.submit ^ExecutorService executor
                          ^Callable (fn []
                            (try
                              (execute-one-job! job)
                              (finally
                                (.release sem)
                                (.offer wake-signal :job-done)))))
                        (catch Exception e
                          ;; Release permit for any submission failure (not just
                          ;; RejectedExecutionException) to prevent permit leak
                          (.release sem)
                          (when-not (instance? RejectedExecutionException e)
                            (log/error e "Failed to submit job to executor"
                                       {:job-id (:id job)})))))
        loop-fn (fn []
                  (log/info "Job worker started"
                            {:owner-id owner-id
                             :poll-interval-ms poll-interval-ms
                             :claim-limit claim-limit
                             :effective-claim-limit effective-claim-limit
                             :concurrency concurrency
                             :handler-types (vec (keys handlers))})
                  (loop [poll-count 0]
                    (when @running
                      (try
                        (let [available (.drainPermits sem)]
                          (if (pos? available)
                            ;; Capacity available — claim up to available jobs
                            (let [jobs (try
                                         (job-store/claim-jobs! pool
                                           {:owner-id owner-id
                                            :lease-duration-seconds lease-duration-seconds
                                            :limit (min available effective-claim-limit)})
                                         (catch Exception e
                                           ;; Restore all drained permits so the semaphore
                                           ;; doesn't get stuck at 0 on claim failure
                                           (.release sem available)
                                           (throw e)))]
                              ;; Return permits for slots we didn't fill
                              (.release sem (- available (count jobs)))
                              (if (seq jobs)
                                (doseq [job jobs]
                                  (submit-job! job))
                                ;; No jobs in DB — wait for wake or poll interval
                                (.poll wake-signal poll-interval-ms TimeUnit/MILLISECONDS)))
                            ;; All slots busy — wait for a job to complete or wake
                            (.poll wake-signal poll-interval-ms TimeUnit/MILLISECONDS)))
                        ;; Periodic reconciliation. availablePermits is a racy read —
                        ;; a job could complete between drainPermits and here — but
                        ;; that's fine: the check is a heuristic to skip reconciliation
                        ;; when the worker is clearly saturated. False negatives just
                        ;; defer reconciliation to the next eligible cycle.
                        (when (and (zero? (mod poll-count reconcile-interval-polls))
                                   (pos? (.availablePermits sem)))
                          (reconcile-undispatched! pool event-queue env exec-opts))
                        (catch InterruptedException e
                          (.interrupt (Thread/currentThread))
                          (log/warn "Worker thread interrupted" {:owner-id owner-id}))
                        (catch Throwable e
                          (log/error e "Job worker error" {:owner-id owner-id})))
                      (recur (inc poll-count))))
                  (log/info "Job worker stopped" {:owner-id owner-id}))]
    (let [worker-future (future (loop-fn))]
      {:stop! (fn []
                (when (compare-and-set! stopping? false true)
                  (log/info "Job worker stop requested" {:owner-id owner-id})
                  (reset! running false)
                  (.offer wake-signal :stop)
                  ;; Wait for coordinator loop to finish before shutting down
                  ;; executor. This ensures any in-flight claim-jobs! completes
                  ;; and submits its jobs before the executor rejects new work.
                  ;; Timeout prevents stop! from hanging if the coordinator is
                  ;; stuck in a hung DB query (connection pool exhaustion, etc.).
                  (try
                    (deref worker-future (* 2 shutdown-timeout-ms) nil)
                    (catch Exception e
                      (log/warn e "Worker loop ended with error"
                                {:owner-id owner-id})))
                  (when-not (future-done? worker-future)
                    (log/warn "Coordinator did not exit in time, cancelling"
                              {:owner-id owner-id
                               :timeout-ms (* 2 shutdown-timeout-ms)})
                    (future-cancel worker-future))
                  (shutdown-executor! executor owner-id shutdown-timeout-ms)))
       :wake! (fn []
                (.offer wake-signal :wake))})))

(defn wake!
  "Wake the worker to check for new jobs immediately.
   Used after creating a job to minimize latency."
  [wake-signal]
  (when wake-signal
    (.offer ^LinkedBlockingQueue wake-signal :wake)))
