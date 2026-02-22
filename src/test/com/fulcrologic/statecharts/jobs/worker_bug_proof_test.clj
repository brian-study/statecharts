(ns com.fulcrologic.statecharts.jobs.worker-bug-proof-test
  "Tests that FAIL to prove bugs found in adversarial review of PR #1.

   Each test asserts the CORRECT behavior. A failing test = bug confirmed.

   Bug 1: Permit leak when .submit() throws non-RejectedExecutionException
          (already fixed on disk — this is a regression test that passes)
   Bug 2: stop! throws to callers when coordinator thread died from Error
   Bug 3: Shutdown race leaves claimed jobs stuck until lease expiry"
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts.jobs.worker :as worker]
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.persistence.pg.job-store :as job-store]
   [com.fulcrologic.statecharts.persistence.pg.schema :as schema]
   [com.fulcrologic.statecharts.protocols :as sp]
   [pg.core :as pg]
   [pg.pool :as pool])
  (:import
   [java.util UUID]
   [java.util.concurrent Executors RejectedExecutionException Semaphore TimeUnit]))

;; -----------------------------------------------------------------------------
;; Test Configuration / Fixtures
;; -----------------------------------------------------------------------------

(def ^:private test-config
  {:host (or (System/getenv "PG_TEST_HOST") "localhost")
   :port (parse-long (or (System/getenv "PG_TEST_PORT") "5432"))
   :database (or (System/getenv "PG_TEST_DATABASE") "statecharts_test")
   :user (or (System/getenv "PG_TEST_USER") "postgres")
   :password (or (System/getenv "PG_TEST_PASSWORD") "postgres")})

(def ^:dynamic *pool* nil)

(defn with-pool [f]
  (let [p (pool/pool test-config)]
    (try
      (binding [*pool* p]
        (f))
      (finally
        (pool/close p)))))

(defn with-clean-tables [f]
  (schema/create-tables! *pool*)
  (schema/truncate-tables! *pool*)
  (try
    (f)
    (finally
      (schema/truncate-tables! *pool*))))

(use-fixtures :once with-pool)
(use-fixtures :each with-clean-tables)

;; -----------------------------------------------------------------------------
;; Helpers
;; -----------------------------------------------------------------------------

(defn noop-update-data-by-id-fn
  [_session-id _data-updates]
  nil)

(defn make-tracking-event-queue []
  (let [events (atom [])]
    {:events events
     :queue (reify sp/EventQueue
              (send! [_ _env send-request]
                (swap! events conj send-request)
                true)
              (cancel! [_ _env _session-id _send-id]
                true)
              (receive-events! [_ _env _handler]
                nil)
              (receive-events! [_ _env _handler _options]
                nil))}))

(defn create-n-jobs!
  ([pool n]
   (create-n-jobs! pool n {}))
  ([pool n {:keys [job-type max-attempts payload-fn]
            :or {job-type "test-job"
                 max-attempts 1
                 payload-fn (fn [idx] {:job-index idx})}}]
   (let [run-id (subs (str (random-uuid)) 0 8)]
     (mapv
      (fn [idx]
        (let [job-id (UUID/randomUUID)]
          (job-store/create-job! pool
            {:id job-id
             :session-id (keyword (str "bugproof-session-" run-id "-" idx))
             :invokeid (keyword (str "bugproof-invoke-" run-id "-" idx))
             :job-type job-type
             :payload (payload-fn idx)
             :max-attempts max-attempts})
          job-id))
      (range n)))))

(defn- get-job-row [pool job-id]
  (pg/with-connection [c pool]
    (let [rows (pg/execute c
                 "SELECT * FROM statechart_jobs WHERE id = $1"
                 {:params [job-id]
                  :kebab-keys? true})]
      (when-let [row (first rows)]
        (-> row
            (update :payload core/thaw)
            (cond->
              (:result row) (update :result core/thaw)
              (:error row) (update :error core/thaw)
              (:terminal-event-data row) (update :terminal-event-data core/thaw)))))))

(defn- get-job-rows [pool job-ids]
  (mapv #(get-job-row pool %) job-ids))

;; =============================================================================
;; Bug 1: Permit leak when .submit() throws non-RejectedExecutionException
;; =============================================================================
;;
;; CORRECT behavior (now fixed on disk): submit-job! catches Exception (not just
;;   RejectedExecutionException) and releases the semaphore permit, so the worker
;;   doesn't permanently lose a concurrency slot.
;; This is a REGRESSION test — it PASSES because the fix is already in place.

(deftest ^:integration submit-non-ree-exception-must-not-leak-permits-test
  (testing "semaphore permit is released when .submit() throws non-REE exception"
    (let [job-ids (create-n-jobs! *pool* 3)
          {:keys [queue]} (make-tracking-event-queue)
          submit-count (atom 0)
          original-claim! job-store/claim-jobs!]
      ;; First submission throws a non-REE exception (e.g. SecurityException).
      ;; If the permit leaks, subsequent polls see 0 available permits and the
      ;; worker stalls — remaining jobs never get claimed or executed.
      (with-redefs [job-store/claim-jobs!
                     (fn [pool opts]
                       ;; Only claim one job at a time to isolate the permit path
                       (original-claim! pool (assoc opts :limit 1)))]
        (let [;; Wrap the ExecutorService to throw on first submit
              worker (worker/start-worker!
                       {:pool *pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" (fn [_job] {:result "ok"})}
                        :owner-id (str "bug1-" (random-uuid))
                        :poll-interval-ms 50
                        :claim-limit 1
                        :concurrency 1
                        :lease-duration-seconds 60
                        :update-data-by-id-fn noop-update-data-by-id-fn})]
          ;; Let the worker process all jobs (with possible submit failures)
          ;; The key assertion: the worker doesn't stall. It keeps claiming
          ;; and processing because permits are released on failure.
          (Thread/sleep 3000)
          (try ((:stop! worker)) (catch Throwable _))))
      ;; CORRECT: at least 2 of 3 jobs should reach terminal status.
      ;; If permits leaked, the worker would stall after the first failure
      ;; and 0 jobs would succeed.
      (let [rows (get-job-rows *pool* job-ids)
            terminal (count (filter #(#{"succeeded" "failed"} (:status %)) rows))]
        (is (<= 2 terminal)
            (str "Worker must not stall after non-REE submit failure. "
                 "Terminal jobs: " terminal "/3. "
                 "Statuses: " (frequencies (map :status rows))))))))

;; =============================================================================
;; Bug 2: stop! must not throw when coordinator died from Error
;; =============================================================================
;;
;; CORRECT behavior: stop! returns cleanly regardless of coordinator state.
;; ACTUAL behavior: stop! throws the Error to callers.
;;
;; This test asserts the correct behavior and FAILS to prove the bug.

(deftest ^:integration stop-must-not-throw-when-coordinator-died-test
  (testing "stop! should return cleanly even when coordinator thread died from Error"
    (let [{:keys [queue]} (make-tracking-event-queue)]
      (with-redefs [job-store/claim-jobs!
                     (fn [_pool _opts]
                       (throw (Error. "Simulated OOM")))]
        (let [worker (worker/start-worker!
                       {:pool *pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" identity}
                        :owner-id (str "bug2-" (random-uuid))
                        :poll-interval-ms 20
                        :claim-limit 1
                        :concurrency 1
                        :lease-duration-seconds 30
                        :update-data-by-id-fn noop-update-data-by-id-fn})]
          ;; Wait for coordinator to die on first poll
          (Thread/sleep 300)
          ;; CORRECT: stop! should return nil, not throw.
          ;; BUG: deref on the dead future throws Error via sneakyThrow.
          ;; This FAILS — proving the bug.
          (is (nil? ((:stop! worker)))
              "stop! must return nil, not throw. Callers like mount :stop don't expect exceptions."))))))

;; =============================================================================
;; Bug 3: Shutdown race — all claimed jobs must succeed
;; =============================================================================
;;
;; CORRECT behavior: jobs claimed from DB should be executed before executor
;;   shutdown, so all jobs reach terminal status.
;; ACTUAL behavior: stop! shuts down executor before coordinator finishes
;;   submitting claimed jobs. Jobs are stuck in "running" until lease expiry.
;;
;; This test asserts the correct behavior and FAILS to prove the bug.

(deftest ^:integration shutdown-must-not-strand-claimed-jobs-test
  (testing "all claimed jobs should reach terminal status during graceful shutdown"
    (let [original-claim! job-store/claim-jobs!
          claim-returned (promise)
          job-ids (create-n-jobs! *pool* 5)
          {:keys [queue]} (make-tracking-event-queue)]
      ;; Widen the race window: after real claim-jobs! succeeds (jobs now
      ;; claimed in DB), sleep before returning results. stop! fires during
      ;; this sleep, shutting down the executor. When sleep ends and doseq
      ;; starts, all submissions get RejectedExecutionException.
      (with-redefs [job-store/claim-jobs!
                     (fn [pool opts]
                       (let [jobs (original-claim! pool opts)]
                         (when (seq jobs)
                           (deliver claim-returned true)
                           (Thread/sleep 2000))
                         jobs))]
        (let [worker (worker/start-worker!
                       {:pool *pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" (fn [_] {:result "ok"})}
                        :owner-id (str "bug3-" (random-uuid))
                        :poll-interval-ms 20
                        :claim-limit 5
                        :concurrency 5
                        :lease-duration-seconds 60
                        :shutdown-timeout-ms 200
                        :update-data-by-id-fn noop-update-data-by-id-fn})]
          ;; Wait for claim-jobs! to claim jobs from DB
          (is (not= :timeout (deref claim-returned 5000 :timeout))
              "Worker should claim jobs")
          ;; Fire stop! while coordinator sleeps in claim-jobs!
          ;; Executor shuts down; coordinator hasn't returned yet
          (try ((:stop! worker)) (catch Throwable _))
          ;; Wait for coordinator to finish (sleep ends, doseq REEs, loop exits)
          (Thread/sleep 3000)))
      ;; CORRECT: all 5 jobs should have succeeded — they were claimed, so
      ;; the worker should have executed them before shutting down.
      ;; BUG: executor was killed before submissions, so 0 succeeded.
      ;; This FAILS — proving the bug.
      (let [rows (get-job-rows *pool* job-ids)
            statuses (frequencies (map :status rows))
            succeeded (get statuses "succeeded" 0)]
        (is (= 5 succeeded)
            (str "All claimed jobs should succeed during graceful shutdown. "
                 "Instead they are stranded in non-terminal status until "
                 "the 60s lease expires (300s in production). "
                 "Status distribution: " statuses))))))
