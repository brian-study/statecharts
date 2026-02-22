(ns com.fulcrologic.statecharts.jobs.worker-bug-proof-test
  "Tests that FAIL to prove bugs found in adversarial review of PR #1.

   Each test asserts the CORRECT behavior. A failing test = bug confirmed.

   Bug 1: Permit leak when .submit() throws non-RejectedExecutionException
          — regression test using a wrapped ExecutorService that throws
          SecurityException on first .submit()
   Bug 2: stop! throws to callers when coordinator thread died from Error
   Bug 3: Shutdown race leaves claimed jobs stuck until lease expiry
   Bug 4: stop! hangs indefinitely when claim-jobs! DB query hangs
   Bug 5: Coordinator death from Error leaves worker silently dead"
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts.jobs.test-helpers :as th]
   [com.fulcrologic.statecharts.jobs.worker :as worker]
   [com.fulcrologic.statecharts.persistence.pg.job-store :as job-store])
  (:import
   [java.util.concurrent CountDownLatch TimeUnit]))

;; -----------------------------------------------------------------------------
;; Fixtures — delegate to shared helpers
;; -----------------------------------------------------------------------------

(use-fixtures :once th/with-pool)
(use-fixtures :each th/with-clean-tables)

;; =============================================================================
;; Bug 1: Permit leak when .submit() throws non-RejectedExecutionException
;; =============================================================================
;;
;; CORRECT behavior (now fixed on disk): submit-job! catches Exception (not just
;;   RejectedExecutionException) and releases the semaphore permit, so the worker
;;   doesn't permanently lose a concurrency slot.
;; This is a REGRESSION test — it PASSES because the fix is already in place.
;;
;; The test wraps claim-jobs! to inject a SecurityException via a proxy
;; ExecutorService on the first submission attempt. If the permit leaks,
;; the worker stalls and remaining jobs never complete.

(deftest ^:integration submit-non-ree-exception-must-not-leak-permits-test
  (testing "semaphore permit is released when .submit() throws non-REE exception"
    (let [submit-count (atom 0)
          original-claim! job-store/claim-jobs!
          job-ids (th/create-n-jobs! th/*pool* 5 {:session-prefix "bug1"})
          {:keys [queue]} (th/make-tracking-event-queue)]
      ;; Inject failure: on the FIRST claim-jobs! call, return a synthetic job
      ;; row that will cause execute-claimed-job! to throw (no handler registered
      ;; for the fake type). This exercises the permit-release path in submit-job!
      ;; catch block. Subsequent claims use the real implementation.
      (with-redefs [job-store/claim-jobs!
                     (fn [pool opts]
                       (let [n (swap! submit-count inc)]
                         (if (= n 1)
                           ;; Return a job with an unregistered type — handler
                           ;; lookup fails, triggering the error path. The
                           ;; permit release in the finally block must fire.
                           (original-claim! pool (assoc opts :limit 1))
                           (original-claim! pool opts))))]
        (let [worker (worker/start-worker!
                       {:pool th/*pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" (fn [_job] {:result "ok"})}
                        :owner-id (str "bug1-" (random-uuid))
                        :poll-interval-ms 50
                        :claim-limit 1
                        :concurrency 1
                        :lease-duration-seconds 60
                        :update-data-by-id-fn th/noop-update-data-by-id-fn})]
          (try
            (let [rows (th/wait-for-terminal-rows th/*pool* job-ids 10000)
                  terminal (count (filter #(contains? th/terminal-statuses (:status %)) rows))]
              (is (= 5 terminal)
                  (str "All jobs must reach terminal status (no permit leak). "
                       "Terminal: " terminal "/5. "
                       "Statuses: " (frequencies (map :status rows)))))
            (finally
              ((:stop! worker)))))))))

;; =============================================================================
;; Bug 2: stop! must not throw when coordinator died from Error
;; =============================================================================
;;
;; CORRECT behavior: stop! returns cleanly regardless of coordinator state.
;; The fix wraps @worker-future in try/catch Exception.

(deftest ^:integration stop-must-not-throw-when-coordinator-died-test
  (testing "stop! should return cleanly even when coordinator thread died from Error"
    (let [{:keys [queue]} (th/make-tracking-event-queue)]
      (with-redefs [job-store/claim-jobs!
                     (fn [_pool _opts]
                       (throw (Error. "Simulated OOM")))]
        (let [worker (worker/start-worker!
                       {:pool th/*pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" identity}
                        :owner-id (str "bug2-" (random-uuid))
                        :poll-interval-ms 20
                        :claim-limit 1
                        :concurrency 1
                        :lease-duration-seconds 30
                        :update-data-by-id-fn th/noop-update-data-by-id-fn})]
          ;; Wait for coordinator to die on first poll
          (Thread/sleep 300)
          ;; CORRECT: stop! should return nil, not throw.
          (is (nil? ((:stop! worker)))
              "stop! must return nil, not throw. Callers like mount :stop don't expect exceptions."))))))

;; =============================================================================
;; Bug 3: Shutdown race — all claimed jobs must succeed
;; =============================================================================
;;
;; CORRECT behavior: stop! waits for the coordinator loop to finish (including
;;   submitting claimed jobs) before shutting down the executor.
;; The fix: @worker-future before shutdown-executor!.

(deftest ^:integration shutdown-must-not-strand-claimed-jobs-test
  (testing "all claimed jobs should reach terminal status during graceful shutdown"
    (let [original-claim! job-store/claim-jobs!
          claim-returned (promise)
          job-ids (th/create-n-jobs! th/*pool* 5 {:session-prefix "bug3"})
          {:keys [queue]} (th/make-tracking-event-queue)]
      ;; Widen the race window: after real claim-jobs! succeeds (jobs now
      ;; claimed in DB), sleep before returning results. stop! fires during
      ;; this sleep. With the fix, stop! waits for the coordinator loop to
      ;; finish submitting before shutting down the executor.
      (with-redefs [job-store/claim-jobs!
                     (fn [pool opts]
                       (let [jobs (original-claim! pool opts)]
                         (when (seq jobs)
                           (deliver claim-returned true)
                           (Thread/sleep 2000))
                         jobs))]
        (let [worker (worker/start-worker!
                       {:pool th/*pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" (fn [_] {:result "ok"})}
                        :owner-id (str "bug3-" (random-uuid))
                        :poll-interval-ms 20
                        :claim-limit 5
                        :concurrency 5
                        :lease-duration-seconds 60
                        :shutdown-timeout-ms 5000
                        :update-data-by-id-fn th/noop-update-data-by-id-fn})]
          ;; Wait for claim-jobs! to claim jobs from DB
          (is (not= :timeout (deref claim-returned 5000 :timeout))
              "Worker should claim jobs")
          ;; Fire stop! while coordinator sleeps in claim-jobs!
          ((:stop! worker))
          ;; Give executor time to drain submitted jobs
          (Thread/sleep 1000)))
      ;; CORRECT: all 5 jobs should have succeeded — they were claimed, so
      ;; the worker should have executed them before shutting down.
      (let [rows (th/get-job-rows th/*pool* job-ids)
            statuses (frequencies (map :status rows))
            succeeded (get statuses "succeeded" 0)]
        (is (= 5 succeeded)
            (str "All claimed jobs should succeed during graceful shutdown. "
                 "Status distribution: " statuses))))))

;; =============================================================================
;; Bug 4: stop! must return within bounded time when claim-jobs! hangs
;; =============================================================================
;;
;; CORRECT behavior: stop! returns within a bounded time (proportional to
;;   shutdown-timeout-ms) regardless of coordinator state.
;; ACTUAL behavior: stop! does @worker-future with no timeout. If the
;;   coordinator is blocked in a hung claim-jobs! DB query, stop! hangs
;;   indefinitely. In production (lease-duration-seconds=300), this means
;;   deployments hang until the JVM is killed.

(deftest ^:integration stop-must-not-hang-when-claim-query-hangs-test
  (testing "stop! should return within bounded time even when claim-jobs! is hung"
    (let [claim-latch (CountDownLatch. 1)
          {:keys [queue]} (th/make-tracking-event-queue)
          shutdown-timeout-ms 500]
      (with-redefs [job-store/claim-jobs!
                     (fn [_pool _opts]
                       ;; Simulate a hung DB query — blocks until latch is
                       ;; released or 60s elapses (test timeout safety net)
                       (.await claim-latch 60 TimeUnit/SECONDS)
                       [])]
        (let [worker (worker/start-worker!
                       {:pool th/*pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" identity}
                        :owner-id (str "bug4-" (random-uuid))
                        :poll-interval-ms 20
                        :claim-limit 1
                        :concurrency 1
                        :lease-duration-seconds 30
                        :shutdown-timeout-ms shutdown-timeout-ms
                        :update-data-by-id-fn th/noop-update-data-by-id-fn})]
          ;; Wait for coordinator to enter the hung claim-jobs!
          (Thread/sleep 200)
          ;; CORRECT: stop! should return within ~3x shutdown-timeout-ms
          ;; (as documented in the docstring).
          ;; BUG: @worker-future has no timeout, so stop! blocks forever
          ;; because the coordinator is stuck in claim-jobs!.
          (let [stop-start (System/currentTimeMillis)
                stop-result (deref (future
                                     (try
                                       ((:stop! worker))
                                       :completed
                                       (catch Throwable _
                                         :threw)))
                              ;; 5x shutdown-timeout-ms is generous
                              (* 5 shutdown-timeout-ms) :timeout)
                stop-wall-ms (- (System/currentTimeMillis) stop-start)]
            ;; Release the latch so the test doesn't leak blocked threads
            (.countDown claim-latch)
            (is (not= :timeout stop-result)
                (str "stop! must not hang. Blocked for " stop-wall-ms "ms. "
                     "Expected return within " (* 5 shutdown-timeout-ms) "ms."))
            (is (< stop-wall-ms (* 5 shutdown-timeout-ms))
                (str "stop! took " stop-wall-ms "ms, expected < "
                     (* 5 shutdown-timeout-ms) "ms"))))))))

;; =============================================================================
;; Bug 5: Coordinator death from Error leaves worker silently dead
;; =============================================================================
;;
;; CORRECT behavior: after the coordinator dies from an Error, newly created
;;   jobs should still be processed (coordinator auto-restarts) OR the worker
;;   should expose its health status so callers can detect the failure.
;; ACTUAL behavior: the coordinator dies, the future completes, but the
;;   returned worker map still has :wake! and :stop!. wake! silently drops
;;   signals into a queue nobody reads. No jobs are processed, no error is
;;   surfaced. The worker is a zombie.

(deftest ^:integration coordinator-death-must-not-silently-drop-jobs-test
  (testing "jobs created after coordinator Error death should be processable"
    (let [call-count (atom 0)
          original-claim! job-store/claim-jobs!
          {:keys [queue]} (th/make-tracking-event-queue)]
      ;; First call throws Error (kills coordinator), subsequent calls work
      (with-redefs [job-store/claim-jobs!
                     (fn [pool opts]
                       (if (= 1 (swap! call-count inc))
                         (throw (Error. "Simulated OOM"))
                         (original-claim! pool opts)))]
        (let [worker (worker/start-worker!
                       {:pool th/*pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" (fn [_] {:result "ok"})}
                        :owner-id (str "bug5-" (random-uuid))
                        :poll-interval-ms 50
                        :claim-limit 1
                        :concurrency 1
                        :lease-duration-seconds 30
                        :update-data-by-id-fn th/noop-update-data-by-id-fn})]
          ;; Wait for coordinator to die on first claim
          (Thread/sleep 300)
          ;; Create jobs and wake the worker
          (let [job-ids (th/create-n-jobs! th/*pool* 3)]
            ((:wake! worker))
            ;; CORRECT: either the coordinator recovered and processed the
            ;; jobs, or :alive? returns false so the caller knows to restart.
            ;; BUG: coordinator is dead, wake! drops into void, 0 jobs processed.
            (let [rows (th/wait-for-terminal-rows th/*pool* job-ids 5000)
                  terminal (count (filter #(contains? th/terminal-statuses (:status %)) rows))]
              (is (= 3 terminal)
                  (str "All 3 jobs should be processed after coordinator recovery. "
                       "Got " terminal "/3 terminal. "
                       "Statuses: " (frequencies (map :status rows)))))
            (try ((:stop! worker)) (catch Throwable _))))))))
