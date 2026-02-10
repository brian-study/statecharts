(ns ^:integration com.fulcrologic.statecharts.jobs.worker-spec
  "Integration tests for the durable job worker.

   These tests require a running PostgreSQL instance.
   Uses the same test configuration as integration-test.clj.

   To run:
     clj -M:test -m kaocha.runner --focus :integration"
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts.events :as evts]
   [com.fulcrologic.statecharts.jobs.worker :as worker]
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.persistence.pg.event-queue :as pg-eq]
   [com.fulcrologic.statecharts.persistence.pg.job-store :as job-store]
   [com.fulcrologic.statecharts.persistence.pg.schema :as schema]
   [com.fulcrologic.statecharts.protocols :as sp]
   [pg.core :as pg]
   [pg.pool :as pool])
  (:import
   [java.util UUID]))

;; -----------------------------------------------------------------------------
;; Test Configuration
;; -----------------------------------------------------------------------------

(def ^:private test-config
  {:host (or (System/getenv "PG_TEST_HOST") "localhost")
   :port (parse-long (or (System/getenv "PG_TEST_PORT") "5432"))
   :database (or (System/getenv "PG_TEST_DATABASE") "statecharts_test")
   :user (or (System/getenv "PG_TEST_USER") "postgres")
   :password (or (System/getenv "PG_TEST_PASSWORD") "postgres")})

(def ^:dynamic *pool* nil)

;; -----------------------------------------------------------------------------
;; Test Fixtures
;; -----------------------------------------------------------------------------

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
;; Test Helpers
;; -----------------------------------------------------------------------------

(defn- create-test-job!
  "Create a job directly in the DB and return the job-id."
  [pool {:keys [session-id invokeid job-type payload max-attempts]
         :or {session-id :test-session
              invokeid :test-invoke
              job-type "test-job"
              payload {:foo "bar"}
              max-attempts 3}}]
  (let [job-id (UUID/randomUUID)]
    (job-store/create-job! pool {:id job-id
                                 :session-id session-id
                                 :invokeid invokeid
                                 :job-type job-type
                                 :payload payload
                                 :max-attempts max-attempts})
    job-id))

(defn- get-job-row
  "Fetch a job row from the DB by id. Returns raw row with thawed fields."
  [pool job-id]
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

(defn- make-tracking-event-queue
  "Create a mock event queue that records all sent events."
  []
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

(defn- noop-update-data-by-id-fn
  "A no-op update-data-by-id function for tests that don't need data model updates."
  [_session-id _data-updates]
  nil)

;; -----------------------------------------------------------------------------
;; Handler Execution Tests
;; -----------------------------------------------------------------------------

(deftest ^:integration handler-receives-correct-keys-test
  (testing "handler receives correct keys"
    (testing "job-id, params, session-id, update-fn, continue-fn are all provided"
      (let [received-keys (promise)
            job-id (create-test-job! *pool* {:payload {:x 1 :y 2}})
            {:keys [queue]} (make-tracking-event-queue)
            handler-fn (fn [ctx]
                         (deliver received-keys (set (keys ctx)))
                         {:result "ok"})
            stop! (worker/start-worker!
                    {:pool *pool*
                     :event-queue queue
                     :env {}
                     :handlers {"test-job" handler-fn}
                     :owner-id "test-worker-keys"
                     :poll-interval-ms 50
                     :update-data-by-id-fn noop-update-data-by-id-fn})]
        (try
          (let [keys-received (deref received-keys 5000 :timeout)]
            (is (not= :timeout keys-received) "Handler should have been called")
            (is (contains? keys-received :job-id))
            (is (contains? keys-received :params))
            (is (contains? keys-received :session-id))
            (is (contains? keys-received :update-fn))
            (is (contains? keys-received :continue-fn)))
          (finally
            (stop!)))))))

(deftest ^:integration handler-receives-correct-values-test
  (testing "handler receives correct param values"
    (let [received-ctx (promise)
          payload {:task "generate" :count 5}
          job-id (create-test-job! *pool* {:session-id :my-session
                                           :payload payload})
          {:keys [queue]} (make-tracking-event-queue)
          handler-fn (fn [ctx]
                       (deliver received-ctx ctx)
                       nil)
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"test-job" handler-fn}
                   :owner-id "test-worker-values"
                   :poll-interval-ms 50
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (let [ctx (deref received-ctx 5000 :timeout)]
          (is (not= :timeout ctx) "Handler should have been called")
          (is (= (str job-id) (:job-id ctx))
              "job-id should be the UUID as a string")
          (is (= payload (:params ctx))
              "params should be the deserialized job payload")
          (is (= :my-session (:session-id ctx))
              "session-id should match the job's session-id")
          (is (fn? (:update-fn ctx))
              "update-fn should be a function")
          (is (fn? (:continue-fn ctx))
              "continue-fn should be a function"))
        (finally
          (stop!))))))

(deftest ^:integration continue-fn-returns-true-when-lease-owned-test
  (testing "continue-fn returns true when lease owned and job not cancelled"
    (let [continue-result (promise)
          job-id (create-test-job! *pool* {})
          {:keys [queue]} (make-tracking-event-queue)
          handler-fn (fn [{:keys [continue-fn]}]
                       (deliver continue-result (continue-fn))
                       nil)
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"test-job" handler-fn}
                   :owner-id "test-worker-continue"
                   :poll-interval-ms 50
                   :lease-duration-seconds 60
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (let [result (deref continue-result 5000 :timeout)]
          (is (not= :timeout result) "Handler should have been called")
          (is (true? result) "continue-fn should return true when lease is owned and job is not cancelled"))
        (finally
          (stop!))))))

(deftest ^:integration continue-fn-returns-false-when-job-cancelled-test
  (testing "continue-fn returns false when job is cancelled in DB (I6)"
    (let [continue-result (promise)
          job-id (create-test-job! *pool* {:session-id :cancel-session
                                           :invokeid :cancel-invoke})
          {:keys [queue]} (make-tracking-event-queue)
          handler-fn (fn [{:keys [continue-fn]}]
                       ;; Cancel the job in the DB while handler is running
                       (job-store/cancel! *pool* :cancel-session :cancel-invoke)
                       ;; Now continue-fn should detect the cancellation
                       (deliver continue-result (continue-fn))
                       nil)
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"test-job" handler-fn}
                   :owner-id "test-worker-cancel"
                   :poll-interval-ms 50
                   :lease-duration-seconds 60
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (let [result (deref continue-result 5000 :timeout)]
          (is (not= :timeout result) "Handler should have been called")
          (is (false? result) "continue-fn should return false when job is cancelled"))
        (finally
          (stop!))))))

(deftest ^:integration handler-return-value-used-for-terminal-event-test
  (testing "handler return value is used for terminal event payload"
    (let [handler-done (promise)
          job-id (create-test-job! *pool* {:invokeid :my-invoke})
          {:keys [queue events]} (make-tracking-event-queue)
          handler-fn (fn [_ctx]
                       (let [result {:generated-id 42 :status "complete"}]
                         (deliver handler-done true)
                         result))
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"test-job" handler-fn}
                   :owner-id "test-worker-return"
                   :poll-interval-ms 50
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (deref handler-done 5000 :timeout)
        ;; Give the worker a moment to finish post-handler processing
        (Thread/sleep 200)
        (let [row (get-job-row *pool* job-id)]
          (is (= "succeeded" (:status row))
              "Job should be marked as succeeded")
          (is (some? (:terminal-event-data row))
              "Terminal event data should be stored")
          (is (= 42 (:generated-id (:terminal-event-data row)))
              "Terminal event data should include handler return value")
          (is (= (str job-id) (:job-id (:terminal-event-data row)))
              "Terminal event data should include job-id"))
        (finally
          (stop!))))))

;; -----------------------------------------------------------------------------
;; Terminal Dispatch Tests
;; -----------------------------------------------------------------------------

(deftest ^:integration handler-success-transitions-to-succeeded-test
  (testing "on handler success, job status transitions to succeeded with terminal event stored"
    (let [handler-done (promise)
          job-id (create-test-job! *pool* {:invokeid :success-invoke})
          {:keys [queue events]} (make-tracking-event-queue)
          handler-fn (fn [_ctx]
                       (deliver handler-done true)
                       {:answer 42})
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"test-job" handler-fn}
                   :owner-id "test-worker-success"
                   :poll-interval-ms 50
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (deref handler-done 5000 :timeout)
        (Thread/sleep 200)
        (let [row (get-job-row *pool* job-id)
              expected-event-name (pr-str (evts/invoke-done-event :success-invoke))]
          (is (= "succeeded" (:status row)))
          (is (= expected-event-name (:terminal-event-name row))
              "Terminal event name should be the done event for the invokeid")
          (is (map? (:terminal-event-data row))
              "Terminal event data should be stored")
          ;; Verify event was dispatched
          (is (pos? (count @events))
              "Terminal event should have been dispatched via event queue"))
        (finally
          (stop!))))))

(deftest ^:integration handler-exception-retries-with-backoff-test
  (testing "on handler exception with retries remaining, job goes to pending with backoff"
    (let [attempt-count (atom 0)
          first-failure-done (promise)
          job-id (create-test-job! *pool* {:invokeid :retry-invoke
                                           :max-attempts 3})
          {:keys [queue]} (make-tracking-event-queue)
          handler-fn (fn [_ctx]
                       (let [n (swap! attempt-count inc)]
                         (when (= n 1)
                           (deliver first-failure-done true))
                         (throw (ex-info "Transient error" {:attempt n}))))
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"test-job" handler-fn}
                   :owner-id "test-worker-retry"
                   :poll-interval-ms 50
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (deref first-failure-done 5000 :timeout)
        ;; Give the worker time to process the failure
        (Thread/sleep 200)
        (let [row (get-job-row *pool* job-id)]
          ;; After first failure (attempt=1, max-attempts=3), should be pending for retry
          (is (= "pending" (:status row))
              "Job should be re-enqueued as pending after retryable failure")
          (is (some? (:next-run-at row))
              "next-run-at should be set for backoff delay")
          (is (nil? (:lease-owner row))
              "Lease should be released on failure")
          (is (some? (:error row))
              "Error should be stored"))
        (finally
          (stop!))))))

(deftest ^:integration handler-exception-max-attempts-exhausted-test
  (testing "on handler exception with max attempts exhausted, job goes to failed"
    (let [handler-done (promise)
          job-id (create-test-job! *pool* {:invokeid :exhausted-invoke
                                           :max-attempts 1})
          {:keys [queue events]} (make-tracking-event-queue)
          handler-fn (fn [_ctx]
                       (deliver handler-done true)
                       (throw (ex-info "Permanent error" {:reason "bad input"})))
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"test-job" handler-fn}
                   :owner-id "test-worker-exhausted"
                   :poll-interval-ms 50
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (deref handler-done 5000 :timeout)
        (Thread/sleep 200)
        (let [row (get-job-row *pool* job-id)
              expected-event-name (pr-str (evts/invoke-error-event :exhausted-invoke))]
          (is (= "failed" (:status row))
              "Job should be marked as permanently failed")
          (is (= expected-event-name (:terminal-event-name row))
              "Terminal event name should be the error event for the invokeid")
          (is (some? (:terminal-event-data row))
              "Terminal event data should be stored with error details")
          (is (string? (:message (:terminal-event-data row)))
              "Error data should include the exception message")
          ;; Verify error event was dispatched
          (is (pos? (count @events))
              "Error terminal event should have been dispatched"))
        (finally
          (stop!))))))

(deftest ^:integration backoff-increases-with-attempts-test
  (testing "backoff increases with attempts (exponential: 2^attempt seconds)"
    ;; The backoff-seconds function in job-store is: min(60, 2^attempt)
    ;; We test this indirectly by checking that next_run_at moves further out
    ;; on successive failures
    (let [attempt-tracker (atom {:runs 0 :first-next-run nil :second-next-run nil})
          second-failure-done (promise)
          job-id (create-test-job! *pool* {:invokeid :backoff-invoke
                                           :max-attempts 5})
          {:keys [queue]} (make-tracking-event-queue)
          handler-fn (fn [_ctx]
                       (let [n (:runs (swap! attempt-tracker update :runs inc))]
                         (when (= n 1)
                           ;; After first failure, record next_run_at
                           (future
                             (Thread/sleep 100)
                             (let [row (get-job-row *pool* job-id)]
                               (swap! attempt-tracker assoc :first-next-run (:next-run-at row)))))
                         (when (= n 2)
                           (future
                             (Thread/sleep 100)
                             (let [row (get-job-row *pool* job-id)]
                               (swap! attempt-tracker assoc :second-next-run (:next-run-at row))
                               (deliver second-failure-done true))))
                         (throw (ex-info "Fail" {:n n}))))
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"test-job" handler-fn}
                   :owner-id "test-worker-backoff"
                   :poll-interval-ms 50
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        ;; Wait for at least two failures to complete
        (deref second-failure-done 15000 :timeout)
        (Thread/sleep 500)
        (let [{:keys [first-next-run second-next-run]} @attempt-tracker]
          (when (and first-next-run second-next-run)
            (is (.isBefore first-next-run second-next-run)
                "Second retry should have a later next-run-at (larger backoff) than the first")))
        (finally
          (stop!))))))

;; -----------------------------------------------------------------------------
;; Worker Lifecycle Tests
;; -----------------------------------------------------------------------------

(deftest ^:integration worker-claims-available-jobs-test
  (testing "worker claims and executes available jobs"
    (let [processed-jobs (atom [])
          done-latch (promise)
          _job-id-1 (create-test-job! *pool* {:session-id :sess-1
                                              :invokeid :inv-1
                                              :payload {:id 1}})
          _job-id-2 (create-test-job! *pool* {:session-id :sess-2
                                              :invokeid :inv-2
                                              :payload {:id 2}})
          {:keys [queue]} (make-tracking-event-queue)
          handler-fn (fn [{:keys [params]}]
                       (swap! processed-jobs conj params)
                       (when (= 2 (count @processed-jobs))
                         (deliver done-latch true))
                       nil)
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"test-job" handler-fn}
                   :owner-id "test-worker-claim"
                   :poll-interval-ms 50
                   :claim-limit 5
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (deref done-latch 5000 :timeout)
        (Thread/sleep 200)
        (is (= 2 (count @processed-jobs))
            "Worker should have processed both jobs")
        (let [ids (set (map :id @processed-jobs))]
          (is (= #{1 2} ids)
              "Both jobs should have been processed"))
        (finally
          (stop!))))))

(deftest ^:integration worker-stops-cleanly-on-shutdown-test
  (testing "worker stops cleanly when stop function is called"
    (let [handler-called (atom false)
          {:keys [queue]} (make-tracking-event-queue)
          ;; No jobs in DB, so handler should never be called
          handler-fn (fn [_ctx]
                       (reset! handler-called true)
                       nil)
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"test-job" handler-fn}
                   :owner-id "test-worker-stop"
                   :poll-interval-ms 50
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      ;; Let the worker run a few poll cycles
      (Thread/sleep 200)
      ;; Stop should return without hanging
      (let [stop-future (future (stop!))
            result (deref stop-future 5000 :timeout)]
        (is (not= :timeout result)
            "stop function should return promptly without hanging"))
      ;; After stop, adding a new job should NOT be processed
      (create-test-job! *pool* {})
      (Thread/sleep 300)
      (is (false? @handler-called)
          "No jobs should be processed after worker is stopped"))))

(deftest ^:integration start-worker-returns-stop-function-test
  (testing "start-worker! returns a function"
    (let [{:keys [queue]} (make-tracking-event-queue)
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"test-job" identity}
                   :owner-id "test-worker-returns-fn"
                   :poll-interval-ms 100
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (is (fn? stop!) "start-worker! should return a stop function")
        (finally
          (stop!))))))

(deftest ^:integration no-handler-for-job-type-fails-permanently-test
  (testing "job with no registered handler fails permanently"
    (let [job-id (create-test-job! *pool* {:job-type "unknown-job-type"
                                           :invokeid :no-handler-invoke})
          {:keys [queue]} (make-tracking-event-queue)
          stop! (worker/start-worker!
                  {:pool *pool*
                   :event-queue queue
                   :env {}
                   :handlers {"other-type" identity}
                   :owner-id "test-worker-no-handler"
                   :poll-interval-ms 50
                   :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        ;; Give the worker time to claim and fail the job
        (Thread/sleep 500)
        (let [row (get-job-row *pool* job-id)]
          (is (= "failed" (:status row))
              "Job with no handler should be marked as permanently failed"))
        (finally
          (stop!))))))
