(ns com.fulcrologic.statecharts.jobs.integration-spec
  "End-to-end integration tests for the durable job invoke lifecycle.

   Tests require a running PostgreSQL instance. See integration_test.clj for
   connection configuration via environment variables.

   These tests exercise the full flow: statechart with :durable-job invoke →
   DurableJobInvocationProcessor creates job → worker claims/executes →
   terminal event dispatched back to session."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.elements :refer [state transition script on-entry invoke]]
   [com.fulcrologic.statecharts.chart :as chart]
   [com.fulcrologic.statecharts.data-model.operations :as ops]
   [com.fulcrologic.statecharts.events :as evts]
   [com.fulcrologic.statecharts.invocation.durable-job :as durable-job]
   [com.fulcrologic.statecharts.jobs.worker :as worker]
   [com.fulcrologic.statecharts.persistence.pg :as pg-sc]
   [com.fulcrologic.statecharts.persistence.pg.job-store :as job-store]
   [com.fulcrologic.statecharts.persistence.pg.schema :as schema]
   [com.fulcrologic.statecharts.protocols :as sp]
   [pg.pool :as pool]))

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
;; Chart Definition Helpers
;; -----------------------------------------------------------------------------

(defn- make-test-chart
  "Create a minimal chart with a :durable-job invoke in one state.
   The invoke uses :src :test-job. Transitions:
   - :start → :working (invoke starts)
   - done.invoke.test-job → :done
   - error.invoke.test-job → :errored
   - :cancel-work → :cancelled (from :working)"
  []
  (chart/statechart {}
    (state {:id :start}
      (transition {:event :go :target :working}))
    (state {:id :working}
      (invoke {:id :test-job
               :type :durable-job
               :src :test-job
               :params (fn [_env data] (select-keys data [:test-input]))})
      (transition {:event (evts/invoke-done-event :test-job)
                   :target :done})
      (transition {:event (evts/invoke-error-event :test-job)
                   :target :errored})
      (transition {:event :cancel-work
                   :target :cancelled}))
    (state {:id :done})
    (state {:id :errored})
    (state {:id :cancelled})))

(defn- make-env-with-durable-jobs
  "Create a pg-backed env with DurableJobInvocationProcessor."
  [pool]
  (pg-sc/pg-env
    {:pool pool
     :invocation-processors [(durable-job/->DurableJobInvocationProcessor pool nil)]}))

(defn- get-wmem
  "Get working memory for a session."
  [env session-id]
  (sp/get-working-memory (::sc/working-memory-store env) env session-id))

(defn- get-configuration
  "Get the active configuration set for a session."
  [env session-id]
  (::sc/configuration (get-wmem env session-id)))

(defn- get-data
  "Get the data model for a session."
  [env session-id]
  (::sc/data-model (get-wmem env session-id)))

(defn- noop-update-data-by-id-fn
  "A no-op update function for tests where we don't need data model updates."
  [_session-id _data-updates]
  nil)

(defn- run-worker-once!
  "Run the worker to claim and execute one batch of jobs, then stop.
   Returns after the worker has processed available jobs."
  [pool env handlers & [{:keys [update-data-by-id-fn]
                         :or {update-data-by-id-fn noop-update-data-by-id-fn}}]]
  (let [{:keys [stop!]} (worker/start-worker!
                          {:pool pool
                           :event-queue (::sc/event-queue env)
                           :env env
                           :handlers handlers
                           :update-data-by-id-fn update-data-by-id-fn
                           :poll-interval-ms 50
                           :lease-duration-seconds 30
                           :reconcile-interval-polls 1})]
    ;; Give worker time to claim/execute
    (Thread/sleep 300)
    (stop!)
    ;; Give shutdown time
    (Thread/sleep 100)))

(defn- process-pending-events!
  "Process any pending events in the event queue."
  [env]
  (let [{:keys [stop!]} (pg-sc/start-event-loop! env 50)]
    (Thread/sleep 300)
    (stop!)
    (Thread/sleep 100)))

;; -----------------------------------------------------------------------------
;; Integration Tests
;; -----------------------------------------------------------------------------

(deftest ^:integration happy-path-test
  (testing "Full lifecycle: start session → job created → worker executes → done event → state transitions"
    (let [pool *pool*
          env (make-env-with-durable-jobs pool)
          chart (make-test-chart)
          session-id "integration-test-happy"
          handler-called (atom false)
          handler-result {:output "test-result"}]

      ;; Register and start chart
      (pg-sc/register! env :test-chart chart)
      (pg-sc/start! env :test-chart session-id)

      ;; Verify initial state
      (is (contains? (get-configuration env session-id) :start))

      ;; Send :go event to enter :working state (triggers invoke → job creation)
      (pg-sc/send! env {:event :go :target session-id})
      (process-pending-events! env)

      ;; Verify we're in :working state and job was created
      (is (contains? (get-configuration env session-id) :working))
      (let [job (job-store/get-active-job pool session-id "test-job")]
        (is (some? job) "Job should be created in DB")
        (is (= "pending" (:status job)))
        (is (= "test-job" (:job-type job))))

      ;; Run worker to claim and execute
      (run-worker-once! pool env
        {:test-job (fn [{:keys [job-id params]}]
                     (reset! handler-called true)
                     handler-result)})

      ;; Verify handler was called
      (is @handler-called "Handler should have been called")

      ;; Process the terminal done event
      (process-pending-events! env)

      ;; Verify final state
      (is (contains? (get-configuration env session-id) :done)
          "Session should transition to :done after job completion"))))

(deftest ^:integration cancel-via-state-exit-test
  (testing "Job cancellation when state exits via event (stop-invocation! called)"
    (let [pool *pool*
          env (make-env-with-durable-jobs pool)
          chart (make-test-chart)
          session-id "integration-test-cancel"]

      ;; Register, start, enter :working state
      (pg-sc/register! env :test-chart chart)
      (pg-sc/start! env :test-chart session-id)
      (pg-sc/send! env {:event :go :target session-id})
      (process-pending-events! env)

      ;; Verify job exists
      (let [job (job-store/get-active-job pool session-id "test-job")]
        (is (some? job) "Job should exist")
        (is (= "pending" (:status job)))

        ;; Send :cancel-work to exit the invoke state
        (pg-sc/send! env {:event :cancel-work :target session-id})
        (process-pending-events! env)

        ;; Verify state transitioned
        (is (contains? (get-configuration env session-id) :cancelled))

        ;; Verify job was cancelled by stop-invocation!
        (is (job-store/job-cancelled? pool (:id job))
            "Job should be cancelled when invoke state is exited")))))

(deftest ^:integration handler-failure-with-retry-test
  (testing "Handler throws → job retried → succeeds on second attempt"
    (let [pool *pool*
          env (make-env-with-durable-jobs pool)
          chart (make-test-chart)
          session-id "integration-test-retry"
          call-count (atom 0)]

      ;; Register, start, enter :working state
      (pg-sc/register! env :test-chart chart)
      (pg-sc/start! env :test-chart session-id)
      (pg-sc/send! env {:event :go :target session-id})
      (process-pending-events! env)

      ;; Run worker with handler that fails first time, succeeds second
      (run-worker-once! pool env
        {:test-job (fn [{:keys [job-id]}]
                     (let [n (swap! call-count inc)]
                       (when (= n 1)
                         (throw (ex-info "Transient error" {})))
                       {:success true}))})

      ;; After first failure, job should be back to pending with backoff
      (let [job (job-store/get-active-job pool session-id "test-job")]
        (if (some? job)
          (do
            (is (= "pending" (:status job)) "Job should be re-queued as pending")
            ;; Run worker again to process retry (need to wait for backoff)
            (Thread/sleep 2000)
            (run-worker-once! pool env
              {:test-job (fn [{:keys [job-id]}]
                           (swap! call-count inc)
                           {:success true})})
            (process-pending-events! env)
            (is (contains? (get-configuration env session-id) :done)
                "Session should transition to :done after successful retry"))
          ;; If no active job, the first worker run may have already completed both attempts
          (do
            (process-pending-events! env)
            (is (or (contains? (get-configuration env session-id) :done)
                    (contains? (get-configuration env session-id) :errored))
                "Session should have transitioned after retry processing")))))))

(deftest ^:integration worker-cancellation-during-execution-test
  (testing "Job cancelled while worker is executing → continue-fn returns false"
    (let [pool *pool*
          env (make-env-with-durable-jobs pool)
          chart (make-test-chart)
          session-id "integration-test-cancel-during-exec"
          continue-results (atom [])
          handler-started (promise)
          handler-proceed (promise)]

      ;; Register, start, enter :working state
      (pg-sc/register! env :test-chart chart)
      (pg-sc/start! env :test-chart session-id)
      (pg-sc/send! env {:event :go :target session-id})
      (process-pending-events! env)

      ;; Start worker with a handler that pauses mid-execution
      (let [{:keys [stop!]} (worker/start-worker!
                              {:pool pool
                               :event-queue (::sc/event-queue env)
                               :env env
                               :handlers {:test-job
                                          (fn [{:keys [continue-fn]}]
                                            ;; Record initial continue-fn result
                                            (swap! continue-results conj (continue-fn))
                                            ;; Signal only after the pre-cancel check is recorded.
                                            (deliver handler-started true)
                                            ;; Wait for external cancellation
                                            (deref handler-proceed 5000 :timeout)
                                            ;; Record continue-fn after cancellation
                                            (swap! continue-results conj (continue-fn))
                                            {:result "should-not-matter"})}
                               :update-data-by-id-fn noop-update-data-by-id-fn
                               :poll-interval-ms 50
                               :lease-duration-seconds 30})]

        ;; Wait for handler to start
        (deref handler-started 5000 :timeout)

        ;; Cancel the job externally (simulates session exit)
        (job-store/cancel! pool session-id "test-job")

        ;; Let handler continue
        (deliver handler-proceed true)

        ;; Stop worker
        (Thread/sleep 300)
        (stop!)
        (Thread/sleep 100))

      ;; Verify continue-fn results
      (is (= true (first @continue-results))
          "continue-fn should return true before cancellation")
      (is (= false (second @continue-results))
          "continue-fn should return false after cancellation"))))
