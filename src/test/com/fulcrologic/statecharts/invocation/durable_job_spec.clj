(ns ^:integration com.fulcrologic.statecharts.invocation.durable-job-spec
  "Integration tests for DurableJobInvocationProcessor.

   Requires a running PostgreSQL instance. See integration_test.clj for
   environment variable configuration."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.data-model.operations :as ops]
   [com.fulcrologic.statecharts.data-model.working-memory-data-model :as wmdm]
   [com.fulcrologic.statecharts.invocation.durable-job :as sut]
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.persistence.pg.job-store :as job-store]
   [com.fulcrologic.statecharts.persistence.pg.schema :as schema]
   [com.fulcrologic.statecharts.protocols :as sp]
   [pg.core :as pg]
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
  (let [pool (pool/pool test-config)]
    (try
      (binding [*pool* pool]
        (f))
      (finally
        (pool/close pool)))))

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

(defn- make-env
  "Build a processing env with a FlatWorkingMemoryDataModel and a volatile
   holding working memory for the given session-id."
  [session-id]
  (let [data-model (wmdm/new-flat-model)
        wmem       {::sc/session-id session-id
                    ::sc/data-model {}}
        vwmem      (volatile! wmem)]
    {::sc/data-model data-model
     ::sc/vwmem      vwmem}))

(defn- data-model-contents
  "Read the current flat data model contents from the env's volatile working memory."
  [env]
  (some-> (::sc/vwmem env) deref ::sc/data-model))

(defn- get-job-status
  "Fetch the raw status string for a job by id."
  [pool job-id]
  (let [rows (pg/with-connection [c pool]
               (pg/execute c
                 "SELECT status FROM statechart_jobs WHERE id = $1"
                 {:params [job-id]
                  :kebab-keys? true}))]
    (:status (first rows))))

(defn- set-job-status!
  "Directly update a job's status in the database (test helper)."
  [pool job-id status]
  (pg/with-connection [c pool]
    (pg/execute c
      "UPDATE statechart_jobs SET status = $1, updated_at = now() WHERE id = $2"
      {:params [status job-id]})))

;; -----------------------------------------------------------------------------
;; Tests: supports-invocation-type?
;; -----------------------------------------------------------------------------

(deftest ^:integration supports-invocation-type?-test
  (let [processor (sut/->DurableJobInvocationProcessor *pool*)]

    (testing "returns true for :durable-job"
      (is (true? (sp/supports-invocation-type? processor :durable-job))))

    (testing "returns false for :future"
      (is (false? (sp/supports-invocation-type? processor :future))))

    (testing "returns false for :statechart"
      (is (false? (sp/supports-invocation-type? processor :statechart))))))

;; -----------------------------------------------------------------------------
;; Tests: start-invocation!
;; -----------------------------------------------------------------------------

(deftest ^:integration start-invocation-creates-job-test
  (let [processor  (sut/->DurableJobInvocationProcessor *pool*)
        session-id :test-session-1
        env        (make-env session-id)
        invokeid   :invoke.gen-quiz]

    (testing "creates job in DB and stores job-id in data model via ops/assign"
      (sp/start-invocation! processor env
        {:invokeid invokeid
         :src      :quiz-content-generation
         :params   {:quiz-id 42}})

      (let [dm       (data-model-contents env)
            job-id-key  (keyword "invoke.gen-quiz" "job-id")
            job-kind-key (keyword "invoke.gen-quiz" "job-kind")
            stored-job-id (get dm job-id-key)
            stored-kind   (get dm job-kind-key)]

        (testing "job-id is stored as a string UUID in the data model"
          (is (some? stored-job-id))
          (is (string? stored-job-id))
          (is (some? (parse-uuid stored-job-id))))

        (testing "job-kind is stored as the string form of the src keyword"
          (is (= "quiz-content-generation" stored-kind)))

        (testing "job exists in the database with correct attributes"
          (let [job (job-store/get-active-job *pool* session-id invokeid)]
            (is (some? job))
            (is (= (parse-uuid stored-job-id) (:id job)))
            (is (= "quiz-content-generation" (:job-type job)))
            (is (= {:quiz-id 42} (:payload job)))
            (is (= "pending" (:status job)))))))))

(deftest ^:integration start-invocation-uses-session-id-test
  (let [processor  (sut/->DurableJobInvocationProcessor *pool*)
        session-id (random-uuid)
        env        (make-env session-id)]

    (testing "uses env/session-id to associate job with the correct session"
      (sp/start-invocation! processor env
        {:invokeid :invoke.task
         :src      :some-task
         :params   {}})

      (let [job (job-store/get-active-job *pool* session-id :invoke.task)]
        (is (some? job))
        (is (= (core/session-id->str session-id)
               (core/session-id->str session-id)))))))

(deftest ^:integration start-invocation-idempotent-test
  (let [processor  (sut/->DurableJobInvocationProcessor *pool*)
        session-id :test-session-idem
        env1       (make-env session-id)
        env2       (make-env session-id)
        invokeid   :invoke.idem]

    (testing "I1: second call with same session+invokeid returns the same job-id"
      (sp/start-invocation! processor env1
        {:invokeid invokeid
         :src      :my-job
         :params   {:attempt 1}})

      (sp/start-invocation! processor env2
        {:invokeid invokeid
         :src      :my-job
         :params   {:attempt 2}})

      (let [dm1     (data-model-contents env1)
            dm2     (data-model-contents env2)
            id-key  (keyword "invoke.idem" "job-id")
            job-id1 (get dm1 id-key)
            job-id2 (get dm2 id-key)]
        (is (some? job-id1))
        (is (= job-id1 job-id2) "Both invocations should reference the same job-id")))))

;; -----------------------------------------------------------------------------
;; Tests: stop-invocation!
;; -----------------------------------------------------------------------------

(deftest ^:integration stop-invocation-cancels-pending-job-test
  (let [processor  (sut/->DurableJobInvocationProcessor *pool*)
        session-id :test-session-stop-pending
        env        (make-env session-id)
        invokeid   :invoke.cancel-me]

    (sp/start-invocation! processor env
      {:invokeid invokeid
       :src      :cancellable-job
       :params   {}})

    (let [dm     (data-model-contents env)
          id-key (keyword "invoke.cancel-me" "job-id")
          job-id (parse-uuid (get dm id-key))]

      (testing "job starts as pending"
        (is (= "pending" (get-job-status *pool* job-id))))

      (testing "stop-invocation! cancels the pending job"
        (sp/stop-invocation! processor env {:invokeid invokeid})
        (is (= "cancelled" (get-job-status *pool* job-id)))))))

(deftest ^:integration stop-invocation-cancels-running-job-test
  (let [processor  (sut/->DurableJobInvocationProcessor *pool*)
        session-id :test-session-stop-running
        env        (make-env session-id)
        invokeid   :invoke.running]

    (sp/start-invocation! processor env
      {:invokeid invokeid
       :src      :long-running-job
       :params   {}})

    (let [dm     (data-model-contents env)
          id-key (keyword "invoke.running" "job-id")
          job-id (parse-uuid (get dm id-key))]

      ;; Simulate the worker claiming the job (status -> running)
      (set-job-status! *pool* job-id "running")

      (testing "job is now running"
        (is (= "running" (get-job-status *pool* job-id))))

      (testing "stop-invocation! cancels the running job"
        (sp/stop-invocation! processor env {:invokeid invokeid})
        (is (= "cancelled" (get-job-status *pool* job-id)))))))

(deftest ^:integration stop-invocation-noop-on-succeeded-job-test
  (let [processor  (sut/->DurableJobInvocationProcessor *pool*)
        session-id :test-session-stop-succeeded
        env        (make-env session-id)
        invokeid   :invoke.done]

    (sp/start-invocation! processor env
      {:invokeid invokeid
       :src      :completed-job
       :params   {}})

    (let [dm     (data-model-contents env)
          id-key (keyword "invoke.done" "job-id")
          job-id (parse-uuid (get dm id-key))]

      ;; Simulate job completing successfully
      (set-job-status! *pool* job-id "succeeded")

      (testing "I7: stop-invocation! is a no-op on succeeded job"
        (sp/stop-invocation! processor env {:invokeid invokeid})
        (is (= "succeeded" (get-job-status *pool* job-id))
            "Status should remain succeeded, not change to cancelled")))))

(deftest ^:integration stop-invocation-noop-on-failed-job-test
  (let [processor  (sut/->DurableJobInvocationProcessor *pool*)
        session-id :test-session-stop-failed
        env        (make-env session-id)
        invokeid   :invoke.failed]

    (sp/start-invocation! processor env
      {:invokeid invokeid
       :src      :failing-job
       :params   {}})

    (let [dm     (data-model-contents env)
          id-key (keyword "invoke.failed" "job-id")
          job-id (parse-uuid (get dm id-key))]

      ;; Simulate job failing permanently
      (set-job-status! *pool* job-id "failed")

      (testing "I7: stop-invocation! is a no-op on failed job"
        (sp/stop-invocation! processor env {:invokeid invokeid})
        (is (= "failed" (get-job-status *pool* job-id))
            "Status should remain failed, not change to cancelled")))))

;; -----------------------------------------------------------------------------
;; Tests: forward-event!
;; -----------------------------------------------------------------------------

(deftest ^:integration forward-event-returns-nil-test
  (let [processor (sut/->DurableJobInvocationProcessor *pool*)]

    (testing "forward-event! returns nil (durable jobs do not support event forwarding)"
      (is (nil? (sp/forward-event! processor {} {:type :durable-job
                                                  :invokeid :invoke.whatever
                                                  :event {:name :some-event}}))))))
