(ns ^:integration com.fulcrologic.statecharts.persistence.pg.job-store-spec
  "Integration tests for PostgreSQL job store.

   These tests require a running PostgreSQL instance.

   To run these tests:
   1. Ensure PostgreSQL is running on localhost:5432
   2. Create a test database: createdb statecharts_test
   3. Run with: clj -M:test -m kaocha.runner --focus :integration

   Environment variables for custom configuration:
   - PG_TEST_HOST (default: localhost)
   - PG_TEST_PORT (default: 5432)
   - PG_TEST_DATABASE (default: statecharts_test)
   - PG_TEST_USER (default: postgres)
   - PG_TEST_PASSWORD (default: postgres)"
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.persistence.pg.job-store :as sut]
   [com.fulcrologic.statecharts.persistence.pg.schema :as schema]
   [pg.core :as pg]
   [pg.pool :as pool])
  (:import
   [java.time OffsetDateTime Duration]
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
;; Test Helpers
;; -----------------------------------------------------------------------------

(defn- make-job-params
  "Build a job params map with defaults. Overrides can be supplied."
  ([] (make-job-params {}))
  ([overrides]
   (merge {:id (UUID/randomUUID)
           :session-id :test-session
           :invokeid :invoke-1
           :job-type "http"
           :payload {:url "https://example.com"}
           :max-attempts 3}
          overrides)))

(defn- get-job-row
  "Fetch a raw job row by id for assertion purposes."
  [pool job-id]
  (first
    (pg/execute pool
      "SELECT * FROM statechart_jobs WHERE id = $1"
      {:params [job-id]
       :kebab-keys? true})))

(defn- set-job-status!
  "Directly set a job's status in the database (test helper)."
  [pool job-id status]
  (pg/execute pool
    "UPDATE statechart_jobs SET status = $1, updated_at = now() WHERE id = $2"
    {:params [status job-id]}))

(defn- set-lease!
  "Directly set lease fields on a job (test helper)."
  [pool job-id owner expires-at]
  (pg/execute pool
    (str "UPDATE statechart_jobs SET lease_owner = $1, lease_expires_at = $2,"
         " status = 'running', updated_at = now() WHERE id = $3")
    {:params [owner expires-at job-id]}))

(defn- set-next-run-at!
  "Directly set next_run_at on a job (test helper)."
  [pool job-id next-run-at]
  (pg/execute pool
    "UPDATE statechart_jobs SET next_run_at = $1, updated_at = now() WHERE id = $2"
    {:params [next-run-at job-id]}))

;; -----------------------------------------------------------------------------
;; 1. CRUD Operations
;; -----------------------------------------------------------------------------

(deftest ^:integration create-job-returns-new-uuid-test
  (testing "create-job! returns new UUID on fresh insert"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      (is (uuid? job-id) "returns a UUID")
      (is (= (:id params) job-id) "returns the id provided in params"))))

(deftest ^:integration create-job-persists-fields-test
  (testing "create-job! persists all fields correctly"
    (let [params (make-job-params {:payload {:key "value" :nested {:a 1}}
                                   :max-attempts 5})
          job-id (sut/create-job! *pool* params)
          row (get-job-row *pool* job-id)]
      (is (= "pending" (:status row)) "status defaults to pending")
      (is (= 0 (:attempt row)) "attempt defaults to 0")
      (is (= 5 (:max-attempts row)) "max-attempts is stored")
      (is (= "http" (:job-type row)) "job-type is stored")
      (is (= (core/session-id->str :test-session)
             (:session-id row))
          "session-id is stored as string")
      (is (= "invoke-1" (:invokeid row)) "invokeid is stored as name string")
      (is (= {:key "value" :nested {:a 1}}
             (core/thaw (:payload row)))
          "payload is frozen and stored"))))

(deftest ^:integration create-job-idempotency-i1-test
  (testing "create-job! returns existing UUID on conflict (I1 — idempotency)"
    (let [params (make-job-params)
          first-id (sut/create-job! *pool* params)
          ;; Second insert with same session-id + invokeid but different UUID
          second-id (sut/create-job! *pool* (assoc params :id (UUID/randomUUID)))]
      (is (= first-id second-id)
          "returns the existing job-id when an active job exists for same session+invokeid"))))

(deftest ^:integration create-job-allows-new-after-terminal-test
  (testing "create-job! allows new job after previous is terminal"
    (let [params (make-job-params)
          first-id (sut/create-job! *pool* params)]
      ;; Mark the first job as succeeded (terminal)
      (set-job-status! *pool* first-id "succeeded")

      (let [new-params (make-job-params {:id (UUID/randomUUID)})
            second-id (sut/create-job! *pool* new-params)]
        (is (uuid? second-id) "new job is created")
        (is (not= first-id second-id) "new job has different id")
        (is (= (:id new-params) second-id) "returns the new job's id")))

    (testing "also works when previous job is failed"
      (let [params (make-job-params {:session-id :session-failed
                                     :invokeid :invoke-fail})
            first-id (sut/create-job! *pool* params)]
        (set-job-status! *pool* first-id "failed")
        (let [second-id (sut/create-job! *pool* (assoc params :id (UUID/randomUUID)))]
          (is (not= first-id second-id)))))

    (testing "also works when previous job is cancelled"
      (let [params (make-job-params {:session-id :session-cancelled
                                     :invokeid :invoke-cancel})
            first-id (sut/create-job! *pool* params)]
        (set-job-status! *pool* first-id "cancelled")
        (let [second-id (sut/create-job! *pool* (assoc params :id (UUID/randomUUID)))]
          (is (not= first-id second-id)))))))

(deftest ^:integration create-job-default-max-attempts-test
  (testing "create-job! defaults max-attempts to 3 when not specified"
    (let [params (dissoc (make-job-params) :max-attempts)
          job-id (sut/create-job! *pool* params)
          row (get-job-row *pool* job-id)]
      (is (= 3 (:max-attempts row))))))

;; -----------------------------------------------------------------------------
;; 2. Claim Operations
;; -----------------------------------------------------------------------------

(deftest ^:integration claim-jobs-returns-pending-ordered-test
  (testing "claim-jobs! returns pending jobs ordered by next_run_at"
    (let [now (OffsetDateTime/now)
          ;; Create three jobs with staggered next_run_at
          id-1 (:id (make-job-params {:session-id :s1 :invokeid :i1}))
          id-2 (:id (make-job-params {:session-id :s2 :invokeid :i2}))
          id-3 (:id (make-job-params {:session-id :s3 :invokeid :i3}))]
      (sut/create-job! *pool* (make-job-params {:id id-1 :session-id :s1 :invokeid :i1}))
      (sut/create-job! *pool* (make-job-params {:id id-2 :session-id :s2 :invokeid :i2}))
      (sut/create-job! *pool* (make-job-params {:id id-3 :session-id :s3 :invokeid :i3}))
      ;; Set next_run_at so id-3 is earliest, id-1 latest
      (set-next-run-at! *pool* id-1 (.plus now (Duration/ofSeconds 10)))
      (set-next-run-at! *pool* id-2 (.minus now (Duration/ofSeconds 5)))
      (set-next-run-at! *pool* id-3 (.minus now (Duration/ofSeconds 10)))

      (let [claimed (sut/claim-jobs! *pool* {:owner-id "worker-1" :limit 10})]
        ;; Only id-2 and id-3 should be claimable (next_run_at <= now)
        ;; id-1 has next_run_at in the future
        (is (= 2 (count claimed)) "only past-due jobs are claimed")
        (is (= id-3 (:id (first claimed)))
            "job with earliest next_run_at comes first")
        (is (= id-2 (:id (second claimed)))
            "job with later next_run_at comes second")))))

(deftest ^:integration claim-jobs-sets-running-state-test
  (testing "claim-jobs! sets status=running, lease_owner, lease_expires_at, increments attempt"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)
          claimed (sut/claim-jobs! *pool* {:owner-id "worker-A"
                                           :lease-duration-seconds 120
                                           :limit 5})]
      (is (= 1 (count claimed)))
      (let [claimed-job (first claimed)
            row (get-job-row *pool* job-id)]
        (is (= "running" (:status row)) "status is set to running")
        (is (= "worker-A" (:lease-owner row)) "lease_owner is set")
        (is (some? (:lease-expires-at row)) "lease_expires_at is set")
        (is (= 1 (:attempt row)) "attempt is incremented from 0 to 1")))))

(deftest ^:integration claim-jobs-thaws-payload-test
  (testing "claim-jobs! returns jobs with thawed payload"
    (let [payload {:url "https://example.com" :headers {"Authorization" "Bearer xyz"}}
          params (make-job-params {:payload payload})
          _ (sut/create-job! *pool* params)
          claimed (sut/claim-jobs! *pool* {:owner-id "worker-1" :limit 5})]
      (is (= payload (:payload (first claimed)))
          "payload is deserialized back to Clojure data"))))

(deftest ^:integration claim-jobs-skips-running-with-valid-lease-test
  (testing "claim-jobs! skips already-running jobs with valid leases"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      ;; Set the job as running with a lease that expires in the future
      (set-lease! *pool* job-id "worker-A"
                  (.plus (OffsetDateTime/now) (Duration/ofMinutes 5)))

      (let [claimed (sut/claim-jobs! *pool* {:owner-id "worker-B" :limit 5})]
        (is (empty? claimed) "no jobs are claimed when all have valid leases")))))

(deftest ^:integration claim-jobs-reclaims-expired-lease-test
  (testing "claim-jobs! reclaims running jobs with expired leases (stale worker recovery)"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      ;; Set the job as running with an expired lease
      (set-lease! *pool* job-id "dead-worker"
                  (.minus (OffsetDateTime/now) (Duration/ofMinutes 1)))

      (let [claimed (sut/claim-jobs! *pool* {:owner-id "new-worker"
                                              :lease-duration-seconds 60
                                              :limit 5})]
        (is (= 1 (count claimed)) "expired-lease job is reclaimed")
        (let [row (get-job-row *pool* job-id)]
          (is (= "new-worker" (:lease-owner row))
              "lease_owner is updated to new worker")
          (is (= "running" (:status row)) "status remains running"))))))

(deftest ^:integration claim-jobs-respects-limit-test
  (testing "claim-jobs! respects the limit parameter"
    ;; Create 5 pending jobs
    (doseq [i (range 5)]
      (sut/create-job! *pool* (make-job-params {:id (UUID/randomUUID)
                                                :session-id (keyword (str "s" i))
                                                :invokeid (keyword (str "i" i))})))
    (let [claimed (sut/claim-jobs! *pool* {:owner-id "worker-1" :limit 2})]
      (is (= 2 (count claimed)) "only claims up to the limit"))))

;; -----------------------------------------------------------------------------
;; 3. Heartbeat
;; -----------------------------------------------------------------------------

(deftest ^:integration heartbeat-extends-lease-test
  (testing "heartbeat! extends lease for owned job, returns true"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      ;; Claim the job first
      (sut/claim-jobs! *pool* {:owner-id "worker-1"
                                :lease-duration-seconds 30
                                :limit 1})
      (let [row-before (get-job-row *pool* job-id)
            original-expires (:lease-expires-at row-before)
            ;; Small delay so new lease is later
            _ (Thread/sleep 50)
            result (sut/heartbeat! *pool* job-id "worker-1" 120)
            row-after (get-job-row *pool* job-id)]
        (is (true? result) "returns true when lease is extended")
        (is (.isAfter (:lease-expires-at row-after) original-expires)
            "lease_expires_at is extended beyond original")))))

(deftest ^:integration heartbeat-returns-false-wrong-owner-test
  (testing "heartbeat! returns false when lease_owner doesn't match (I8)"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      ;; Claim by worker-1
      (sut/claim-jobs! *pool* {:owner-id "worker-1"
                                :lease-duration-seconds 60
                                :limit 1})
      ;; worker-2 tries to heartbeat
      (let [result (sut/heartbeat! *pool* job-id "worker-2" 120)]
        (is (false? result) "returns false when owner doesn't match")))))

(deftest ^:integration heartbeat-returns-false-non-running-test
  (testing "heartbeat! returns false when job is no longer running"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      ;; Job is pending, not running
      (let [result (sut/heartbeat! *pool* job-id "worker-1" 60)]
        (is (false? result) "returns false for non-running job")))))

;; -----------------------------------------------------------------------------
;; 4. Terminal Operations
;; -----------------------------------------------------------------------------

(deftest ^:integration complete-sets-succeeded-test
  (testing "complete! sets status=succeeded and stores result"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      (sut/complete! *pool* job-id
                     {:entity-id 42 :name "Created"}
                     "done.invoke.http"
                     {:entity-id 42})
      (let [row (get-job-row *pool* job-id)]
        (is (= "succeeded" (:status row)) "status is succeeded")
        (is (= {:entity-id 42 :name "Created"}
               (core/thaw (:result row)))
            "result is stored and can be thawed")
        (is (= "done.invoke.http" (:terminal-event-name row))
            "terminal event name is stored")
        (is (= {:entity-id 42}
               (core/thaw (:terminal-event-data row)))
            "terminal event data is stored")))))

(deftest ^:integration fail-with-retries-remaining-test
  (testing "fail! with attempts remaining: sets status=pending, schedules retry with backoff"
    (let [params (make-job-params {:max-attempts 3})
          job-id (sut/create-job! *pool* params)
          now-before (OffsetDateTime/now)]
      ;; Fail on attempt 1 (of max 3)
      (sut/fail! *pool* job-id 1 3
                 {:message "Connection timeout"}
                 "error.invoke.http"
                 {:error "timeout"})
      (let [row (get-job-row *pool* job-id)]
        (is (= "pending" (:status row)) "status is set back to pending")
        (is (.isAfter (:next-run-at row) now-before)
            "next_run_at is set in the future for backoff")
        (is (nil? (:lease-owner row)) "lease_owner is cleared")
        (is (nil? (:lease-expires-at row)) "lease_expires_at is cleared")
        (is (= {:message "Connection timeout"}
               (core/thaw (:error row)))
            "error is stored")))))

(deftest ^:integration fail-with-attempts-exhausted-test
  (testing "fail! with attempts exhausted: sets status=failed, stores error and terminal event"
    (let [params (make-job-params {:max-attempts 3})
          job-id (sut/create-job! *pool* params)]
      ;; Fail on attempt 3 (of max 3) -- exhausted
      (sut/fail! *pool* job-id 3 3
                 {:message "Permanent failure"}
                 "error.invoke.http"
                 {:error "permanent"})
      (let [row (get-job-row *pool* job-id)]
        (is (= "failed" (:status row)) "status is failed")
        (is (= {:message "Permanent failure"}
               (core/thaw (:error row)))
            "error is stored")
        (is (= "error.invoke.http" (:terminal-event-name row))
            "terminal event name is stored")
        (is (= {:error "permanent"}
               (core/thaw (:terminal-event-data row)))
            "terminal event data is stored")))))

(deftest ^:integration fail-retry-backoff-increases-test
  (testing "fail! backoff increases with attempt number"
    ;; Attempt 1: backoff = 2^1 = 2s
    ;; Attempt 2: backoff = 2^2 = 4s
    ;; We verify the second retry is scheduled further in the future.
    (let [params-1 (make-job-params {:session-id :s-backoff-1 :invokeid :i-backoff-1})
          params-2 (make-job-params {:session-id :s-backoff-2 :invokeid :i-backoff-2})
          id-1 (sut/create-job! *pool* params-1)
          id-2 (sut/create-job! *pool* params-2)]
      (sut/fail! *pool* id-1 1 5 {:msg "err"} nil nil)
      (sut/fail! *pool* id-2 2 5 {:msg "err"} nil nil)
      (let [row-1 (get-job-row *pool* id-1)
            row-2 (get-job-row *pool* id-2)]
        (is (.isBefore (:next-run-at row-1) (:next-run-at row-2))
            "higher attempt number has later next_run_at")))))

;; -----------------------------------------------------------------------------
;; 5. Cancellation (I7)
;; -----------------------------------------------------------------------------

(deftest ^:integration cancel-pending-to-cancelled-test
  (testing "cancel! transitions pending -> cancelled"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)
          rows-affected (sut/cancel! *pool* :test-session :invoke-1)]
      (is (= 1 rows-affected) "one row affected")
      (let [row (get-job-row *pool* job-id)]
        (is (= "cancelled" (:status row)) "status is cancelled")))))

(deftest ^:integration cancel-running-to-cancelled-test
  (testing "cancel! transitions running -> cancelled"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      ;; Set to running
      (set-lease! *pool* job-id "worker-1"
                  (.plus (OffsetDateTime/now) (Duration/ofMinutes 5)))
      (let [rows-affected (sut/cancel! *pool* :test-session :invoke-1)]
        (is (= 1 rows-affected) "one row affected")
        (let [row (get-job-row *pool* job-id)]
          (is (= "cancelled" (:status row)) "status is cancelled"))))))

(deftest ^:integration cancel-noop-on-succeeded-test
  (testing "cancel! is no-op on succeeded jobs"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      (set-job-status! *pool* job-id "succeeded")
      (let [rows-affected (sut/cancel! *pool* :test-session :invoke-1)]
        (is (= 0 rows-affected) "zero rows affected")
        (let [row (get-job-row *pool* job-id)]
          (is (= "succeeded" (:status row)) "status remains succeeded"))))))

(deftest ^:integration cancel-noop-on-failed-test
  (testing "cancel! is no-op on failed jobs"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      (set-job-status! *pool* job-id "failed")
      (let [rows-affected (sut/cancel! *pool* :test-session :invoke-1)]
        (is (= 0 rows-affected) "zero rows affected")
        (let [row (get-job-row *pool* job-id)]
          (is (= "failed" (:status row)) "status remains failed"))))))

(deftest ^:integration cancel-by-session-cancels-all-active-test
  (testing "cancel-by-session! cancels all active jobs for a session"
    (let [session-id :session-multi
          id-1 (sut/create-job! *pool* (make-job-params {:id (UUID/randomUUID)
                                                         :session-id session-id
                                                         :invokeid :invoke-a}))
          id-2 (sut/create-job! *pool* (make-job-params {:id (UUID/randomUUID)
                                                         :session-id session-id
                                                         :invokeid :invoke-b}))
          id-3 (sut/create-job! *pool* (make-job-params {:id (UUID/randomUUID)
                                                         :session-id session-id
                                                         :invokeid :invoke-c}))]
      ;; Set one job to running
      (set-lease! *pool* id-2 "worker-1"
                  (.plus (OffsetDateTime/now) (Duration/ofMinutes 5)))

      (let [rows-affected (sut/cancel-by-session! *pool* session-id)]
        (is (= 3 rows-affected) "all three active jobs are cancelled")
        (is (= "cancelled" (:status (get-job-row *pool* id-1))))
        (is (= "cancelled" (:status (get-job-row *pool* id-2))))
        (is (= "cancelled" (:status (get-job-row *pool* id-3))))))))

(deftest ^:integration cancel-by-session-leaves-terminal-untouched-test
  (testing "cancel-by-session! leaves terminal jobs untouched"
    (let [session-id :session-mixed
          id-pending (sut/create-job! *pool* (make-job-params {:id (UUID/randomUUID)
                                                               :session-id session-id
                                                               :invokeid :invoke-pending}))
          id-succeeded (sut/create-job! *pool* (make-job-params {:id (UUID/randomUUID)
                                                                  :session-id session-id
                                                                  :invokeid :invoke-succeeded}))
          id-failed (sut/create-job! *pool* (make-job-params {:id (UUID/randomUUID)
                                                               :session-id session-id
                                                               :invokeid :invoke-failed}))]
      ;; Set terminal statuses
      (set-job-status! *pool* id-succeeded "succeeded")
      (set-job-status! *pool* id-failed "failed")

      (let [rows-affected (sut/cancel-by-session! *pool* session-id)]
        (is (= 1 rows-affected) "only the pending job is cancelled")
        (is (= "cancelled" (:status (get-job-row *pool* id-pending))))
        (is (= "succeeded" (:status (get-job-row *pool* id-succeeded)))
            "succeeded job is untouched")
        (is (= "failed" (:status (get-job-row *pool* id-failed)))
            "failed job is untouched")))))

;; -----------------------------------------------------------------------------
;; 6. Queries
;; -----------------------------------------------------------------------------

(deftest ^:integration get-active-job-returns-active-test
  (testing "get-active-job returns the active (pending) job"
    (let [payload {:url "https://example.com"}
          params (make-job-params {:payload payload})
          job-id (sut/create-job! *pool* params)
          active (sut/get-active-job *pool* :test-session :invoke-1)]
      (is (some? active) "returns non-nil for active job")
      (is (= job-id (:id active)) "returns the correct job")
      (is (= payload (:payload active)) "payload is thawed")))

  (testing "get-active-job returns the active (running) job"
    (let [params (make-job-params {:session-id :s-running :invokeid :i-running})
          job-id (sut/create-job! *pool* params)]
      (set-lease! *pool* job-id "worker-1"
                  (.plus (OffsetDateTime/now) (Duration/ofMinutes 5)))
      (let [active (sut/get-active-job *pool* :s-running :i-running)]
        (is (some? active) "returns non-nil for running job")
        (is (= job-id (:id active)))))))

(deftest ^:integration get-active-job-returns-nil-test
  (testing "get-active-job returns nil when no active job exists"
    (is (nil? (sut/get-active-job *pool* :nonexistent :no-invoke))
        "returns nil for non-existent session+invokeid"))

  (testing "get-active-job returns nil for terminal jobs"
    (let [params (make-job-params {:session-id :s-terminal :invokeid :i-terminal})
          job-id (sut/create-job! *pool* params)]
      (set-job-status! *pool* job-id "succeeded")
      (is (nil? (sut/get-active-job *pool* :s-terminal :i-terminal))
          "returns nil when job is succeeded"))))

(deftest ^:integration job-cancelled-returns-true-test
  (testing "job-cancelled? returns true for cancelled job"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      (set-job-status! *pool* job-id "cancelled")
      (is (true? (sut/job-cancelled? *pool* job-id))))))

(deftest ^:integration job-cancelled-returns-false-test
  (testing "job-cancelled? returns false for running job"
    (let [params (make-job-params {:session-id :s-running-check :invokeid :i-running-check})
          job-id (sut/create-job! *pool* params)]
      (set-lease! *pool* job-id "worker-1"
                  (.plus (OffsetDateTime/now) (Duration/ofMinutes 5)))
      (is (false? (sut/job-cancelled? *pool* job-id)))))

  (testing "job-cancelled? returns false for pending job"
    (let [params (make-job-params {:session-id :s-pending-check :invokeid :i-pending-check})
          job-id (sut/create-job! *pool* params)]
      (is (false? (sut/job-cancelled? *pool* job-id))))))

;; -----------------------------------------------------------------------------
;; 7. Reconciliation Queries
;; -----------------------------------------------------------------------------

(deftest ^:integration get-undispatched-terminal-jobs-test
  (testing "get-undispatched-terminal-jobs returns succeeded jobs with terminal event"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      (sut/complete! *pool* job-id
                     {:result-data true}
                     "done.invoke.http"
                     {:entity-id 42})
      (let [undispatched (sut/get-undispatched-terminal-jobs *pool* 10)]
        (is (= 1 (count undispatched)))
        (is (= job-id (:id (first undispatched))))
        (is (= "done.invoke.http" (:terminal-event-name (first undispatched))))
        (is (= {:entity-id 42} (:terminal-event-data (first undispatched)))
            "terminal-event-data is thawed"))))

  (testing "get-undispatched-terminal-jobs excludes already-dispatched"
    (let [params (make-job-params {:session-id :s-dispatched :invokeid :i-dispatched})
          job-id (sut/create-job! *pool* params)]
      (sut/complete! *pool* job-id {:ok true} "done.invoke" {})
      (sut/mark-terminal-event-dispatched! *pool* job-id)
      (let [undispatched (sut/get-undispatched-terminal-jobs *pool* 10)]
        ;; Only the job from the first test block should be here (if this runs in same test),
        ;; but the dispatched one should not appear
        (is (not (some #(= job-id (:id %)) undispatched))
            "dispatched job is excluded")))))

(deftest ^:integration mark-terminal-event-dispatched-test
  (testing "mark-terminal-event-dispatched! sets the timestamp"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      (sut/complete! *pool* job-id {:ok true} "done.invoke" {})
      (is (nil? (:terminal-event-dispatched-at (get-job-row *pool* job-id)))
          "not dispatched initially")
      (sut/mark-terminal-event-dispatched! *pool* job-id)
      (is (some? (:terminal-event-dispatched-at (get-job-row *pool* job-id)))
          "timestamp is set after marking dispatched"))))

;; -----------------------------------------------------------------------------
;; 8. Partial Result (I9)
;; -----------------------------------------------------------------------------

(deftest ^:integration store-partial-result-test
  (testing "store-partial-result! stores intermediate result"
    (let [params (make-job-params)
          job-id (sut/create-job! *pool* params)]
      (sut/store-partial-result! *pool* job-id {:entity-id 99 :step "created"})
      (let [row (get-job-row *pool* job-id)]
        (is (= {:entity-id 99 :step "created"}
               (core/thaw (:result row)))
            "partial result is stored and retrievable")
        (is (= "pending" (:status row))
            "status remains pending (not completed)")))))
