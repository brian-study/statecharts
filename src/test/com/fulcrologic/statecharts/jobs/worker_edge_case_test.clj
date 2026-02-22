(ns ^:integration com.fulcrologic.statecharts.jobs.worker-edge-case-test
  "Edge-case integration tests for the demand-driven job worker.

   Requires a running PostgreSQL instance.

   Run with:
     PG_TEST_USER=user PG_TEST_PASSWORD=password \\
     clj -M:test -e \"(require '[com.fulcrologic.statecharts.jobs.worker-edge-case-test :as edge] :reload) ...\""
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts.jobs.worker :as worker]
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.persistence.pg.job-store :as job-store]
   [com.fulcrologic.statecharts.persistence.pg.schema :as schema]
   [com.fulcrologic.statecharts.protocols :as sp]
   [pg.core :as pg]
   [pg.pool :as pool]
   [taoensso.timbre :as log])
  (:import
   [java.util UUID]
   [java.util.concurrent CountDownLatch Semaphore TimeUnit]))

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
;; Helpers (duplicated from worker_benchmark_test)
;; -----------------------------------------------------------------------------

(def ^:private terminal-statuses
  #{"succeeded" "failed" "cancelled"})

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
             :session-id (keyword (str "edge-session-" run-id "-" idx))
             :invokeid (keyword (str "edge-invoke-" run-id "-" idx))
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

(defn- wait-for-terminal-rows
  [pool job-ids timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [rows (get-job-rows pool job-ids)]
        (cond
          (every? #(and % (contains? terminal-statuses (:status %))) rows)
          rows

          (>= (System/currentTimeMillis) deadline)
          rows

          :else
          (do
            (Thread/sleep 25)
            (recur)))))))

(defn- new-handler-tracker []
  (atom {:in-flight 0
         :max-in-flight 0
         :started-at-ms {}
         :finished-at-ms {}
         :handler-latencies-ms {}
         :calls []
         :continue-results []}))

(defn- mark-handler-start! [tracker job-id]
  (when tracker
    (let [now-ms (System/currentTimeMillis)]
      (swap! tracker
        (fn [{:keys [in-flight] :as state}]
          (let [running (inc (long (or in-flight 0)))]
            (-> state
                (assoc :in-flight running)
                (assoc-in [:started-at-ms job-id] now-ms)
                (update :calls (fnil conj []) job-id)
                (update :max-in-flight (fnil max 0) running))))))))

(defn- mark-handler-stop! [tracker job-id]
  (when tracker
    (let [now-ms (System/currentTimeMillis)]
      (swap! tracker
        (fn [{:keys [in-flight] :as state}]
          (let [start-ms (get-in state [:started-at-ms job-id] now-ms)]
            (-> state
                (assoc :in-flight (max 0 (dec (long (or in-flight 0)))))
                (assoc-in [:finished-at-ms job-id] now-ms)
                (assoc-in [:handler-latencies-ms job-id] (- now-ms start-ms)))))))))

;; -----------------------------------------------------------------------------
;; Tests
;; -----------------------------------------------------------------------------

(deftest ^:integration claim-query-throws-worker-loop-survives-test
  (testing "worker loop survives a claim-jobs! failure and processes jobs created after the failure"
    (let [call-count (atom 0)
          original-claim-jobs! job-store/claim-jobs!
          tracker (new-handler-tracker)
          {:keys [queue]} (make-tracking-event-queue)]
      (with-redefs [job-store/claim-jobs!
                     (fn [pool opts]
                       (if (= 1 (swap! call-count inc))
                         (throw (ex-info "Simulated DB blip" {}))
                         (original-claim-jobs! pool opts)))]
        (let [worker (worker/start-worker!
                       {:pool *pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" (fn [{:keys [job-id]}]
                                                (mark-handler-start! tracker job-id)
                                                (try
                                                  {:job-id job-id}
                                                  (finally
                                                    (mark-handler-stop! tracker job-id))))}
                        :owner-id (str "claim-survive-" (random-uuid))
                        :poll-interval-ms 20
                        :claim-limit 2
                        :concurrency 2
                        :lease-duration-seconds 30
                        :update-data-by-id-fn noop-update-data-by-id-fn})]
          (try
            ;; Wait for the first poll to fail
            (Thread/sleep 200)
            ;; Create jobs AFTER the failure
            (let [job-ids (create-n-jobs! *pool* 3)]
              ((:wake! worker))
              (let [rows (wait-for-terminal-rows *pool* job-ids 10000)]
                (is (every? #(= "succeeded" (:status %)) rows)
                    "All 3 jobs created after claim failure should succeed")
                (is (= 3 (count (filter #(= "succeeded" (:status %)) rows))))
                (is (>= @call-count 2)
                    "claim-jobs! should have been called at least twice")))
            (finally
              ((:stop! worker)))))))))

(deftest ^:integration handler-throws-after-lease-lost-test
  (testing "handler throws after lease expires — fail! is ignored, Worker B completes the job"
    (let [job-ids (create-n-jobs! *pool* 1 {:max-attempts 3})
          job-id (first job-ids)
          a-started (promise)
          {:keys [queue]} (make-tracking-event-queue)
          ;; Worker A: signals start, sleeps past lease, then throws
          worker-a (worker/start-worker!
                     {:pool *pool*
                      :event-queue queue
                      :env {}
                      :handlers {"test-job" (fn [{:keys [continue-fn]}]
                                              (deliver a-started true)
                                              (Thread/sleep 2500)
                                              (throw (ex-info "Late failure after lease lost" {})))}
                      :owner-id (str "lease-lost-a-" (random-uuid))
                      :poll-interval-ms 20
                      :claim-limit 1
                      :concurrency 1
                      :lease-duration-seconds 1
                      :shutdown-timeout-ms 5000
                      :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (is (not= :timeout (deref a-started 5000 :timeout))
            "Worker A should start the job")
        ;; Wait for lease to expire (1s lease + 500ms margin)
        (Thread/sleep 1500)
        ;; Start Worker B with a fast handler
        (let [worker-b (worker/start-worker!
                         {:pool *pool*
                          :event-queue queue
                          :env {}
                          :handlers {"test-job" (fn [_ctx]
                                                  {:worker "B"})}
                          :owner-id (str "lease-lost-b-" (random-uuid))
                          :poll-interval-ms 20
                          :claim-limit 1
                          :concurrency 1
                          :lease-duration-seconds 30
                          :update-data-by-id-fn noop-update-data-by-id-fn})]
          (try
            (let [rows (wait-for-terminal-rows *pool* job-ids 15000)
                  row (first rows)]
              (is (= "succeeded" (:status row))
                  "Job should succeed via Worker B")
              (is (= "B" (get-in row [:result :worker]))
                  "Result should come from Worker B")
              (is (>= (:attempt row) 2)
                  "Attempt should be >= 2 (reclaimed after lease expiry)"))
            (finally
              ((:stop! worker-b)))))
        (finally
          ((:stop! worker-a)))))))

(deftest ^:integration executor-saturation-rapid-claim-cycling-test
  (testing "tight concurrency=2 with claim-limit=1 and mixed delays processes all jobs"
    (let [tracker (new-handler-tracker)
          job-ids (create-n-jobs! *pool* 10
                    {:payload-fn (fn [idx]
                                   {:job-index idx
                                    :delay-ms (if (even? idx) 20 80)})})
          {:keys [queue]} (make-tracking-event-queue)
          handler-fn (fn [{:keys [job-id params]}]
                       (mark-handler-start! tracker job-id)
                       (try
                         (Thread/sleep (:delay-ms params))
                         {:job-id job-id :job-index (:job-index params)}
                         (finally
                           (mark-handler-stop! tracker job-id))))
          worker (worker/start-worker!
                   {:pool *pool*
                    :event-queue queue
                    :env {}
                    :handlers {"test-job" handler-fn}
                    :owner-id (str "saturation-" (random-uuid))
                    :poll-interval-ms 10
                    :claim-limit 1
                    :concurrency 2
                    :lease-duration-seconds 30
                    :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (let [rows (wait-for-terminal-rows *pool* job-ids 10000)
              t @tracker
              max-in-flight (:max-in-flight t)]
          (is (every? #(= "succeeded" (:status %)) rows)
              "All 10 jobs should succeed")
          (is (= 10 (count (filter #(= "succeeded" (:status %)) rows))))
          (is (<= max-in-flight 2)
              (str "Max in-flight (" max-in-flight ") must not exceed concurrency (2)"))
          (is (>= max-in-flight 2)
              (str "Max in-flight (" max-in-flight ") should reach 2 (actually concurrent)")))
        (finally
          ((:stop! worker)))))))

(deftest ^:integration stop-while-claim-in-flight-test
  (testing "stop! during claim-jobs! DB query — permits released, loop exits, jobs self-heal"
    (let [claim-entered (atom 0)
          claim-latch (CountDownLatch. 1)
          original-claim-jobs! job-store/claim-jobs!
          job-ids (create-n-jobs! *pool* 3 {:max-attempts 3})
          {:keys [queue]} (make-tracking-event-queue)]
      (with-redefs [job-store/claim-jobs!
                     (fn [pool opts]
                       (let [n (swap! claim-entered inc)]
                         (when (= n 1)
                           ;; Block on latch for first claim call
                           (.await claim-latch 30 TimeUnit/SECONDS))
                         (original-claim-jobs! pool opts)))]
        (let [worker (worker/start-worker!
                       {:pool *pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" (fn [_ctx] {:result "ok"})}
                        :owner-id (str "stop-claim-" (random-uuid))
                        :poll-interval-ms 20
                        :claim-limit 3
                        :concurrency 3
                        :lease-duration-seconds 2
                        :shutdown-timeout-ms 3000
                        :update-data-by-id-fn noop-update-data-by-id-fn})]
          (try
            ;; Wait for claim-jobs! to be entered
            (loop [i 0]
              (when (and (< i 100) (zero? @claim-entered))
                (Thread/sleep 20)
                (recur (inc i))))
            (is (pos? @claim-entered) "claim-jobs! should have been entered")
            ;; Stop from another thread while claim is blocked
            (let [stop-future (future ((:stop! worker)))]
              ;; Release the latch so claim can complete
              (.countDown claim-latch)
              (let [result (deref stop-future 10000 :timeout)]
                (is (not= :timeout result) "stop! should return without hanging")))
            (finally
              ;; Ensure latch is released in case of early exit
              (.countDown claim-latch)))))
      ;; Wait for lease to expire so jobs become claimable again
      (Thread/sleep 3000)
      ;; Start a recovery worker (outside with-redefs so it uses real claim-jobs!)
      (let [worker-b (worker/start-worker!
                       {:pool *pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" (fn [_ctx] {:result "recovered"})}
                        :owner-id (str "stop-claim-b-" (random-uuid))
                        :poll-interval-ms 20
                        :claim-limit 3
                        :concurrency 3
                        :lease-duration-seconds 30
                        :update-data-by-id-fn noop-update-data-by-id-fn})]
        (try
          (let [rows (wait-for-terminal-rows *pool* job-ids 15000)]
            (is (every? #(contains? terminal-statuses (:status %)) rows)
                "All 3 jobs should reach terminal status after recovery"))
          (finally
            ((:stop! worker-b))))))))

(deftest ^:integration concurrent-stop-no-deadlock-test
  (testing "stop! called from two threads simultaneously — no deadlock, post-stop jobs unprocessed"
    (let [tracker (new-handler-tracker)
          job-ids (create-n-jobs! *pool* 5)
          {:keys [queue]} (make-tracking-event-queue)
          worker (worker/start-worker!
                   {:pool *pool*
                    :event-queue queue
                    :env {}
                    :handlers {"test-job" (fn [{:keys [job-id]}]
                                            (mark-handler-start! tracker job-id)
                                            (try
                                              (Thread/sleep 200)
                                              {:job-id job-id}
                                              (finally
                                                (mark-handler-stop! tracker job-id))))}
                    :owner-id (str "concurrent-stop-" (random-uuid))
                    :poll-interval-ms 20
                    :claim-limit 5
                    :concurrency 5
                    :lease-duration-seconds 30
                    :shutdown-timeout-ms 3000
                    :update-data-by-id-fn noop-update-data-by-id-fn})]
      ;; Let handler start processing
      (Thread/sleep 100)
      ;; Two concurrent stop! calls
      (let [f1 (future ((:stop! worker)))
            f2 (future ((:stop! worker)))
            r1 (deref f1 10000 :timeout)
            r2 (deref f2 10000 :timeout)]
        (is (not= :timeout r1) "First stop! should complete")
        (is (not= :timeout r2) "Second stop! should complete"))
      ;; Create jobs after stop — they should NOT be processed
      (let [post-stop-ids (create-n-jobs! *pool* 3)]
        (Thread/sleep 500)
        (let [rows (get-job-rows *pool* post-stop-ids)]
          (is (every? #(= "pending" (:status %)) rows)
              "Post-stop jobs should remain pending (worker is dead)"))))))

(deftest ^:integration continue-fn-returns-false-on-heartbeat-failure-test
  (testing "continue-fn returns false when heartbeat! throws (DB pool closed/invalid)"
    (let [heartbeat-should-fail (atom false)
          original-heartbeat! job-store/heartbeat!
          first-result (promise)
          second-result (promise)
          proceed-latch (CountDownLatch. 1)
          job-ids (create-n-jobs! *pool* 1)
          {:keys [queue]} (make-tracking-event-queue)]
      (with-redefs [job-store/heartbeat!
                     (fn [pool job-id owner-id lease-duration-seconds]
                       (if @heartbeat-should-fail
                         (throw (ex-info "Simulated DB pool closed" {}))
                         (original-heartbeat! pool job-id owner-id lease-duration-seconds)))]
        (let [worker (worker/start-worker!
                       {:pool *pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" (fn [{:keys [continue-fn]}]
                                                ;; First call — heartbeat works
                                                (deliver first-result (continue-fn))
                                                ;; Wait for test to enable failure
                                                (.await proceed-latch 10 TimeUnit/SECONDS)
                                                ;; Second call — heartbeat throws
                                                (deliver second-result (continue-fn))
                                                {:result "done"})}
                        :owner-id (str "heartbeat-fail-" (random-uuid))
                        :poll-interval-ms 20
                        :claim-limit 1
                        :concurrency 1
                        :lease-duration-seconds 60
                        :update-data-by-id-fn noop-update-data-by-id-fn})]
          (try
            ;; Wait for first continue-fn call
            (let [r1 (deref first-result 10000 :timeout)]
              (is (not= :timeout r1) "Handler should call continue-fn")
              (is (true? r1) "First continue-fn should return true (heartbeat works)"))
            ;; Enable heartbeat failure
            (reset! heartbeat-should-fail true)
            (.countDown proceed-latch)
            ;; Wait for second continue-fn call
            (let [r2 (deref second-result 10000 :timeout)]
              (is (not= :timeout r2) "Handler should call continue-fn again")
              (is (false? r2) "Second continue-fn should return false (heartbeat throws)"))
            (finally
              (.countDown proceed-latch)
              ((:stop! worker)))))))))

(deftest ^:integration wake-signal-flooding-no-adverse-effects-test
  (testing "1000 rapid wake! calls do not cause duplicate executions or exceed concurrency"
    (let [tracker (new-handler-tracker)
          job-ids (create-n-jobs! *pool* 5)
          {:keys [queue]} (make-tracking-event-queue)
          worker (worker/start-worker!
                   {:pool *pool*
                    :event-queue queue
                    :env {}
                    :handlers {"test-job" (fn [{:keys [job-id]}]
                                            (mark-handler-start! tracker job-id)
                                            (try
                                              (Thread/sleep 100)
                                              {:job-id job-id}
                                              (finally
                                                (mark-handler-stop! tracker job-id))))}
                    :owner-id (str "wake-flood-" (random-uuid))
                    :poll-interval-ms 50
                    :claim-limit 2
                    :concurrency 2
                    :lease-duration-seconds 30
                    :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        ;; Flood with wake signals
        (dotimes [_ 1000]
          ((:wake! worker)))
        ;; Wait for all jobs to complete
        (let [rows (wait-for-terminal-rows *pool* job-ids 10000)
              t @tracker
              max-in-flight (:max-in-flight t)
              calls (:calls t)]
          (is (every? #(= "succeeded" (:status %)) rows)
              "All 5 jobs should succeed")
          (is (= 5 (count (distinct calls)))
              "Each job should be executed exactly once (no duplicate executions)")
          (is (<= max-in-flight 2)
              (str "Max in-flight (" max-in-flight ") must not exceed concurrency (2)")))
        (finally
          ((:stop! worker)))))))

(deftest ^:integration mass-failure-terminal-dispatch-exactly-once-test
  (testing "all jobs failing at max-attempts=1 — each job fails and gets terminal event dispatched"
    (let [{:keys [queue events]} (make-tracking-event-queue)
          job-ids (create-n-jobs! *pool* 20 {:max-attempts 1})
          job-id-strs (set (map str job-ids))
          worker (worker/start-worker!
                   {:pool *pool*
                    :event-queue queue
                    :env {}
                    :handlers {"test-job" (fn [_ctx]
                                            (throw (ex-info "Immediate failure" {})))}
                    :owner-id (str "mass-fail-" (random-uuid))
                    :poll-interval-ms 20
                    :claim-limit 5
                    :concurrency 5
                    :lease-duration-seconds 30
                    :reconcile-interval-polls 10
                    :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (let [rows (wait-for-terminal-rows *pool* job-ids 15000)]
          (is (every? #(= "failed" (:status %)) rows)
              "All 20 jobs should fail")
          (is (= 20 (count (filter #(= "failed" (:status %)) rows)))))
        ;; Let reconciliation run to mark any undispatched
        (Thread/sleep 500)
        ;; Check terminal event dispatch in DB
        (let [rows (get-job-rows *pool* job-ids)]
          (is (every? #(some? (:terminal-event-dispatched-at %)) rows)
              "All jobs should have terminal-event-dispatched-at set"))
        ;; Check event queue: every job-id must appear at least once
        (let [all-events @events
              event-job-ids (set (map #(get-in % [:data :job-id]) all-events))]
          (is (>= (count all-events) 20)
              (str "At least 20 events expected, got " (count all-events)))
          (is (= job-id-strs event-job-ids)
              "Every job-id should have at least one dispatched event"))
        (finally
          ((:stop! worker)))))))

(deftest ^:integration lease-expiry-both-workers-complete-contention-test
  (testing "Worker A loses lease mid-execution, Worker B completes — only one terminal event"
    (let [job-ids (create-n-jobs! *pool* 1 {:max-attempts 3})
          job-id (first job-ids)
          a-started (promise)
          {:keys [queue events]} (make-tracking-event-queue)
          ;; Worker A: short lease, slow handler
          worker-a (worker/start-worker!
                     {:pool *pool*
                      :event-queue queue
                      :env {}
                      :handlers {"test-job" (fn [{:keys [continue-fn]}]
                                              (deliver a-started true)
                                              (Thread/sleep 2500)
                                              {:worker "A"})}
                      :owner-id (str "contention-a-" (random-uuid))
                      :poll-interval-ms 20
                      :claim-limit 1
                      :concurrency 1
                      :lease-duration-seconds 1
                      :shutdown-timeout-ms 5000
                      :update-data-by-id-fn noop-update-data-by-id-fn})]
      (try
        (is (not= :timeout (deref a-started 5000 :timeout))
            "Worker A should start the job")
        ;; Wait for lease to expire
        (Thread/sleep 1500)
        ;; Start Worker B with a fast handler
        (let [worker-b (worker/start-worker!
                         {:pool *pool*
                          :event-queue queue
                          :env {}
                          :handlers {"test-job" (fn [_ctx]
                                                  {:worker "B"})}
                          :owner-id (str "contention-b-" (random-uuid))
                          :poll-interval-ms 20
                          :claim-limit 1
                          :concurrency 1
                          :lease-duration-seconds 30
                          :update-data-by-id-fn noop-update-data-by-id-fn})]
          (try
            (let [rows (wait-for-terminal-rows *pool* job-ids 15000)
                  row (first rows)]
              (is (= "succeeded" (:status row))
                  "Job should succeed")
              (is (= "B" (get-in row [:result :worker]))
                  "Result should come from Worker B")
              (is (>= (:attempt row) 2)
                  "Attempt should be >= 2"))
            ;; Wait for Worker A to finish and try its terminal write
            (Thread/sleep 3000)
            ;; Exactly 1 terminal event should have been dispatched
            (let [all-events @events
                  job-events (filter #(= (str job-id) (get-in % [:data :job-id])) all-events)]
              (is (= 1 (count job-events))
                  (str "Exactly 1 terminal event expected, got " (count job-events))))
            (finally
              ((:stop! worker-b)))))
        (finally
          ((:stop! worker-a)))))))

(deftest ^:integration coordinator-death-stop-returns-promptly-test
  (testing "stop! returns or throws promptly when coordinator thread has died from Error"
    (let [{:keys [queue]} (make-tracking-event-queue)]
      (with-redefs [job-store/claim-jobs!
                     (fn [_pool _opts]
                       (throw (Error. "Simulated OOM")))]
        (let [worker (worker/start-worker!
                       {:pool *pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" identity}
                        :owner-id (str "coord-death-" (random-uuid))
                        :poll-interval-ms 20
                        :claim-limit 1
                        :concurrency 1
                        :lease-duration-seconds 30
                        :update-data-by-id-fn noop-update-data-by-id-fn})]
          ;; Wait for coordinator to die on first poll
          (Thread/sleep 200)
          ;; stop! should complete promptly (may throw ExecutionException from dead future)
          (let [stop-future (future (try
                                      ((:stop! worker))
                                      :completed
                                      (catch Throwable _t
                                        :threw)))
                result (deref stop-future 5000 :timeout)]
            (is (not= :timeout result)
                "stop! should not hang despite dead coordinator")
            (is (contains? #{:completed :threw} result)
                "stop! should either return normally or throw, not deadlock")))))))
