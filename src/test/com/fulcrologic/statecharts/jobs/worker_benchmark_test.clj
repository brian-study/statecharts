(ns ^:integration com.fulcrologic.statecharts.jobs.worker-benchmark-test
  "Integration benchmark/chaos harness for the demand-driven job worker.

   Requires a running PostgreSQL instance.

   Run with:
     PG_TEST_USER=user PG_TEST_PASSWORD=password \\
     clj -M:test -e \"(require '[com.fulcrologic.statecharts.jobs.worker-benchmark-test :as bench] :reload) ...\""
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts.jobs.test-helpers :as th]
   [com.fulcrologic.statecharts.jobs.worker :as worker]
   [com.fulcrologic.statecharts.persistence.pg.job-store :as job-store]
   [taoensso.timbre :as log])
  (:import
   [java.time Duration]
   [java.util.concurrent CountDownLatch TimeUnit]))

;; -----------------------------------------------------------------------------
;; Fixtures — delegate to shared helpers
;; -----------------------------------------------------------------------------

(use-fixtures :once th/with-pool)
(use-fixtures :each th/with-clean-tables)

;; -----------------------------------------------------------------------------
;; Benchmark-specific Helpers
;; -----------------------------------------------------------------------------

(defn- row-latency-ms [{:keys [created-at updated-at]}]
  (when (and created-at updated-at)
    (.toMillis (Duration/between created-at updated-at))))

(defn- percentile
  [values p]
  (if (seq values)
    (let [sorted (vec (sort values))
          n (count sorted)
          idx (int (Math/ceil (* (/ p 100.0) n)))]
      (nth sorted (max 0 (dec idx))))
    0))

(defn- start-workers!
  [pool queue worker-count handlers
   {:keys [owner-prefix poll-interval-ms claim-limit concurrency lease-duration-seconds shutdown-timeout-ms]
    :or {owner-prefix "bench-worker"
         poll-interval-ms 20
         claim-limit 1
         concurrency 1
         lease-duration-seconds 30
         shutdown-timeout-ms 5000}}]
  (mapv
   (fn [idx]
     (worker/start-worker!
      {:pool pool
       :event-queue queue
       :env {}
       :handlers handlers
       :owner-id (str owner-prefix "-" idx "-" (random-uuid))
       :poll-interval-ms poll-interval-ms
       :claim-limit claim-limit
       :concurrency concurrency
       :lease-duration-seconds lease-duration-seconds
       :shutdown-timeout-ms shutdown-timeout-ms
       :update-data-by-id-fn th/noop-update-data-by-id-fn}))
   (range worker-count)))

(defn- stop-workers! [workers]
  (doseq [{:keys [stop!]} workers]
    (stop!))
  true)

(defn run-benchmark!
  "Start workers, run jobs to completion, and return benchmark metrics."
  [{:keys [pool job-count delay-ms handler-fn max-attempts timeout-ms
           worker-count poll-interval-ms lease-duration-seconds claim-limit concurrency
           tracker shutdown-timeout-ms]
    :or {job-count 20
         delay-ms 200
         max-attempts 1
         timeout-ms 20000
         worker-count 1
         poll-interval-ms 20
         lease-duration-seconds 30
         claim-limit 10
         concurrency 10
         shutdown-timeout-ms 5000}}]
  (assert pool "pool is required")
  (let [{:keys [queue]} (th/make-tracking-event-queue)
        tracker (or tracker (th/new-handler-tracker))
        handler-fn (or handler-fn (th/make-delay-handler delay-ms {:tracker tracker}))
        job-ids (th/create-n-jobs! pool job-count
                  {:max-attempts max-attempts
                   :session-prefix "bench"
                   :payload-fn (fn [idx] {:job-index idx})})
        started-at-ms (System/currentTimeMillis)
        workers (start-workers!
                  pool queue worker-count {"test-job" handler-fn}
                  {:owner-prefix "bench"
                   :poll-interval-ms poll-interval-ms
                   :lease-duration-seconds lease-duration-seconds
                   :claim-limit claim-limit
                   :concurrency concurrency
                   :shutdown-timeout-ms shutdown-timeout-ms})]
    (try
      (let [rows (th/wait-for-terminal-rows pool job-ids timeout-ms)
            finished-at-ms (System/currentTimeMillis)
            counts-by-outcome (->> rows (map :status) (remove nil?) frequencies)
            per-job-latencies (into {}
                                    (map (fn [{:keys [id] :as row}]
                                           [(str id) (row-latency-ms row)]))
                                    rows)
            latencies (remove nil? (vals per-job-latencies))
            terminal-complete? (every? #(contains? th/terminal-statuses (:status %)) rows)]
        {:job-count job-count
         :worker-count worker-count
         :concurrency concurrency
         :claim-limit claim-limit
         :started-at-ms started-at-ms
         :finished-at-ms finished-at-ms
         :total-wall-ms (- finished-at-ms started-at-ms)
         :per-job-latencies per-job-latencies
         :p50-ms (percentile latencies 50)
         :p95-ms (percentile latencies 95)
         :p99-ms (percentile latencies 99)
         :counts-by-outcome counts-by-outcome
         :max-in-flight (:max-in-flight @tracker)
         :jobs-succeeded (get counts-by-outcome "succeeded" 0)
         :jobs-failed (get counts-by-outcome "failed" 0)
         :jobs-cancelled (get counts-by-outcome "cancelled" 0)
         :handler-latencies-ms (:handler-latencies-ms @tracker)
         :timed-out? (not terminal-complete?)
         :rows rows
         :job-ids job-ids})
      (finally
        (stop-workers! workers)))))

;; -----------------------------------------------------------------------------
;; Tests
;; -----------------------------------------------------------------------------

(deftest ^:integration throughput-test
  (testing "concurrent worker is faster than sequential baseline"
    (let [job-count 20
          delay-ms 250
          timeout-ms 45000
          benchmark-opts {:pool th/*pool*
                          :job-count job-count
                          :delay-ms delay-ms
                          :timeout-ms timeout-ms
                          :worker-count 1
                          :claim-limit 20
                          :concurrency 20}
          sequential (run-benchmark! (assoc benchmark-opts :concurrency 1 :claim-limit 1))
          concurrent (run-benchmark! benchmark-opts)]
      (doseq [m [sequential concurrent]]
        (is (false? (:timed-out? m)))
        (is (= job-count (:jobs-succeeded m))))
      (is (= 1 (:max-in-flight sequential)))
      (is (> (:max-in-flight concurrent) 1))
      ;; Use ratio assertions to reduce timing flakiness across machines.
      (is (< (:total-wall-ms concurrent) (* 0.60 (:total-wall-ms sequential)))))))

(deftest ^:integration error-isolation-test
  (testing "failing jobs do not contaminate successful jobs"
    (let [job-count 12
          failing #{1 4 7 10}
          tracker (th/new-handler-tracker)
          metrics (run-benchmark! {:pool th/*pool*
                                   :job-count job-count
                                   :max-attempts 1
                                   :claim-limit 12
                                   :concurrency 12
                                   :timeout-ms 30000
                                   :tracker tracker
                                   :handler-fn (th/make-failing-handler
                                                {:fail-job-indexes failing
                                                 :delay-ms 75
                                                 :tracker tracker})})]
      (is (false? (:timed-out? metrics)))
      (is (= (count failing) (:jobs-failed metrics)))
      (is (= (- job-count (count failing)) (:jobs-succeeded metrics))))))

(deftest ^:integration shutdown-during-execution-test
  (testing "stop! is bounded during in-flight execution"
    (let [job-count 10
          tracker (th/new-handler-tracker)
          started (promise)
          job-ids (th/create-n-jobs! th/*pool* job-count {:max-attempts 1 :session-prefix "bench"})
          {:keys [queue]} (th/make-tracking-event-queue)
          workers (start-workers! th/*pool* queue 1
                    {"test-job" (th/make-slow-handler 4000
                                  {:tracker tracker
                                   :started-promise started
                                   :worker-tag "shutdown"})}
                    {:owner-prefix "shutdown"
                     :poll-interval-ms 10
                     :claim-limit 5
                     :concurrency 5
                     :lease-duration-seconds 30
                     :shutdown-timeout-ms 1200})]
      (try
        (is (not= :timeout (deref started 5000 :timeout)))
        (Thread/sleep 150)
        (let [{:keys [stop!]} (first workers)
              stop-start (System/currentTimeMillis)
              stop-result (deref (future (stop!)) 5000 :timeout)
              stop-wall-ms (- (System/currentTimeMillis) stop-start)
              rows (th/get-job-rows th/*pool* job-ids)
              pending (count (filter #(= "pending" (:status %)) rows))]
          (is (not= :timeout stop-result))
          (is (< stop-wall-ms 2500))
          ;; Only up to the first claimed batch should have started; remainder stays pending.
          (is (>= pending 5)))
        (finally
          (stop-workers! workers))))))

(deftest ^:integration lease-expiry-recovery-test
  (testing "job can be reclaimed after lease expiry and completed exactly once"
    (let [job-id (first (th/create-n-jobs! th/*pool* 1 {:max-attempts 3 :session-prefix "bench"}))
          slow-started (promise)
          fast-runs (atom 0)
          {:keys [queue]} (th/make-tracking-event-queue)
          slow-worker (worker/start-worker!
                       {:pool th/*pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" (fn [{:keys [continue-fn]}]
                                                 (deliver slow-started true)
                                                 (Thread/sleep 2400)
                                                 {:worker "slow"
                                                  :continue-after-sleep (continue-fn)})}
                        :owner-id "lease-slow"
                        :poll-interval-ms 20
                        :claim-limit 1
                        :concurrency 1
                        :lease-duration-seconds 1
                        :update-data-by-id-fn th/noop-update-data-by-id-fn})
          fast-worker (delay
                        (worker/start-worker!
                         {:pool th/*pool*
                          :event-queue queue
                          :env {}
                          :handlers {"test-job" (fn [_]
                                                   (swap! fast-runs inc)
                                                   {:worker "fast"})}
                          :owner-id "lease-fast"
                          :poll-interval-ms 20
                          :claim-limit 1
                          :concurrency 1
                          :lease-duration-seconds 1
                          :update-data-by-id-fn th/noop-update-data-by-id-fn}))]
      (try
        (is (not= :timeout (deref slow-started 5000 :timeout)))
        (force fast-worker)
        (let [rows (th/wait-for-terminal-rows th/*pool* [job-id] 15000)
              row (first rows)]
          (is (= "succeeded" (:status row)))
          (is (>= (:attempt row) 2))
          (is (= "fast" (get-in row [:result :worker])))
          (is (pos? @fast-runs)))
        (finally
          ((:stop! slow-worker))
          (when (realized? fast-worker)
            ((:stop! @fast-worker))))))))

(deftest ^:integration concurrent-claim-safety-test
  (testing "no job is executed twice with two workers"
    (let [job-count 20
          tracker (th/new-handler-tracker)
          seen-job-ids (atom [])
          metrics (run-benchmark!
                   {:pool th/*pool*
                    :job-count job-count
                    :delay-ms 120
                    :timeout-ms 30000
                    :worker-count 2
                    :claim-limit 1
                    :concurrency 1
                    :tracker tracker
                    :handler-fn (th/make-delay-handler
                                 120
                                 {:tracker tracker
                                  :on-start (fn [job-id _]
                                              (swap! seen-job-ids conj job-id))})})]
      (is (false? (:timed-out? metrics)))
      (is (= job-count (:jobs-succeeded metrics)))
      (is (= job-count (count @seen-job-ids)))
      (is (= job-count (count (distinct @seen-job-ids))))
      (is (every? #(= 1 (:attempt %)) (:rows metrics))))))

;; -----------------------------------------------------------------------------
;; Permit / Concurrency Invariant Tests
;; -----------------------------------------------------------------------------

(deftest ^:integration permit-leak-regression-test
  (testing "worker recovers after claim-jobs! throws — no permit leak"
    (let [call-count (atom 0)
          original-claim-jobs! job-store/claim-jobs!
          job-count 5
          tracker (th/new-handler-tracker)
          job-ids (th/create-n-jobs! th/*pool* job-count {:session-prefix "bench"})
          {:keys [queue]} (th/make-tracking-event-queue)]
      ;; with-redefs must wrap the entire test body (not just start-worker!)
      ;; because the coordinator polls on a future thread — if with-redefs
      ;; only wraps start-worker!, the mock is restored before the first poll.
      (with-redefs [job-store/claim-jobs!
                     (fn [pool opts]
                       (if (= 1 (swap! call-count inc))
                         (throw (ex-info "Simulated claim-jobs! failure" {}))
                         (original-claim-jobs! pool opts)))]
        (let [worker (worker/start-worker!
                       {:pool th/*pool*
                        :event-queue queue
                        :env {}
                        :handlers {"test-job" (th/make-delay-handler 30 {:tracker tracker})}
                        :owner-id (str "permit-leak-" (random-uuid))
                        :poll-interval-ms 20
                        :claim-limit 2
                        :concurrency 2
                        :lease-duration-seconds 30
                        :update-data-by-id-fn th/noop-update-data-by-id-fn})]
          (try
            (let [rows (th/wait-for-terminal-rows th/*pool* job-ids 10000)]
              (is (every? #(= "succeeded" (:status %)) rows)
                  "All jobs should succeed despite claim-jobs! throwing on first call")
              (is (= job-count (count (filter #(= "succeeded" (:status %)) rows)))))
            (finally
              ((:stop! worker)))))))))

(deftest ^:integration demand-driven-slot-filling-test
  (testing "freed slots are filled immediately, not after batch completes"
    (let [tracker (th/new-handler-tracker)
          ;; job 0 = slow (800ms), jobs 1,2,3 = fast (50ms)
          job-ids (th/create-n-jobs! th/*pool* 4
                    {:session-prefix "bench"
                     :payload-fn (fn [idx] {:job-index idx
                                            :delay-ms (if (zero? idx) 800 50)})})
          {:keys [queue]} (th/make-tracking-event-queue)
          handler-fn (fn [{:keys [job-id params] :as ctx}]
                       (th/mark-handler-start! tracker job-id)
                       (try
                         (Thread/sleep (:delay-ms params))
                         {:job-id job-id :job-index (:job-index params)}
                         (finally
                           (th/mark-handler-stop! tracker job-id))))
          worker (worker/start-worker!
                   {:pool th/*pool*
                    :event-queue queue
                    :env {}
                    :handlers {"test-job" handler-fn}
                    :owner-id (str "demand-" (random-uuid))
                    :poll-interval-ms 20
                    :claim-limit 2
                    :concurrency 2
                    :lease-duration-seconds 30
                    :update-data-by-id-fn th/noop-update-data-by-id-fn})]
      (try
        (let [rows (th/wait-for-terminal-rows th/*pool* job-ids 5000)
              t @tracker
              started (:started-at-ms t)
              finished (:finished-at-ms t)
              ;; Find the slow job (job-index 0) and a fast job that started after a slot freed
              slow-job-id (str (nth job-ids 0))
              ;; job 2 (third job) must have started in a slot freed by a fast job finishing
              third-job-id (str (nth job-ids 2))]
          (is (every? #(= "succeeded" (:status %)) rows)
              "All 4 jobs should succeed")
          ;; The critical assertion: job 2 started BEFORE the slow job 0 finished.
          ;; This proves demand-driven filling — a fast job finished, freed the slot,
          ;; and job 2 was claimed into that slot while job 0 was still running.
          (when (and (get started third-job-id)
                     (get finished slow-job-id))
            (is (< (get started third-job-id) (get finished slow-job-id))
                "Third job should start before slow job finishes (demand-driven slot filling)")))
        (finally
          ((:stop! worker)))))))

(deftest ^:integration concurrency-invariant-under-load-test
  (testing "max in-flight never exceeds concurrency limit across many drain/release cycles"
    (let [job-count 30
          concurrency 3
          tracker (th/new-handler-tracker)
          job-ids (th/create-n-jobs! th/*pool* job-count {:session-prefix "bench"})
          {:keys [queue]} (th/make-tracking-event-queue)
          worker (worker/start-worker!
                   {:pool th/*pool*
                    :event-queue queue
                    :env {}
                    :handlers {"test-job" (th/make-delay-handler 30 {:tracker tracker})}
                    :owner-id (str "concurrency-inv-" (random-uuid))
                    :poll-interval-ms 20
                    :claim-limit 5
                    :concurrency concurrency
                    :lease-duration-seconds 30
                    :update-data-by-id-fn th/noop-update-data-by-id-fn})]
      (try
        (let [rows (th/wait-for-terminal-rows th/*pool* job-ids 15000)
              t @tracker
              max-in-flight (:max-in-flight t)]
          (is (every? #(= "succeeded" (:status %)) rows)
              "All 30 jobs should succeed")
          (is (<= max-in-flight concurrency)
              (str "Max in-flight (" max-in-flight ") must not exceed concurrency (" concurrency ")"))
          (is (>= max-in-flight 2)
              (str "Max in-flight (" max-in-flight ") should be >= 2 (actually concurrent, not sequential)")))
        (finally
          ((:stop! worker)))))))

(deftest ^:integration stop-race-lease-recovery-test
  (testing "jobs claimed but not executed during stop self-heal via lease expiry"
    (let [job-count 5
          lease-duration 2
          started-latch (CountDownLatch. 1)
          block-latch (CountDownLatch. 1)
          job-ids (th/create-n-jobs! th/*pool* job-count {:max-attempts 3 :session-prefix "bench"})
          {:keys [queue]} (th/make-tracking-event-queue)
          ;; Worker A: handler signals start then blocks forever
          worker-a (worker/start-worker!
                     {:pool th/*pool*
                      :event-queue queue
                      :env {}
                      :handlers {"test-job" (fn [_ctx]
                                              (.countDown started-latch)
                                              (.await block-latch)
                                              {:worker "a"})}
                      :owner-id (str "stop-race-a-" (random-uuid))
                      :poll-interval-ms 20
                      :claim-limit 5
                      :concurrency 5
                      :lease-duration-seconds lease-duration
                      :shutdown-timeout-ms 1000
                      :update-data-by-id-fn th/noop-update-data-by-id-fn})]
      (try
        ;; Wait for at least one handler to start
        (.await started-latch 5 TimeUnit/SECONDS)
        ;; Release the block latch so handlers can complete during shutdown
        (.countDown block-latch)
        ;; Stop worker A — some jobs may be claimed but not executing
        ((:stop! worker-a))
        ;; Wait for lease to expire so stranded jobs become claimable
        (Thread/sleep (* (inc lease-duration) 1000))
        ;; Start worker B with a fast handler to pick up any stranded jobs
        (let [worker-b (worker/start-worker!
                         {:pool th/*pool*
                          :event-queue queue
                          :env {}
                          :handlers {"test-job" (fn [_ctx] {:worker "b"})}
                          :owner-id (str "stop-race-b-" (random-uuid))
                          :poll-interval-ms 20
                          :claim-limit 5
                          :concurrency 5
                          :lease-duration-seconds 30
                          :update-data-by-id-fn th/noop-update-data-by-id-fn})]
          (try
            (let [rows (th/wait-for-terminal-rows th/*pool* job-ids 15000)]
              (is (every? #(contains? th/terminal-statuses (:status %)) rows)
                  "All jobs should reach terminal status via lease recovery"))
            (finally
              ((:stop! worker-b)))))
        (finally
          ;; Ensure block latch is released in case of early exit
          (.countDown block-latch))))))
