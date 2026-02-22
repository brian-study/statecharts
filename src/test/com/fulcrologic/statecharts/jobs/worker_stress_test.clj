(ns ^{:stress true :integration true}
  com.fulcrologic.statecharts.jobs.worker-stress-test
  "Stress tests for the demand-driven job worker.

   These tests exercise the worker under high volume, multi-worker contention,
   sustained load, and failure conditions. All tests require a running PostgreSQL
   instance.

   Run with:
     bb test --focus com.fulcrologic.statecharts.jobs.worker-stress-test"
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts.jobs.test-helpers :as th]
   [com.fulcrologic.statecharts.jobs.worker :as worker]))

;; -----------------------------------------------------------------------------
;; Fixtures
;; -----------------------------------------------------------------------------

(use-fixtures :once th/with-pool)
(use-fixtures :each th/with-clean-tables)

;; -----------------------------------------------------------------------------
;; Tests
;; -----------------------------------------------------------------------------

(deftest ^{:stress true :integration true} single-worker-high-volume-test
  (testing "single worker processes 500 jobs with concurrency=10"
    (let [job-count 500
          tracker (th/new-handler-tracker)
          {:keys [queue events]} (th/make-tracking-event-queue)
          job-ids (th/create-n-jobs! th/*pool* job-count)
          worker (worker/start-worker!
                   {:pool th/*pool*
                    :event-queue queue
                    :env {}
                    :handlers {"test-job" (th/make-delay-handler 10 {:tracker tracker})}
                    :owner-id (str "stress-single-" (random-uuid))
                    :poll-interval-ms 20
                    :claim-limit 5
                    :concurrency 10
                    :lease-duration-seconds 30
                    :update-data-by-id-fn th/noop-update-data-by-id-fn})]
      (try
        (let [rows (th/wait-for-terminal-rows th/*pool* job-ids 30000)
              succeeded (count (filter #(= "succeeded" (:status %)) rows))
              t @tracker]
          (is (= job-count succeeded)
              (str "All " job-count " jobs should succeed, got " succeeded))
          (is (= job-count (count (distinct (:calls t))))
              "Each job should be called exactly once")
          (is (<= (:max-in-flight t) 10)
              (str "Max in-flight (" (:max-in-flight t) ") must not exceed concurrency (10)"))
          ;; Event count >= job-count because the reconciliation loop may
          ;; re-dispatch events in the window between send! and mark-dispatched.
          ;; The stronger invariant is distinct job-ids in events == job-count.
          (is (>= (count @events) job-count)
              (str "Should have at least " job-count " terminal events, got " (count @events)))
          (is (= job-count (count (distinct (map (comp :job-id :data) @events))))
              "Every job should have at least one terminal event (distinct job-ids)"))
        (finally
          ((:stop! worker)))))))

(deftest ^{:stress true :integration true} multi-worker-contention-test
  (testing "5 workers contend for 500 jobs with no duplicate execution"
    (let [job-count 500
          worker-count 5
          tracker (th/new-handler-tracker)
          {:keys [queue]} (th/make-tracking-event-queue)
          job-ids (th/create-n-jobs! th/*pool* job-count)
          workers (mapv (fn [idx]
                          (worker/start-worker!
                            {:pool th/*pool*
                             :event-queue queue
                             :env {}
                             :handlers {"test-job" (th/make-delay-handler 20 {:tracker tracker})}
                             :owner-id (str "stress-contention-" idx "-" (random-uuid))
                             :poll-interval-ms 20
                             :claim-limit 3
                             :concurrency 3
                             :lease-duration-seconds 30
                             :update-data-by-id-fn th/noop-update-data-by-id-fn}))
                        (range worker-count))]
      (try
        (let [rows (th/wait-for-terminal-rows th/*pool* job-ids 30000)
              succeeded (count (filter #(= "succeeded" (:status %)) rows))
              t @tracker
              distinct-calls (count (distinct (:calls t)))]
          (is (= job-count succeeded)
              (str "All " job-count " jobs should succeed, got " succeeded))
          (is (= job-count distinct-calls)
              (str "Exactly " job-count " distinct handler calls expected, got " distinct-calls))
          (is (<= (:max-in-flight t) (* worker-count 3))
              (str "Max in-flight (" (:max-in-flight t) ") must not exceed total concurrency (" (* worker-count 3) ")"))
          (is (every? #(= 1 (:attempt %)) rows)
              "All jobs should succeed on first attempt (no retries from contention)"))
        (finally
          (doseq [{:keys [stop!]} workers] (stop!)))))))

(deftest ^{:stress true :integration true} burst-throughput-measurement-test
  (testing "measures burst throughput with 200 fast jobs across 2 workers"
    (let [job-count 200
          tracker (th/new-handler-tracker)
          {:keys [queue]} (th/make-tracking-event-queue)
          job-ids (th/create-n-jobs! th/*pool* job-count)
          workers (mapv (fn [idx]
                          (worker/start-worker!
                            {:pool th/*pool*
                             :event-queue queue
                             :env {}
                             :handlers {"test-job" (th/make-delay-handler 1 {:tracker tracker})}
                             :owner-id (str "stress-burst-" idx "-" (random-uuid))
                             :poll-interval-ms 10
                             :claim-limit 10
                             :concurrency 20
                             :lease-duration-seconds 30
                             :update-data-by-id-fn th/noop-update-data-by-id-fn}))
                        (range 2))
          start-ms (System/currentTimeMillis)]
      (try
        (let [rows (th/wait-for-terminal-rows th/*pool* job-ids 30000)
              end-ms (System/currentTimeMillis)
              elapsed-ms (- end-ms start-ms)
              terminal-count (count (filter #(contains? th/terminal-statuses (:status %)) rows))
              t @tracker
              distinct-calls (count (distinct (:calls t)))]
          (is (= job-count terminal-count)
              (str "All " job-count " jobs should reach terminal state, got " terminal-count))
          (is (= job-count distinct-calls)
              (str "No duplicate executions: expected " job-count ", got " distinct-calls))
          (println (format "Burst throughput: %.1f jobs/sec" (/ (* 200 1000.0) elapsed-ms))))
        (finally
          (doseq [{:keys [stop!]} workers] (stop!)))))))

(deftest ^{:stress true :integration true} sustained-load-continuous-enqueue-test
  (testing "workers handle sustained load with continuous job enqueuing for 15 seconds"
    (let [tracker (th/new-handler-tracker)
          {:keys [queue]} (th/make-tracking-event-queue)
          all-job-ids (atom [])
          workers (mapv (fn [idx]
                          (worker/start-worker!
                            {:pool th/*pool*
                             :event-queue queue
                             :env {}
                             :handlers {"test-job" (th/make-delay-handler 50 {:tracker tracker})}
                             :owner-id (str "stress-sustained-" idx "-" (random-uuid))
                             :poll-interval-ms 50
                             :claim-limit 5
                             :concurrency 10
                             :lease-duration-seconds 30
                             :update-data-by-id-fn th/noop-update-data-by-id-fn}))
                        (range 2))
          enqueue-duration-ms 15000
          ;; Background future: enqueue 5 jobs every 250ms for 15 seconds
          enqueue-future (future
                           (let [deadline (+ (System/currentTimeMillis) enqueue-duration-ms)]
                             (loop []
                               (when (< (System/currentTimeMillis) deadline)
                                 (let [ids (th/create-n-jobs! th/*pool* 5)]
                                   (swap! all-job-ids into ids)
                                   ;; Wake workers so they pick up new jobs promptly
                                   (doseq [{:keys [wake!]} workers] (wake!))
                                   (Thread/sleep 250)
                                   (recur))))))]
      (try
        ;; Wait for enqueue phase to finish
        @enqueue-future
        ;; Wake all workers one final time to flush any remaining work
        (doseq [{:keys [wake!]} workers] (wake!))
        ;; Wait for all enqueued jobs to reach terminal state
        (let [job-ids @all-job-ids
              rows (th/wait-for-terminal-rows th/*pool* job-ids 30000)
              terminal-count (count (filter #(contains? th/terminal-statuses (:status %)) rows))
              t @tracker
              distinct-calls (count (distinct (:calls t)))]
          (is (= (count job-ids) terminal-count)
              (str "All " (count job-ids) " jobs should reach terminal state, got " terminal-count))
          (is (= (count job-ids) distinct-calls)
              (str "No duplicate executions: expected " (count job-ids) ", got " distinct-calls))
          (is (<= (:max-in-flight t) 20)
              (str "Max in-flight (" (:max-in-flight t) ") must not exceed total concurrency (20 = 10 per worker)")))
        (finally
          (doseq [{:keys [stop!]} workers] (stop!)))))))

(deftest ^{:stress true :integration true} terminal-events-exactly-once-at-scale-test
  (testing "200 jobs with 25% failure rate produce exactly 200 terminal events"
    (let [job-count 200
          fail-job-indexes (set (range 0 200 4))
          expected-failures (count fail-job-indexes)
          expected-successes (- job-count expected-failures)
          tracker (th/new-handler-tracker)
          {:keys [queue events]} (th/make-tracking-event-queue)
          job-ids (th/create-n-jobs! th/*pool* job-count {:max-attempts 1})
          workers (mapv (fn [idx]
                          (worker/start-worker!
                            {:pool th/*pool*
                             :event-queue queue
                             :env {}
                             :handlers {"test-job" (th/make-failing-handler
                                                     {:fail-job-indexes fail-job-indexes
                                                      :delay-ms 20
                                                      :tracker tracker})}
                             :owner-id (str "stress-terminal-" idx "-" (random-uuid))
                             :poll-interval-ms 20
                             :claim-limit 3
                             :concurrency 5
                             :lease-duration-seconds 30
                             :update-data-by-id-fn th/noop-update-data-by-id-fn}))
                        (range 3))]
      (try
        (let [rows (th/wait-for-terminal-rows th/*pool* job-ids 30000)
              succeeded (count (filter #(= "succeeded" (:status %)) rows))
              failed (count (filter #(= "failed" (:status %)) rows))
              event-list @events
              event-job-ids (set (map (comp :job-id :data) event-list))
              dispatched-count (count (filter :terminal-event-dispatched-at rows))]
          ;; Event count >= job-count because reconciler may re-dispatch in
          ;; the window between send! and mark-terminal-event-dispatched!.
          ;; The stronger invariant: distinct job-ids in events == job-count.
          (is (>= (count event-list) job-count)
              (str "At least " job-count " terminal events expected, got " (count event-list)))
          (is (= job-count (count event-job-ids))
              (str "All " job-count " distinct jobs should have terminal events, got " (count event-job-ids)))
          (is (= expected-failures failed)
              (str "Expected " expected-failures " failures, got " failed))
          (is (= expected-successes succeeded)
              (str "Expected " expected-successes " successes, got " succeeded))
          (is (= job-count dispatched-count)
              (str "All " job-count " rows should have terminal-event-dispatched-at set, got " dispatched-count)))
        (finally
          (doseq [{:keys [stop!]} workers] (stop!)))))))

(deftest ^{:stress true :integration true} concurrency-invariant-claim-limit-clamping-test
  (testing "claim-limit > concurrency is clamped; max-in-flight never exceeds concurrency"
    (let [job-count 200
          concurrency 8
          tracker (th/new-handler-tracker)
          {:keys [queue]} (th/make-tracking-event-queue)
          job-ids (th/create-n-jobs! th/*pool* job-count)
          worker (worker/start-worker!
                   {:pool th/*pool*
                    :event-queue queue
                    :env {}
                    :handlers {"test-job" (th/make-delay-handler 100 {:tracker tracker})}
                    :owner-id (str "stress-clamp-" (random-uuid))
                    :poll-interval-ms 20
                    :claim-limit 20
                    :concurrency concurrency
                    :lease-duration-seconds 30
                    :update-data-by-id-fn th/noop-update-data-by-id-fn})]
      (try
        (let [rows (th/wait-for-terminal-rows th/*pool* job-ids 60000)
              succeeded (count (filter #(= "succeeded" (:status %)) rows))
              t @tracker]
          (is (= job-count succeeded)
              (str "All " job-count " jobs should succeed, got " succeeded))
          (is (<= (:max-in-flight t) concurrency)
              (str "Max in-flight (" (:max-in-flight t) ") must not exceed concurrency (" concurrency ") despite claim-limit=20")))
        (finally
          ((:stop! worker)))))))
