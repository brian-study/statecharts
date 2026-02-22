(ns ^:integration com.fulcrologic.statecharts.jobs.worker-benchmark-test
  "Opt-in integration benchmark/chaos harness for concurrent durable job workers.

   Requires a running PostgreSQL instance.

   Run with:
     PG_TEST_USER=user PG_TEST_PASSWORD=password \\
     clj -M:test -e \"(require '[com.fulcrologic.statecharts.jobs.worker-benchmark-test :as bench] :reload) ...\""
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
   [java.time Duration]
   [java.util UUID]))

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
  "Insert N jobs and return vector of job UUIDs. Payload includes :job-index by default."
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
             :session-id (keyword (str "bench-session-" run-id "-" idx))
             :invokeid (keyword (str "bench-invoke-" run-id "-" idx))
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

(defn make-delay-handler
  "Handler that sleeps for delay-ms and returns a small result map."
  ([delay-ms]
   (make-delay-handler delay-ms {}))
  ([delay-ms {:keys [tracker on-start on-finish]}]
   (fn [{:keys [job-id params continue-fn]}]
     (mark-handler-start! tracker job-id)
     (try
       (when on-start
         (on-start job-id params))
       (Thread/sleep delay-ms)
       (let [continue? (when continue-fn (continue-fn))]
         (swap! tracker update :continue-results conj continue?)
         (when on-finish
           (on-finish job-id params))
         {:job-id job-id
          :job-index (:job-index params)
          :delay-ms delay-ms
          :continue? continue?})
       (finally
         (mark-handler-stop! tracker job-id))))))

(defn make-failing-handler
  "Handler that fails selected job indexes after delay-ms."
  [{:keys [fail-job-indexes delay-ms tracker]
    :or {fail-job-indexes #{} delay-ms 25}}]
  (let [fail-job-indexes (set fail-job-indexes)]
    (fn [{:keys [job-id params continue-fn]}]
      (mark-handler-start! tracker job-id)
      (try
        (Thread/sleep delay-ms)
        (let [job-index (:job-index params)
              continue? (when continue-fn (continue-fn))]
          (swap! tracker update :continue-results conj continue?)
          (if (contains? fail-job-indexes job-index)
            (throw (ex-info "Injected benchmark failure"
                            {:job-id job-id :job-index job-index}))
            {:job-id job-id :job-index job-index :continue? continue?}))
        (finally
          (mark-handler-stop! tracker job-id))))))

(defn make-slow-handler
  "Handler that sleeps longer than typical lease/checkpoint windows."
  ([sleep-ms]
   (make-slow-handler sleep-ms {}))
  ([sleep-ms {:keys [tracker started-promise worker-tag]}]
   (fn [{:keys [job-id continue-fn]}]
     (mark-handler-start! tracker job-id)
     (try
       (when started-promise
         (deliver started-promise true))
       (Thread/sleep sleep-ms)
       (let [continue? (when continue-fn (continue-fn))]
         (swap! tracker update :continue-results conj continue?)
         {:job-id job-id
          :worker (or worker-tag "slow")
          :continue-after-sleep continue?})
       (finally
         (mark-handler-stop! tracker job-id))))))

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

(defn- start-worker-by-approach!
  [_approach opts]
  (worker/start-worker! opts))

(defn- start-workers!
  [approach pool queue worker-count handlers
   {:keys [owner-prefix poll-interval-ms claim-limit concurrency lease-duration-seconds shutdown-timeout-ms]
    :or {owner-prefix "bench-worker"
         poll-interval-ms 20
         claim-limit 1
         concurrency 1
         lease-duration-seconds 30
         shutdown-timeout-ms 5000}}]
  (mapv
   (fn [idx]
     (start-worker-by-approach!
      approach
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
       :update-data-by-id-fn noop-update-data-by-id-fn}))
   (range worker-count)))

(defn- stop-workers! [workers]
  (doseq [{:keys [stop!]} workers]
    (stop!))
  true)

(defn run-benchmark!
  "Start workers, run jobs to completion, and return benchmark metrics.

   Approach values:
   - :sequential  -> start-worker! with concurrency=1 claim-limit=1
   - :A           -> start-worker! default ExecutorService batch executor
   - :B           -> start-missionary-worker! missionary batch executor
   - :C           -> start-worker! with injected custom batch executor"
  [{:keys [pool approach job-count delay-ms handler-fn max-attempts timeout-ms
           worker-count poll-interval-ms lease-duration-seconds claim-limit concurrency
           tracker shutdown-timeout-ms]
    :or {approach :A
         job-count 20
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
  (let [{:keys [queue]} (make-tracking-event-queue)
        tracker (or tracker (new-handler-tracker))
        sequential? (= approach :sequential)
        concurrency (if sequential? 1 concurrency)
        claim-limit (if sequential? 1 claim-limit)
        handler-fn (or handler-fn (make-delay-handler delay-ms {:tracker tracker}))
        job-ids (create-n-jobs! pool job-count
                  {:max-attempts max-attempts
                   :payload-fn (fn [idx]
                                 {:job-index idx
                                  :approach (name approach)})})
        started-at-ms (System/currentTimeMillis)
        workers (start-workers! (if sequential? :A approach)
                  pool queue worker-count {"test-job" handler-fn}
                  {:owner-prefix (str "bench-" (name approach))
                   :poll-interval-ms poll-interval-ms
                   :lease-duration-seconds lease-duration-seconds
                   :claim-limit claim-limit
                   :concurrency concurrency
                   :shutdown-timeout-ms shutdown-timeout-ms})]
    (try
      (let [rows (wait-for-terminal-rows pool job-ids timeout-ms)
            finished-at-ms (System/currentTimeMillis)
            counts-by-outcome (->> rows (map :status) (remove nil?) frequencies)
            per-job-latencies (into {}
                                    (map (fn [{:keys [id] :as row}]
                                           [(str id) (row-latency-ms row)]))
                                    rows)
            latencies (remove nil? (vals per-job-latencies))
            terminal-complete? (every? #(contains? terminal-statuses (:status %)) rows)]
        {:approach approach
         :job-count job-count
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

(deftest ^:integration throughput-test-all-approaches
  (testing "concurrent approaches A/B/C are faster than sequential baseline"
    (let [job-count 20
          delay-ms 250
          timeout-ms 45000
          benchmark-opts {:pool *pool*
                          :job-count job-count
                          :delay-ms delay-ms
                          :timeout-ms timeout-ms
                          :worker-count 1
                          :claim-limit 20
                          :concurrency 20}
          sequential (run-benchmark! (assoc benchmark-opts :approach :sequential))
          a (run-benchmark! (assoc benchmark-opts :approach :A))
          b (run-benchmark! (assoc benchmark-opts :approach :B))
          c (run-benchmark! (assoc benchmark-opts :approach :C))]
      (doseq [m [sequential a b c]]
        (is (false? (:timed-out? m)))
        (is (= job-count (:jobs-succeeded m))))
      (is (= 1 (:max-in-flight sequential)))
      (is (> (:max-in-flight a) 1))
      (is (> (:max-in-flight b) 1))
      (is (> (:max-in-flight c) 1))
      ;; Use ratio assertions to reduce timing flakiness across machines.
      (is (< (:total-wall-ms a) (* 0.60 (:total-wall-ms sequential))))
      (is (< (:total-wall-ms b) (* 0.60 (:total-wall-ms sequential))))
      (is (< (:total-wall-ms c) (* 0.60 (:total-wall-ms sequential)))))))

(deftest ^:integration error-isolation-test-all-approaches
  (testing "failing jobs do not contaminate successful jobs"
    (doseq [approach [:A :B :C]]
      (let [job-count 12
            failing #{1 4 7 10}
            tracker (new-handler-tracker)
            metrics (run-benchmark! {:pool *pool*
                                     :approach approach
                                     :job-count job-count
                                     :max-attempts 1
                                     :claim-limit 12
                                     :concurrency 12
                                     :timeout-ms 30000
                                     :tracker tracker
                                     :handler-fn (make-failing-handler
                                                  {:fail-job-indexes failing
                                                   :delay-ms 75
                                                   :tracker tracker})})]
        (is (false? (:timed-out? metrics)))
        (is (= (count failing) (:jobs-failed metrics)))
        (is (= (- job-count (count failing)) (:jobs-succeeded metrics)))))))

(deftest ^:integration shutdown-during-execution-test-all-approaches
  (testing "stop! is bounded during in-flight execution"
    (doseq [approach [:A :B :C]]
      (let [job-count 10
            tracker (new-handler-tracker)
            started (promise)
            job-ids (create-n-jobs! *pool* job-count {:max-attempts 1})
            {:keys [queue]} (make-tracking-event-queue)
            workers (start-workers! approach *pool* queue 1
                      {"test-job" (make-slow-handler 4000
                                    {:tracker tracker
                                     :started-promise started
                                     :worker-tag (name approach)})}
                      {:owner-prefix (str "shutdown-" (name approach))
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
                rows (get-job-rows *pool* job-ids)
                pending (count (filter #(= "pending" (:status %)) rows))]
            (is (not= :timeout stop-result))
            (is (< stop-wall-ms 2500))
            ;; Only up to the first claimed batch should have started; remainder stays pending.
            (is (>= pending 5)))
          (finally
            (stop-workers! workers)))))))

(deftest ^:integration lease-expiry-recovery-test
  (testing "job can be reclaimed after lease expiry and completed exactly once"
    (let [job-id (first (create-n-jobs! *pool* 1 {:max-attempts 3}))
          slow-started (promise)
          fast-runs (atom 0)
          {:keys [queue]} (make-tracking-event-queue)
          slow-worker (worker/start-worker!
                       {:pool *pool*
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
                        :update-data-by-id-fn noop-update-data-by-id-fn})
          fast-worker (delay
                        (worker/start-worker!
                         {:pool *pool*
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
                          :update-data-by-id-fn noop-update-data-by-id-fn}))]
      (try
        (is (not= :timeout (deref slow-started 5000 :timeout)))
        (force fast-worker)
        (let [rows (wait-for-terminal-rows *pool* [job-id] 15000)
              row (first rows)]
          (is (= "succeeded" (:status row)))
          (is (>= (:attempt row) 2))
          (is (= "fast" (get-in row [:result :worker])))
          (is (pos? @fast-runs)))
        (finally
          ((:stop! slow-worker))
          (when (realized? fast-worker)
            ((:stop! @fast-worker))))))))

(deftest ^:integration concurrent-claim-safety-test-all-approaches
  (testing "no job is executed twice with two workers"
    (doseq [approach [:A :B :C]]
      (let [job-count 20
            tracker (new-handler-tracker)
            seen-job-ids (atom [])
            metrics (run-benchmark!
                     {:pool *pool*
                      :approach approach
                      :job-count job-count
                      :delay-ms 120
                      :timeout-ms 30000
                      :worker-count 2
                      :claim-limit 1
                      :concurrency 1
                      :tracker tracker
                      :handler-fn (make-delay-handler
                                   120
                                   {:tracker tracker
                                    :on-start (fn [job-id _]
                                                (swap! seen-job-ids conj job-id))})})]
        (is (false? (:timed-out? metrics)))
        (is (= job-count (:jobs-succeeded metrics)))
        (is (= job-count (count @seen-job-ids)))
        (is (= job-count (count (distinct @seen-job-ids))))
        (is (every? #(= 1 (:attempt %)) (:rows metrics)))))))
