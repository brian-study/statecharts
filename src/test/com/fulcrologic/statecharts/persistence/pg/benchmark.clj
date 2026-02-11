(ns com.fulcrologic.statecharts.persistence.pg.benchmark
  "Load benchmark harness for the PostgreSQL working-memory store.

   This namespace is intentionally plain (no deftest) so it can run from
   `clojure -M:test -m ...` and from bb task orchestration."
  (:require
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.persistence.pg.schema :as schema]
   [com.fulcrologic.statecharts.persistence.pg.working-memory-store :as pg-wms]
   [com.fulcrologic.statecharts.protocols :as sp]
   [pg.pool :as pool])
  (:import
   (java.util ArrayList Arrays Random)
   (java.util.concurrent CyclicBarrier)))

;; -----------------------------------------------------------------------------
;; Defaults and Scenario Catalog
;; -----------------------------------------------------------------------------

(def default-workload
  {:sessions 1000
   :duration-sec 10
   :read-pct 70
   :write-pct 25
   :delete-pct 5
   :seed 42})

(defn default-pool-config
  "Pool config used by benchmarks. Binary flags are always enabled for BYTEA."
  []
  {:host (or (System/getenv "PG_TEST_HOST") "localhost")
   :port (parse-long (or (System/getenv "PG_TEST_PORT") "5432"))
   :database (or (System/getenv "PG_TEST_DATABASE") "statecharts_test")
   :user (or (System/getenv "PG_TEST_USER") "postgres")
   :password (or (System/getenv "PG_TEST_PASSWORD") "postgres")
   :binary-encode? true
   :binary-decode? true})

(def ^:private baseline-scenarios
  [{:name "baseline-1t" :instances 1 :threads-per-instance 1 :cache-size 0}
   {:name "baseline-4t" :instances 1 :threads-per-instance 4 :cache-size 0}
   {:name "baseline-8t" :instances 1 :threads-per-instance 8 :cache-size 0}
   {:name "baseline-16t" :instances 1 :threads-per-instance 16 :cache-size 0}])

(def ^:private cached-scenarios
  [{:name "cached-1i-4t" :instances 1 :threads-per-instance 4 :cache-size 256}
   {:name "cached-1i-8t" :instances 1 :threads-per-instance 8 :cache-size 256}
   {:name "cached-4i-4t" :instances 4 :threads-per-instance 4 :cache-size 256}
   {:name "cached-4i-8t" :instances 4 :threads-per-instance 8 :cache-size 256}
   {:name "cached-read-heavy"
    :instances 1
    :threads-per-instance 8
    :cache-size 256
    :workload-overrides {:read-pct 90 :write-pct 8 :delete-pct 2}}])

;; -----------------------------------------------------------------------------
;; Optional Cache API (loaded dynamically so phase-1 baseline can run alone)
;; -----------------------------------------------------------------------------

(def ^:private cache-api
  (delay
    (try
      (require 'com.fulcrologic.statecharts.persistence.pg.cached-working-memory-store)
      {:new-store (resolve 'com.fulcrologic.statecharts.persistence.pg.cached-working-memory-store/new-caching-store)
       :cache-stats (resolve 'com.fulcrologic.statecharts.persistence.pg.cached-working-memory-store/cache-stats)}
      (catch Throwable _
        nil))))

(defn cache-supported?
  "True when cached-working-memory-store is on classpath."
  []
  (boolean @cache-api))

;; -----------------------------------------------------------------------------
;; Internal Helpers
;; -----------------------------------------------------------------------------

(def ^:private op->code {:read 0 :write 1 :delete 2})
(def ^:private code->op {0 :read 1 :write 2 :delete})

(def ^:private status-ok 0)
(def ^:private status-optimistic-lock 1)
(def ^:private status-error 2)

(def ^:private payload-blob
  ;; ~1KB realistic payload once wrapped in working-memory map + metadata
  (apply str (repeat 768 "x")))

(defn- valid-workload?
  [{:keys [sessions duration-sec read-pct write-pct delete-pct seed]}]
  (and (pos? sessions)
       (pos? duration-sec)
       (>= read-pct 0)
       (>= write-pct 0)
       (>= delete-pct 0)
       (= 100 (+ read-pct write-pct delete-pct))
       (integer? seed)))

(defn- normalize-workload
  [workload]
  (let [w (merge default-workload workload)]
    (when-not (valid-workload? w)
      (throw (ex-info "Invalid workload map"
                      {:workload w
                       :expected "positive sessions/duration; read+write+delete must equal 100"})))
    w))

(defn- normalize-pool-config
  [pool-config]
  (merge (default-pool-config) pool-config
         {:binary-encode? true
          :binary-decode? true}))

(defn- with-pool-size
  "Ensure pool has enough connections to avoid benchmarking queue wait
   as the dominant bottleneck."
  [pool-config threads-per-instance]
  (let [required (max 10 (+ threads-per-instance 2))
        configured (long (or (:pool-size pool-config) 0))]
    (assoc pool-config :pool-size (max required configured))))

(defn- make-session-ids
  [n]
  (mapv #(format "bench-session-%06d" %) (range n)))

(defn- initial-working-memory
  [session-id ^Random rng]
  {::sc/session-id session-id
   ::sc/statechart-src :benchmark/chart
   ::sc/configuration #{:state/active}
   ::sc/initialized-states #{:state/active}
   ::sc/history-value {}
   ::sc/running? true
   ::sc/data-model {:rev 0
                    :sample-a (.nextInt rng 1000000)
                    :sample-b (.nextInt rng 1000000)
                    :blob payload-blob}})

(defn- next-working-memory
  [session-id current-wmem ^Random rng]
  (let [wmem (or current-wmem (initial-working-memory session-id rng))
        data (or (::sc/data-model wmem) {})
        rev (long (or (:rev data) 0))]
    (assoc wmem ::sc/data-model
           (assoc data
                  :rev (inc rev)
                  :sample-a (.nextInt rng 1000000)
                  :sample-b (.nextInt rng 1000000)))))

(defn- pick-op
  [{:keys [read-pct write-pct]} ^Random rng]
  (let [n (.nextInt rng 100)]
    (cond
      (< n read-pct) :read
      (< n (+ read-pct write-pct)) :write
      :else :delete)))

(defn- nanos->ms
  [n]
  (/ (double n) 1000000.0))

(defn- weighted-ms
  [items key]
  (let [weighted-sum (reduce + (map (fn [{:keys [count] :as item}]
                                      (* (double count) (double (get item key 0.0))))
                                    items))
        total (reduce + (map :count items))]
    (if (pos? total)
      (/ weighted-sum total)
      0.0)))

(defn- long-array-summary
  [^longs arr]
  (let [n (alength arr)]
    (if (zero? n)
      {:count 0 :p50-ms 0.0 :p95-ms 0.0 :p99-ms 0.0 :max-ms 0.0}
      (let [idx (fn [pct]
                  (let [rank (long (Math/ceil (* (/ pct 100.0) n)))
                        raw (dec rank)]
                    (-> raw (max 0) (min (dec n)))))
            p50 (nanos->ms (aget arr (idx 50.0)))
            p95 (nanos->ms (aget arr (idx 95.0)))
            p99 (nanos->ms (aget arr (idx 99.0)))
            mx  (nanos->ms (aget arr (dec n)))]
        {:count n :p50-ms p50 :p95-ms p95 :p99-ms p99 :max-ms mx}))))

(defn- arraylist->sorted-long-array
  [^ArrayList values]
  (let [n (.size values)
        arr (long-array n)]
    (dotimes [i n]
      (aset-long arr i (long (.get values i))))
    (Arrays/sort arr)
    arr))

(defn- apply-op!
  [store session-id op ^Random rng]
  (case op
    :read
    (sp/get-working-memory store {} session-id)

    :write
    (let [current (sp/get-working-memory store {} session-id)
          next-wmem (next-working-memory session-id current rng)]
      (sp/save-working-memory! store {} session-id next-wmem))

    :delete
    (sp/delete-working-memory! store {} session-id)))

(defn- run-thread!
  [{:keys [store session-ids workload thread-seed start-barrier duration-ns]}]
  (let [^Random rng (Random. thread-seed)
        ^ArrayList records (ArrayList.)
        sid-count (count session-ids)
        outcome {:errors 0 :optimistic-lock-failures 0}]
    (.await ^CyclicBarrier start-barrier)
    (let [run-start-ns (System/nanoTime)
          deadline-ns (+ run-start-ns duration-ns)]
      (loop [acc outcome]
        (if (>= (System/nanoTime) deadline-ns)
          (assoc acc
                 :records records
                 :run-duration-ns (- (System/nanoTime) run-start-ns))
          (let [session-id (nth session-ids (.nextInt rng sid-count))
                op (pick-op workload rng)
                started-ns (System/nanoTime)
                status (volatile! status-ok)
                acc'
                (try
                  (apply-op! store session-id op rng)
                  acc
                  (catch Exception e
                    (if (pg-wms/optimistic-lock-failure? e)
                      (do
                        (vreset! status status-optimistic-lock)
                        (update acc :optimistic-lock-failures inc))
                      (do
                        (vreset! status status-error)
                        (update acc :errors inc)))))
                elapsed-ns (- (System/nanoTime) started-ns)]
            (.add records
                  (long-array [(long (op->code op))
                               elapsed-ns
                               (long @status)]))
            (recur acc')))))))

(defn- maybe-wrap-cache
  [store cache-size]
  (if (pos? cache-size)
    (if-let [{:keys [new-store]} @cache-api]
      (new-store store cache-size)
      (throw (ex-info "Cache scenario requested but cache store is not available"
                      {:cache-size cache-size
                       :hint "Implement com.fulcrologic.statecharts.persistence.pg.cached-working-memory-store first."})))
    store))

(defn- prepare-session-data!
  [{:keys [pool-config workload]}]
  (let [pool-config (normalize-pool-config pool-config)
        workload (normalize-workload workload)
        session-ids (make-session-ids (:sessions workload))
        p (pool/pool pool-config)
        store (pg-wms/new-store p)
        rng (Random. (:seed workload))]
    (try
      (schema/create-tables! p)
      (schema/truncate-tables! p)
      (doseq [sid session-ids]
        (sp/save-working-memory! store {} sid (initial-working-memory sid rng)))
      session-ids
      (finally
        (pool/close p)))))

(defn prepare-db!
  "Create/truncate tables and seed deterministic session rows for a workload.
   Used by both single-process runs and cluster coordinator mode."
  [opts]
  (prepare-session-data! {:pool-config (normalize-pool-config (:pool-config opts))
                          :workload (normalize-workload (:workload opts))}))

(defn- cache-stats-for-instances
  [instance-stores]
  (when-let [{:keys [cache-stats]} @cache-api]
    (let [stats (keep (fn [{:keys [store]}]
                        (try
                          (cache-stats store)
                          (catch Throwable _
                            nil)))
                      instance-stores)]
      (when (seq stats)
        (let [hits (reduce + (map :hits stats))
              misses (reduce + (map :misses stats))
              size (reduce + (map :size stats))
              max-size (reduce + (map :max-size stats))
              total (+ hits misses)]
          {:hits hits
           :misses misses
           :hit-rate (if (pos? total) (/ hits total) 0.0)
           :size size
           :max-size max-size
           :instances (vec stats)})))))

(defn- aggregate-thread-results
  [thread-results]
  (let [^ArrayList read-lats (ArrayList.)
        ^ArrayList write-lats (ArrayList.)
        ^ArrayList delete-lats (ArrayList.)
        errors (reduce + (map :errors thread-results))
        optimistic (reduce + (map :optimistic-lock-failures thread-results))]
    (doseq [{:keys [records]} thread-results
            record (iterator-seq (.iterator ^ArrayList records))]
      (let [^longs entry record
            op-code (int (aget entry 0))
            elapsed (long (aget entry 1))
            op (code->op op-code)]
        (case op
          :read (.add read-lats elapsed)
          :write (.add write-lats elapsed)
          :delete (.add delete-lats elapsed))))
    (let [read-summary (long-array-summary (arraylist->sorted-long-array read-lats))
          write-summary (long-array-summary (arraylist->sorted-long-array write-lats))
          delete-summary (long-array-summary (arraylist->sorted-long-array delete-lats))
          total (+ (:count read-summary) (:count write-summary) (:count delete-summary))]
      {:total-ops total
       :errors errors
       :optimistic-lock-failures optimistic
       :by-op {:read read-summary
               :write write-summary
               :delete delete-summary}})))

(defn run-scenario!
  "Run a single in-process scenario.

   Scenario keys:
   - :name
   - :instances
   - :threads-per-instance
   - :cache-size
   - :workload-overrides (optional)

   Options:
   - :pool-config
   - :workload
   - :prepared?  (true => skip create/truncate/seed)"
  [scenario {:keys [pool-config workload prepared?] :as _opts}]
  (let [{:keys [name instances threads-per-instance cache-size workload-overrides]
         :or {instances 1 threads-per-instance 1 cache-size 0}} scenario
        pool-config (normalize-pool-config pool-config)
        pool-config (with-pool-size pool-config threads-per-instance)
        workload (normalize-workload (merge workload workload-overrides))
        _ (when-not prepared?
            (prepare-session-data! {:pool-config pool-config :workload workload}))
        session-ids (make-session-ids (:sessions workload))
        instance-stores
        (vec
         (for [_ (range instances)]
           (let [p (pool/pool pool-config)
                 base-store (pg-wms/new-store p)
                 store (maybe-wrap-cache base-store cache-size)]
             {:pool p :store store})))]
    (try
      (let [total-threads (* instances threads-per-instance)
            start-barrier (CyclicBarrier. total-threads)
            duration-ns (* 1000000000 (:duration-sec workload))
            futures
            (vec
             (for [instance-idx (range instances)
                   thread-idx (range threads-per-instance)]
               (let [store (:store (nth instance-stores instance-idx))
                     thread-seed (+ (:seed workload)
                                    (* 100000 instance-idx)
                                    thread-idx)]
                 (future
                   (run-thread! {:store store
                                 :session-ids session-ids
                                 :workload workload
                                 :thread-seed thread-seed
                                 :start-barrier start-barrier
                                 :duration-ns duration-ns})))))
            thread-results (mapv deref futures)
            duration-ms (nanos->ms (reduce max 0 (map :run-duration-ns thread-results)))
            {:keys [total-ops errors optimistic-lock-failures by-op]}
            (aggregate-thread-results thread-results)
            cache-stats (cache-stats-for-instances instance-stores)]
        {:scenario name
         :instances instances
         :threads-per-instance threads-per-instance
         :pool-size (:pool-size pool-config)
         :cache-size cache-size
         :total-ops total-ops
         :duration-ms duration-ms
         :throughput (if (pos? duration-ms)
                       (/ total-ops (/ duration-ms 1000.0))
                       0.0)
         :by-op by-op
         :errors errors
         :optimistic-lock-failures optimistic-lock-failures
         :cache-stats cache-stats
         :workload workload})
      (finally
        (doseq [{:keys [pool]} instance-stores]
          (pool/close pool))))))

(defn run-worker!
  "Worker-friendly entrypoint (single instance, configurable threads/cache).
   Intended for multi-process cluster orchestration."
  [{:keys [name threads-per-instance cache-size prepared?]
    :or {name "worker-scenario" threads-per-instance 8 cache-size 0 prepared? true}
    :as opts}]
  (run-scenario! {:name name
                  :instances 1
                  :threads-per-instance threads-per-instance
                  :cache-size cache-size}
                 (assoc opts :prepared? prepared?)))

(defn run-baseline!
  "Run baseline scenarios (no cache) and return a summary map."
  [opts]
  {:type :baseline
   :cache-supported? (cache-supported?)
   :results (mapv #(run-scenario! % opts) baseline-scenarios)})

(defn run-all!
  "Run all available scenarios.

   If cache namespace is unavailable, cache scenarios are reported in :skipped."
  [opts]
  (let [cache? (cache-supported?)
        scenarios (if cache?
                    (vec (concat baseline-scenarios cached-scenarios))
                    baseline-scenarios)
        skipped (if cache?
                  []
                  (mapv :name cached-scenarios))]
    {:type :all
     :cache-supported? cache?
     :skipped skipped
     :results (mapv #(run-scenario! % opts) scenarios)}))

(defn merge-worker-results
  "Merge per-worker metrics into a single cluster summary.
   Percentiles are weighted averages across workers (approximate)."
  [scenario-name worker-results]
  (let [all-cache-stats (keep :cache-stats worker-results)
        hits (reduce + (map :hits all-cache-stats))
        misses (reduce + (map :misses all-cache-stats))
        size (reduce + (map :size all-cache-stats))
        max-size (reduce + (map :max-size all-cache-stats))
        op-summary
        (into {}
              (for [op [:read :write :delete]]
                (let [items (keep #(get-in % [:by-op op]) worker-results)
                      cnt (reduce + (map :count items))]
                  [op {:count cnt
                       :p50-ms (weighted-ms items :p50-ms)
                       :p95-ms (weighted-ms items :p95-ms)
                       :p99-ms (weighted-ms items :p99-ms)
                       :max-ms (if (seq items)
                                 (apply max (map :max-ms items))
                                 0.0)}])))
        total-ops (reduce + (map :total-ops worker-results))
        duration-ms (if (seq worker-results)
                      (apply max (map :duration-ms worker-results))
                      0.0)
        errors (reduce + (map :errors worker-results))
        optimistic-lock-failures (reduce + (map :optimistic-lock-failures worker-results))]
    {:scenario scenario-name
     :workers (count worker-results)
     :aggregation :weighted-percentiles
     :total-ops total-ops
     :duration-ms duration-ms
     :throughput (if (pos? duration-ms)
                   (/ total-ops (/ duration-ms 1000.0))
                   0.0)
     :by-op op-summary
     :errors errors
     :optimistic-lock-failures optimistic-lock-failures
     :cache-stats (when (seq all-cache-stats)
                    {:hits hits
                     :misses misses
                     :hit-rate (if (pos? (+ hits misses))
                                 (/ hits (+ hits misses))
                                 0.0)
                     :size size
                     :max-size max-size})}))

(defn print-summary!
  [result]
  (doseq [entry (:results result)]
    (println (format "%-20s ops=%9d throughput=%10.1f/s errors=%4d optimistic=%4d pool=%d"
                     (:scenario entry)
                     (:total-ops entry)
                     (:throughput entry)
                     (:errors entry)
                     (:optimistic-lock-failures entry)
                     (:pool-size entry)))
    (println (format "  p95-ms read=%7.3f write=%7.3f delete=%7.3f"
                     (get-in entry [:by-op :read :p95-ms] 0.0)
                     (get-in entry [:by-op :write :p95-ms] 0.0)
                     (get-in entry [:by-op :delete :p95-ms] 0.0)))
    (when-let [cache (:cache-stats entry)]
      (println (format "  cache hit-rate=%5.1f%% hits=%d misses=%d size=%d/%d"
                       (* 100.0 (double (:hit-rate cache)))
                       (:hits cache)
                       (:misses cache)
                       (:size cache)
                       (:max-size cache)))))
  (when-let [skipped (:skipped result)]
    (when (seq skipped)
      (println "Skipped scenarios (cache not yet available):" skipped)))
  result)
