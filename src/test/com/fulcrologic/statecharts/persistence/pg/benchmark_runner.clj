(ns com.fulcrologic.statecharts.persistence.pg.benchmark-runner
  "CLI runner for the PG working-memory load benchmark.

   Modes:
   - baseline: run baseline scenarios in-process
   - all: run baseline + cached scenarios (cache scenarios skipped when unavailable)
   - worker: run a single worker process and optionally write EDN output
   - cluster: spawn N worker JVMs and aggregate results"
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [com.fulcrologic.statecharts.persistence.pg.benchmark :as bench]
   [taoensso.timbre :as log]))

;; -----------------------------------------------------------------------------
;; CLI Parsing
;; -----------------------------------------------------------------------------

(defn- parse-args
  [args]
  (loop [m {} remaining args]
    (if-let [arg (first remaining)]
      (if (str/starts-with? arg "--")
        (let [[k maybe-v] (str/split arg #"=" 2)
              key (keyword (subs k 2))]
          (if maybe-v
            (recur (assoc m key maybe-v) (rest remaining))
            (let [next-arg (second remaining)]
              (if (and next-arg (not (str/starts-with? next-arg "--")))
                (recur (assoc m key next-arg) (nnext remaining))
                (recur (assoc m key true) (rest remaining))))))
        (recur (update m :_positional (fnil conj []) arg) (rest remaining)))
      m)))

(defn- parse-long-opt
  [cli key default]
  (let [v (get cli key)]
    (if (nil? v)
      default
      (let [parsed (cond
                     (number? v) (long v)
                     (string? v) (parse-long v)
                     :else nil)]
        (if (nil? parsed) default parsed)))))

(defn- parse-str-opt
  [cli key default]
  (let [v (get cli key)]
    (if (string? v) v default)))

(defn- cli->workload
  [cli]
  (merge bench/default-workload
         {:sessions (parse-long-opt cli :sessions (:sessions bench/default-workload))
          :duration-sec (parse-long-opt cli :duration-sec (:duration-sec bench/default-workload))
          :read-pct (parse-long-opt cli :read-pct (:read-pct bench/default-workload))
          :write-pct (parse-long-opt cli :write-pct (:write-pct bench/default-workload))
          :delete-pct (parse-long-opt cli :delete-pct (:delete-pct bench/default-workload))
          :seed (parse-long-opt cli :seed (:seed bench/default-workload))}))

(defn- cli->pool-config
  [cli]
  (merge (bench/default-pool-config)
         (cond-> {}
           (get cli :host) (assoc :host (parse-str-opt cli :host nil))
           (get cli :port) (assoc :port (parse-long-opt cli :port nil))
           (get cli :database) (assoc :database (parse-str-opt cli :database nil))
           (get cli :user) (assoc :user (parse-str-opt cli :user nil))
           (contains? cli :password) (assoc :password (parse-str-opt cli :password nil)))))

(defn- common-run-opts
  [cli]
  {:workload (cli->workload cli)
   :pool-config (cli->pool-config cli)})

(defn- effective-cache-size
  "Phase 1 safety: cache store is not implemented yet.
   If caller requests cache-size > 0 but cache API is unavailable,
   force cache-size to 0 so benchmark still runs."
  [requested]
  (if (and (pos? requested) (not (bench/cache-supported?)))
    (do
      (println "Cache store is not available yet; forcing --cache-size 0 for this run.")
      0)
    requested))

;; -----------------------------------------------------------------------------
;; Output Helpers
;; -----------------------------------------------------------------------------

(defn- print-result!
  [result]
  (bench/print-summary! result)
  (println "\nEDN:")
  (pprint/pprint result)
  result)

(defn- ensure-parent-dir!
  [path]
  (let [f (io/file path)
        parent (.getParentFile f)]
    (when parent
      (.mkdirs parent))
    path))

;; -----------------------------------------------------------------------------
;; Worker Mode
;; -----------------------------------------------------------------------------

(defn- run-worker-mode!
  [cli]
  (let [name (parse-str-opt cli :name "worker")
        start-at (parse-long-opt cli :start-at-epoch-ms nil)
        worker-id (parse-long-opt cli :worker-id 0)
        threads (parse-long-opt cli :threads 8)
        cache-size (effective-cache-size (parse-long-opt cli :cache-size 0))
        output-file (parse-str-opt cli :output-file nil)
        run-opts (common-run-opts cli)
        run-opts (update-in run-opts [:workload :seed] + (* 100000 worker-id))]
    (when start-at
      (let [wait-ms (- start-at (System/currentTimeMillis))]
        (when (pos? wait-ms)
          (Thread/sleep wait-ms))))
    (let [result (bench/run-worker!
                  (merge run-opts
                         {:name name
                          :threads-per-instance threads
                          :cache-size cache-size
                          :prepared? true}))]
      (if output-file
        (do
          (ensure-parent-dir! output-file)
          (spit output-file (pr-str result))
          (println "Wrote worker result:" output-file)
          result)
        (print-result! {:results [result]})))))

;; -----------------------------------------------------------------------------
;; Cluster Mode (true multi-process benchmark)
;; -----------------------------------------------------------------------------

(defn- worker-cmd
  [{:keys [worker-id start-at-epoch-ms output-file name threads cache-size workload pool-config]}]
  (vec
   (concat
    ["clojure" "-M:test"
     "-m" "com.fulcrologic.statecharts.persistence.pg.benchmark-runner"
     "--mode" "worker"
     "--worker-id" (str worker-id)
     "--name" name
     "--threads" (str threads)
     "--cache-size" (str cache-size)
     "--start-at-epoch-ms" (str start-at-epoch-ms)
     "--output-file" output-file
     "--sessions" (str (:sessions workload))
     "--duration-sec" (str (:duration-sec workload))
     "--read-pct" (str (:read-pct workload))
     "--write-pct" (str (:write-pct workload))
     "--delete-pct" (str (:delete-pct workload))
     "--seed" (str (:seed workload))
     "--host" (:host pool-config)
     "--port" (str (:port pool-config))
     "--database" (:database pool-config)
     "--user" (:user pool-config)]
    (when-let [pw (:password pool-config)]
      ["--password" pw]))))

(defn- run-cluster-mode!
  [cli]
  (let [instances (parse-long-opt cli :instances 4)
        threads (parse-long-opt cli :threads 8)
        cache-size (effective-cache-size (parse-long-opt cli :cache-size 0))
        name (parse-str-opt cli :name "cluster")
        run-opts (common-run-opts cli)
        output-dir (io/file "target" "bench" (str "cluster-" (System/currentTimeMillis)))
        _ (.mkdirs output-dir)
        start-at-ms (+ (System/currentTimeMillis) 2000)]
    (println "Preparing benchmark DB before cluster run...")
    (bench/prepare-db! run-opts)
    (println (format "Launching %d worker JVMs (threads=%d cache=%d)..." instances threads cache-size))
    (let [workers
          (mapv
           (fn [worker-id]
             (let [output-file (.getPath (io/file output-dir (format "worker-%02d.edn" worker-id)))
                   cmd (worker-cmd {:worker-id worker-id
                                    :start-at-epoch-ms start-at-ms
                                    :output-file output-file
                                    :name name
                                    :threads threads
                                    :cache-size cache-size
                                    :workload (:workload run-opts)
                                    :pool-config (:pool-config run-opts)})
                   process (-> (ProcessBuilder. cmd)
                               (.directory (io/file "."))
                               (.inheritIO)
                               (.start))]
               {:worker-id worker-id
                :process process
                :output-file output-file
                :cmd cmd}))
           (range instances))]
      (doseq [{:keys [worker-id process]} workers]
        (let [exit-code (.waitFor ^Process process)]
          (when-not (zero? exit-code)
            (throw (ex-info "Worker process failed"
                            {:worker-id worker-id
                             :exit-code exit-code})))))
      (let [worker-results
            (mapv (fn [{:keys [output-file]}]
                    (edn/read-string (slurp output-file)))
                  workers)
            merged (bench/merge-worker-results name worker-results)
            result {:type :cluster
                    :instances instances
                    :threads-per-instance threads
                    :cache-size cache-size
                    :worker-results worker-results
                    :result merged
                    :output-dir (.getPath output-dir)}]
        (println "\nCluster Summary:")
        (println (format "%-20s ops=%9d throughput=%10.1f/s errors=%4d optimistic=%4d"
                         (:scenario merged)
                         (:total-ops merged)
                         (:throughput merged)
                         (:errors merged)
                         (:optimistic-lock-failures merged)))
        (println "\nEDN:")
        (pprint/pprint result)
        result))))

;; -----------------------------------------------------------------------------
;; Main
;; -----------------------------------------------------------------------------

(defn -main
  [& args]
  ;; Benchmarks should measure persistence/caching behavior, not log throughput.
  (log/set-min-level! :error)
  (try
    (let [cli (parse-args args)
          mode (keyword (parse-str-opt cli :mode "all"))]
      (case mode
        :baseline
        (-> (bench/run-baseline! (common-run-opts cli))
            (print-result!))

        :all
        (-> (bench/run-all! (common-run-opts cli))
            (print-result!))

        :worker
        (run-worker-mode! cli)

        :cluster
        (run-cluster-mode! cli)

        (throw (ex-info "Unknown benchmark runner mode"
                        {:mode mode
                         :supported [:baseline :all :worker :cluster]}))))
    (shutdown-agents)
    (System/exit 0)
    (catch Throwable t
      (binding [*out* *err*]
        (.printStackTrace t))
      (shutdown-agents)
      (System/exit 1))))
