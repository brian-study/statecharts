(ns com.fulcrologic.statecharts.jobs.test-helpers
  "Shared test infrastructure for worker integration tests.

   Provides fixtures, job creation, row inspection, handler factories, and
   tracker utilities used by worker-spec, worker-benchmark-test, and
   worker-edge-case-test."
  (:require
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.persistence.pg.job-store :as job-store]
   [com.fulcrologic.statecharts.persistence.pg.schema :as schema]
   [com.fulcrologic.statecharts.protocols :as sp]
   [pg.core :as pg]
   [pg.pool :as pool])
  (:import
   [java.util UUID]))

;; -----------------------------------------------------------------------------
;; Test Configuration / Fixtures
;; -----------------------------------------------------------------------------

(def test-config
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

;; -----------------------------------------------------------------------------
;; Constants
;; -----------------------------------------------------------------------------

(def terminal-statuses
  #{"succeeded" "failed" "cancelled"})

;; -----------------------------------------------------------------------------
;; Helpers
;; -----------------------------------------------------------------------------

(defn noop-update-data-by-id-fn
  [_session-id _data-updates]
  nil)

(defn make-tracking-event-queue
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

(defn create-n-jobs!
  "Insert N jobs and return vector of job UUIDs. Payload includes :job-index by default."
  ([pool n]
   (create-n-jobs! pool n {}))
  ([pool n {:keys [job-type max-attempts payload-fn session-prefix]
            :or {job-type "test-job"
                 max-attempts 1
                 session-prefix "test"
                 payload-fn (fn [idx] {:job-index idx})}}]
   (let [run-id (subs (str (random-uuid)) 0 8)]
     (mapv
      (fn [idx]
        (let [job-id (UUID/randomUUID)]
          (job-store/create-job! pool
            {:id job-id
             :session-id (keyword (str session-prefix "-session-" run-id "-" idx))
             :invokeid (keyword (str session-prefix "-invoke-" run-id "-" idx))
             :job-type job-type
             :payload (payload-fn idx)
             :max-attempts max-attempts})
          job-id))
      (range n)))))

(defn get-job-row
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

(defn get-job-rows [pool job-ids]
  (mapv #(get-job-row pool %) job-ids))

(defn wait-for-terminal-rows
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

;; -----------------------------------------------------------------------------
;; Handler Tracker
;; -----------------------------------------------------------------------------

(defn new-handler-tracker []
  (atom {:in-flight 0
         :max-in-flight 0
         :started-at-ms {}
         :finished-at-ms {}
         :handler-latencies-ms {}
         :calls []
         :continue-results []}))

(defn mark-handler-start! [tracker job-id]
  (when tracker
    (let [now-ms (System/currentTimeMillis)]
      (swap! tracker
        (fn [{:keys [in-flight] :as state}]
          (let [running (inc (long in-flight))]
            (-> state
                (assoc :in-flight running)
                (assoc-in [:started-at-ms job-id] now-ms)
                (update :calls conj job-id)
                (update :max-in-flight max running))))))))

(defn mark-handler-stop! [tracker job-id]
  (when tracker
    (let [now-ms (System/currentTimeMillis)]
      (swap! tracker
        (fn [{:keys [in-flight] :as state}]
          (let [start-ms (get-in state [:started-at-ms job-id] now-ms)]
            (-> state
                (assoc :in-flight (max 0 (dec (long in-flight))))
                (assoc-in [:finished-at-ms job-id] now-ms)
                (assoc-in [:handler-latencies-ms job-id] (- now-ms start-ms)))))))))

;; -----------------------------------------------------------------------------
;; Handler Factories
;; -----------------------------------------------------------------------------

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
