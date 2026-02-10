(ns com.fulcrologic.statecharts.persistence.pg
  "PostgreSQL persistence layer for statecharts.

   Provides durable storage for:
   - Working memory (session state)
   - Event queue (with exactly-once delivery)
   - Statechart registry (chart definitions)

   Usage:
   ```clojure
   (require '[com.fulcrologic.statecharts.persistence.pg :as pg-sc])
   (require '[pg.pool :as pool])

   ;; Create pg2 connection pool
   (def pool (pool/pool {:host \"localhost\"
                         :port 5432
                         :database \"statecharts\"
                         :user \"user\"
                         :password \"pass\"}))

   ;; Create tables (safe to call multiple times)
   (pg-sc/create-tables! pool)

   ;; Create environment
   (def env (pg-sc/pg-env {:pool pool}))

   ;; Use like any other statechart env
   (pg-sc/register! env ::my-chart my-chart)
   (pg-sc/start! env ::my-chart \"session-1\")

   ;; Start event loop (returns map with :stop! and :wake! fns)
   (def event-loop (pg-sc/start-event-loop! env 100))

   ;; ... later, to stop
   ((:stop! event-loop))
   ```"
  (:require
   [clojure.string :as str]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.algorithms.v20150901 :as alg]
   [com.fulcrologic.statecharts.algorithms.v20150901-validation :as v]
   [com.fulcrologic.statecharts.data-model.working-memory-data-model :as wmdm]
   [com.fulcrologic.statecharts.event-queue.event-processing :as ep]
   [com.fulcrologic.statecharts.execution-model.lambda :as lambda]
   [com.fulcrologic.statecharts.invocation.future :as i.future]
   [com.fulcrologic.statecharts.invocation.statechart :as i.statechart]
   [com.fulcrologic.statecharts.persistence.pg.event-queue :as pg-eq]
   [com.fulcrologic.statecharts.persistence.pg.schema :as schema]
   [com.fulcrologic.statecharts.persistence.pg.working-memory-store :as pg-wms]
   [com.fulcrologic.statecharts.protocols :as sp]
   [com.fulcrologic.statecharts.registry.local-memory-registry :as mem-reg]
   [com.fulcrologic.statecharts.util :refer [new-uuid]]
   [taoensso.timbre :as log]))

;; -----------------------------------------------------------------------------
;; Schema Management
;; -----------------------------------------------------------------------------

(defn create-tables!
  "Create all required database tables.
   Safe to call multiple times (uses IF NOT EXISTS)."
  [pool]
  (schema/create-tables! pool))

(defn drop-tables!
  "Drop all database tables.
   WARNING: This will delete all data!"
  [pool]
  (schema/drop-tables! pool))

;; -----------------------------------------------------------------------------
;; Environment Creation
;; -----------------------------------------------------------------------------

(defn pg-env
  "Create a statechart environment backed by PostgreSQL.

   Options:
   - :pool - pg2 connection pool (REQUIRED)
   - :node-id - Unique identifier for this worker node (optional, auto-generated if not provided)
   - :data-model - Custom data model (optional, defaults to flat working memory model)
   - :execution-model - Custom execution model (optional, defaults to lambda model)
   - :invocation-processors - Custom invocation processors (optional)

   Returns an ::sc/env map ready for use with statechart processing.

   Example:
   ```clojure
   (def env (pg-env {:pool my-pool}))
   (register! env ::my-chart my-chart)
   (start! env ::my-chart \"session-1\")
   ```"
  [{:keys [pool node-id data-model execution-model invocation-processors]
    :or {node-id (str (random-uuid))}}]
  (assert pool "A pg2 connection pool is required")
  (let [dm (or data-model (wmdm/new-flat-model))
        q (pg-eq/new-queue pool node-id)
        ex (or execution-model (lambda/new-execution-model dm q))
        ;; Use in-memory registry - chart definitions are code, not data.
        ;; Only working memory and events need database persistence.
        registry (mem-reg/new-registry)
        wmstore (pg-wms/new-store pool)
        inv-processors (or invocation-processors
                           [(i.statechart/new-invocation-processor)
                            (i.future/new-future-processor)])]
    {::sc/statechart-registry registry
     ::sc/data-model dm
     ::sc/event-queue q
     ::sc/working-memory-store wmstore
     ::sc/processor (alg/new-processor)
     ::sc/invocation-processors inv-processors
     ::sc/execution-model ex
     ;; Store pool and node-id for maintenance functions
     ::pool pool
     ::node-id node-id}))

;; -----------------------------------------------------------------------------
;; Convenience Functions
;; -----------------------------------------------------------------------------

(defn register!
  "Register a statechart `chart` at `chart-key` in the registry.
   Validates the chart and throws on errors, logs warnings."
  [{::sc/keys [statechart-registry]} chart-key chart]
  (let [problems (v/problems chart)
        errors? (boolean (some #(= :error (:level %)) problems))
        warnings? (boolean (some #(= :warn (:level %)) problems))]
    (cond
      errors? (throw (ex-info "Cannot register invalid chart"
                              {:chart-key chart-key
                               :problems (vec problems)}))
      warnings? (log/warn "Chart" chart-key "has problems:"
                          (str/join "," (map (fn [{:keys [element message]}]
                                               (str element ": " message))
                                             problems))))
    (sp/register-statechart! statechart-registry chart-key chart))
  true)

(defn start!
  "Start a statechart that has been previously registered.

   Returns true on success, throws on problems.

   Note: This persists the initial working memory to the database.
   Events should be sent via the event queue."
  ([env chart-src]
   (start! env chart-src (new-uuid)))
  ([{::sc/keys [processor working-memory-store statechart-registry] :as env}
    chart-src session-id]
   (assert statechart-registry "There must be a statechart registry in env")
   (assert working-memory-store "There must be a working memory store in env")
   (assert (sp/get-statechart statechart-registry chart-src)
           (str "A chart must be registered under " chart-src))
   (let [s0 (sp/start! processor env chart-src {::sc/session-id session-id})]
     (sp/save-working-memory! working-memory-store env session-id s0)
     (log/info "Statechart session started"
               {:session-id session-id
                :statechart-src chart-src
                :initial-configuration (::sc/configuration s0)}))
   true))

(defn send!
  "Send an event to the event queue.

   event-or-request can be:
   - A keyword (event name) with optional target
   - A map with :event, :data, :target, :delay, etc.

   If an event loop is running on this env (via start-event-loop!), and the event
   is not delayed, the loop will be woken up immediately to process it."
  [{::sc/keys [event-queue] ::keys [wake-signal] :as env} event-or-request]
  (let [result (sp/send! event-queue env event-or-request)
        ;; Check if this is an immediate event (no delay)
        delayed? (and (map? event-or-request)
                      (some? (:delay event-or-request))
                      (pos? (:delay event-or-request)))]
    ;; Wake up the event loop if present and event is immediate
    (when (and wake-signal (not delayed?))
      (.offer ^java.util.concurrent.BlockingQueue wake-signal :wake))
    result))

;; -----------------------------------------------------------------------------
;; Event Loop
;; -----------------------------------------------------------------------------

(defn start-event-loop!
  "Start a background event processing loop.

   poll-interval-ms - How often to check for events when idle (default 100ms)
   options - Map with:
     :session-id - Only process events for this session (optional)
     :batch-size - How many events to claim per poll (default 10)

   The loop will wake up immediately when send! is called on this env for
   non-delayed events, so there's no latency for events sent by this instance.

   Returns a map with:
   - :stop! - Function that stops the loop when called
   - :wake! - Function that wakes the loop to process events immediately

   Example:
   ```clojure
   (def event-loop (start-event-loop! env 100))
   ;; ... later
   ((:stop! event-loop))
   ```"
  ([env] (start-event-loop! env 100))
  ([env poll-interval-ms] (start-event-loop! env poll-interval-ms {}))
  ([{::sc/keys [event-queue] ::keys [node-id] :as env} poll-interval-ms options]
   (let [running (atom true)
         ;; Signal queue for immediate wake-up
         wake-signal (java.util.concurrent.LinkedBlockingQueue.)
         ;; Add wake signal to env so send! can use it
         env-with-signal (assoc env ::wake-signal wake-signal)
         handler (fn [env event]
                   (ep/standard-statechart-event-handler env event))
         loop-fn (fn []
                   (log/info "Event loop started"
                             {:node-id node-id
                              :poll-interval-ms poll-interval-ms
                              :session-filter (:session-id options)})
                   (while @running
                     (try
                       (sp/receive-events! event-queue env-with-signal handler options)
                       (catch Exception e
                         (log/error e "Event loop error"
                                    {:node-id node-id})))
                     ;; Wait for wake signal OR timeout (poll interval)
                     ;; .poll returns nil on timeout, :wake on signal
                     (.poll wake-signal poll-interval-ms java.util.concurrent.TimeUnit/MILLISECONDS))
                   (log/info "Event loop stopped" {:node-id node-id}))]
     (future (loop-fn))
     ;; Return map with stop and wake functions
     {:stop! (fn []
               (log/debug "Event loop stop requested" {:node-id node-id})
               (reset! running false)
               (.offer wake-signal :stop))
      :wake! (fn []
               (.offer wake-signal :wake))})))

;; -----------------------------------------------------------------------------
;; Maintenance
;; -----------------------------------------------------------------------------

(defn recover-stale-claims!
  "Recover events that were claimed but never processed.
   Call this periodically if workers can crash during processing.

   timeout-seconds - How old a claim must be to recover (default 30)"
  ([env] (recover-stale-claims! env 30))
  ([env timeout-seconds]
   (pg-eq/recover-stale-claims! (::pool env) timeout-seconds)))

(defn purge-processed-events!
  "Delete old processed events to reclaim database space.
   Call this periodically in production.

   retention-days - How many days of events to keep (default 7)"
  ([env] (purge-processed-events! env 7))
  ([env retention-days]
   (pg-eq/purge-processed-events! (::pool env) retention-days)))

(defn queue-depth
  "Get the current number of unprocessed events.

   Options:
   - :session-id - Count only events for this session"
  ([env] (queue-depth env {}))
  ([env options]
   (pg-eq/queue-depth (::pool env) options)))

;; Note: With in-memory registry, these cache functions are no longer needed.
;; The in-memory registry has no cache layer - charts are stored directly in memory.
;; These functions are kept as no-ops for backwards compatibility.

(defn preload-registry-cache!
  "No-op with in-memory registry. Kept for backwards compatibility."
  [_env]
  nil)

(defn clear-registry-cache!
  "No-op with in-memory registry. Kept for backwards compatibility."
  [_env]
  nil)
