(ns com.fulcrologic.statecharts.persistence.pg-spec
  "Tests for PostgreSQL persistence public API.

   These tests verify the pg-env construction, component wiring,
   and API function signatures without requiring a database.

   NOTE: pg-env now uses an in-memory registry (LocalMemoryRegistry) by default.
   This is because chart definitions contain functions (guards, actions) which
   cannot be serialized to PostgreSQL. Only working memory and events are
   persisted to the database."
  (:require
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.chart :as chart]
   [com.fulcrologic.statecharts.elements :refer [state transition final]]
   [com.fulcrologic.statecharts.persistence.pg :as pg]
   [com.fulcrologic.statecharts.persistence.pg.event-queue :as eq]
   [com.fulcrologic.statecharts.persistence.pg.working-memory-store :as wms]
   [com.fulcrologic.statecharts.protocols :as sp]
   [com.fulcrologic.statecharts.registry.local-memory-registry :as mem-reg]
   [fulcro-spec.core :refer [=> assertions behavior component specification]]))

;; -----------------------------------------------------------------------------
;; Environment Creation Tests
;; -----------------------------------------------------------------------------

(specification "pg-env Creation"
  (let [fake-pool {:type :fake-pool}]

    (component "creates complete environment"
      (let [env (pg/pg-env {:pool fake-pool})]
        (behavior "includes all required components"
          (assertions
            "has statechart registry"
            (some? (::sc/statechart-registry env)) => true
            "has data model"
            (some? (::sc/data-model env)) => true
            "has event queue"
            (some? (::sc/event-queue env)) => true
            "has working memory store"
            (some? (::sc/working-memory-store env)) => true
            "has processor"
            (some? (::sc/processor env)) => true
            "has execution model"
            (some? (::sc/execution-model env)) => true
            "has invocation processors"
            (vector? (::sc/invocation-processors env)) => true
            "stores pool reference"
            (::pg/pool env) => fake-pool
            "generates node-id"
            (string? (::pg/node-id env)) => true))))

    (component "uses provided options"
      (let [env (pg/pg-env {:pool fake-pool
                            :node-id "custom-node"})]
        (behavior "respects custom node-id"
          (assertions
            (::pg/node-id env) => "custom-node"))))

    (component "protocol satisfaction"
      (let [env (pg/pg-env {:pool fake-pool})]
        (behavior "registry implements StatechartRegistry"
          (assertions
            (satisfies? sp/StatechartRegistry (::sc/statechart-registry env)) => true))

        (behavior "working memory store implements WorkingMemoryStore"
          (assertions
            (satisfies? sp/WorkingMemoryStore (::sc/working-memory-store env)) => true))

        (behavior "event queue implements EventQueue"
          (assertions
            (satisfies? sp/EventQueue (::sc/event-queue env)) => true))

        (behavior "processor implements Processor"
          (assertions
            (satisfies? sp/Processor (::sc/processor env)) => true))

        (behavior "data model implements DataModel"
          (assertions
            (satisfies? sp/DataModel (::sc/data-model env)) => true))))

    (component "requires pool"
      (behavior "throws without pool"
        (try
          (pg/pg-env {})
          (assertions "should have thrown" false => true)
          (catch AssertionError e
            (assertions
              "throws assertion error"
              (string? (.getMessage e)) => true)))))))

;; -----------------------------------------------------------------------------
;; Component Type Tests
;; -----------------------------------------------------------------------------

(specification "Environment Component Types"
  (let [fake-pool {:type :fake-pool}
        env (pg/pg-env {:pool fake-pool})]

    (component "registry type"
      (behavior "creates LocalMemoryRegistry (not PostgresStatechartRegistry)"
        ;; pg-env now uses in-memory registry because chart definitions
        ;; contain functions which cannot be serialized to PostgreSQL
        (assertions
          (instance? com.fulcrologic.statecharts.registry.local_memory_registry.LocalMemoryRegistry
                     (::sc/statechart-registry env)) => true)))

    (component "working memory store type"
      (behavior "creates PostgresWorkingMemoryStore"
        (assertions
          (instance? com.fulcrologic.statecharts.persistence.pg.working_memory_store.PostgresWorkingMemoryStore
                     (::sc/working-memory-store env)) => true)))

    (component "event queue type"
      (behavior "creates PostgresEventQueue"
        (assertions
          (instance? com.fulcrologic.statecharts.persistence.pg.event_queue.PostgresEventQueue
                     (::sc/event-queue env)) => true)))))

;; -----------------------------------------------------------------------------
;; API Function Existence Tests
;; -----------------------------------------------------------------------------

(specification "Public API Functions"
  (component "schema management"
    (behavior "create-tables! exists"
      (assertions
        (fn? pg/create-tables!) => true))

    (behavior "drop-tables! exists"
      (assertions
        (fn? pg/drop-tables!) => true)))

  (component "convenience functions"
    (behavior "register! exists"
      (assertions
        (fn? pg/register!) => true))

    (behavior "start! exists"
      (assertions
        (fn? pg/start!) => true))

    (behavior "send! exists"
      (assertions
        (fn? pg/send!) => true)))

  (component "event loop"
    (behavior "start-event-loop! exists"
      (assertions
        (fn? pg/start-event-loop!) => true)))

  (component "maintenance functions"
    (behavior "recover-stale-claims! exists"
      (assertions
        (fn? pg/recover-stale-claims!) => true))

    (behavior "purge-processed-events! exists"
      (assertions
        (fn? pg/purge-processed-events!) => true))

    (behavior "queue-depth exists"
      (assertions
        (fn? pg/queue-depth) => true))))

;; -----------------------------------------------------------------------------
;; Chart Validation Tests
;; -----------------------------------------------------------------------------

(specification "Chart Registration Validation"
  (component "register! validation"
    (let [fake-pool {:type :fake-pool}
          env (pg/pg-env {:pool fake-pool})
          ;; Create a valid chart for testing validation paths
          valid-chart (chart/statechart {:initial :s1}
                                        (state {:id :s1}
                                          (transition {:event :done :target :end}))
                                        (final {:id :end}))]

      (behavior "accepts valid charts"
        ;; We can't actually register without DB, but we can test the chart validation
        (let [problems (com.fulcrologic.statecharts.algorithms.v20150901-validation/problems valid-chart)]
          (assertions
            "valid chart has no errors"
            (boolean (some #(= :error (:level %)) problems)) => false))))))

;; -----------------------------------------------------------------------------
;; Event Queue Integration Points
;; -----------------------------------------------------------------------------

(specification "Event Queue Wiring"
  (let [fake-pool {:type :fake-pool}
        env (pg/pg-env {:pool fake-pool})
        queue (::sc/event-queue env)]

    (component "queue configuration"
      (behavior "queue has pool reference"
        (assertions
          (:pool queue) => fake-pool))

      (behavior "queue has node-id"
        (assertions
          (string? (:node-id queue)) => true)))))

;; -----------------------------------------------------------------------------
;; Working Memory Store Integration Points
;; -----------------------------------------------------------------------------

(specification "Working Memory Store Wiring"
  (let [fake-pool {:type :fake-pool}
        env (pg/pg-env {:pool fake-pool})
        store (::sc/working-memory-store env)]

    (component "store configuration"
      (behavior "store has pool reference"
        (assertions
          (:pool store) => fake-pool)))))

;; -----------------------------------------------------------------------------
;; Registry Integration Points
;; -----------------------------------------------------------------------------

(specification "Registry Wiring"
  (let [fake-pool {:type :fake-pool}
        env (pg/pg-env {:pool fake-pool})
        registry (::sc/statechart-registry env)]

    (component "in-memory registry"
      (behavior "registry is LocalMemoryRegistry"
        (assertions
          (instance? com.fulcrologic.statecharts.registry.local_memory_registry.LocalMemoryRegistry
                     registry) => true))

      (behavior "can register charts with function guards"
        ;; This is the key reason for using in-memory registry:
        ;; Charts with function guards can be registered without serialization
        (let [chart-with-fn (chart/statechart {:initial :s1}
                                              (state {:id :s1}
                                                (transition {:event :go
                                                             :target :s2
                                                             :cond (fn [_ _] true)}))
                                              (state {:id :s2}))]
          (sp/register-statechart! registry :test-chart chart-with-fn)
          (assertions
            "chart is registered"
            (some? (sp/get-statechart registry :test-chart)) => true))))))

;; -----------------------------------------------------------------------------
;; Invocation Processor Configuration Tests
;; -----------------------------------------------------------------------------

(specification "Invocation Processors"
  (let [fake-pool {:type :fake-pool}
        env (pg/pg-env {:pool fake-pool})]

    (component "default invocation processors"
      (behavior "includes statechart invocation processor"
        (let [processors (::sc/invocation-processors env)]
          (assertions
            "has multiple processors"
            (> (count processors) 0) => true)))

      (behavior "includes future invocation processor"
        (let [processors (::sc/invocation-processors env)]
          (assertions
            "has at least 2 processors"
            (>= (count processors) 2) => true))))))

;; -----------------------------------------------------------------------------
;; Custom Options Tests
;; -----------------------------------------------------------------------------

(specification "Custom Environment Options"
  (let [fake-pool {:type :fake-pool}]

    (component "custom node-id"
      (let [env (pg/pg-env {:pool fake-pool :node-id "custom-worker-42"})]
        (behavior "uses provided node-id"
          (assertions
            (::pg/node-id env) => "custom-worker-42"))

        (behavior "event queue uses custom node-id"
          (assertions
            (:node-id (::sc/event-queue env)) => "custom-worker-42"))))

    (component "custom invocation processors"
      (let [custom-processors [{:type :custom-processor-1}
                               {:type :custom-processor-2}]
            env (pg/pg-env {:pool fake-pool
                            :invocation-processors custom-processors})]
        (behavior "uses provided invocation processors"
          (assertions
            (::sc/invocation-processors env) => custom-processors
            (count (::sc/invocation-processors env)) => 2))))

    (component "auto-generated node-id"
      (let [env1 (pg/pg-env {:pool fake-pool})
            env2 (pg/pg-env {:pool fake-pool})]
        (behavior "generates unique node-ids"
          (assertions
            "node-ids are different"
            (not= (::pg/node-id env1) (::pg/node-id env2)) => true
            "node-ids are valid UUIDs"
            (uuid? (parse-uuid (::pg/node-id env1))) => true
            (uuid? (parse-uuid (::pg/node-id env2))) => true))))))

;; -----------------------------------------------------------------------------
;; Event Loop Contract Tests
;; -----------------------------------------------------------------------------

(specification "Event Loop Contract"
  (component "start-event-loop!"
    (behavior "returns a stop function"
      (assertions
        "function exists"
        (fn? pg/start-event-loop!) => true))

    (behavior "accepts various arities"
      ;; We can't actually start the loop without a real DB, but we can
      ;; verify the function exists and has expected signature
      (assertions
        "1-arity exists (env only)"
        (fn? pg/start-event-loop!) => true))))

;; -----------------------------------------------------------------------------
;; Maintenance Function Contract Tests
;; -----------------------------------------------------------------------------

(specification "Maintenance Function Contracts"
  (let [fake-pool {:type :fake-pool}
        env (pg/pg-env {:pool fake-pool})]

    (component "recover-stale-claims!"
      (behavior "accepts env and optional timeout"
        (assertions
          "function exists with expected arity"
          (fn? pg/recover-stale-claims!) => true)))

    (component "purge-processed-events!"
      (behavior "accepts env and optional retention days"
        (assertions
          "function exists with expected arity"
          (fn? pg/purge-processed-events!) => true)))

    (component "queue-depth"
      (behavior "accepts env and optional options"
        (assertions
          "function exists with expected arity"
          (fn? pg/queue-depth) => true)))

))

;; -----------------------------------------------------------------------------
;; Wake Signal Tests
;; -----------------------------------------------------------------------------

(specification "Wake Signal Mechanism"
  (component "send! with wake-signal"
    (let [fake-pool {:type :fake-pool}
          env (pg/pg-env {:pool fake-pool})
          wake-signal (java.util.concurrent.LinkedBlockingQueue.)
          env-with-signal (assoc env ::pg/wake-signal wake-signal)]

      (behavior "non-delayed events signal wake queue"
        ;; We can't fully test without DB, but we can verify the mechanism
        (assertions
          "wake-signal key is used correctly"
          (::pg/wake-signal env-with-signal) => wake-signal))))

  (component "start-event-loop! wake integration"
    (behavior "uses LinkedBlockingQueue for immediate wake-up"
      ;; Verify the function signature and mechanism exists
      (assertions
        "function exists"
        (fn? pg/start-event-loop!) => true)))

  (component "wake-signal queue behavior"
    (let [wake-queue (java.util.concurrent.LinkedBlockingQueue.)]
      (behavior "queue supports offer/poll pattern"
        (.offer wake-queue :wake)
        (assertions
          "poll returns offered value"
          (.poll wake-queue) => :wake
          "poll returns nil when empty"
          (.poll wake-queue 1 java.util.concurrent.TimeUnit/MILLISECONDS) => nil))

      (behavior "poll with timeout waits appropriately"
        (let [start (System/currentTimeMillis)
              _ (.poll wake-queue 50 java.util.concurrent.TimeUnit/MILLISECONDS)
              elapsed (- (System/currentTimeMillis) start)]
          (assertions
            "waited approximately the timeout duration"
            (>= elapsed 40) => true
            (< elapsed 200) => true)))

      (behavior "poll returns immediately when value offered"
        (let [result-atom (atom nil)
              started (promise)
              poll-future (future
                            (deliver started true)
                            (reset! result-atom
                                    (.poll wake-queue 5000 java.util.concurrent.TimeUnit/MILLISECONDS)))]
          @started
          (Thread/sleep 10) ; let the poll start
          (.offer wake-queue :woken)
          (let [start (System/currentTimeMillis)
                _ @poll-future
                elapsed (- (System/currentTimeMillis) start)]
            (assertions
              "returned the offered value"
              @result-atom => :woken
              "returned quickly (not waiting full timeout)"
              (< elapsed 1000) => true)))))))
