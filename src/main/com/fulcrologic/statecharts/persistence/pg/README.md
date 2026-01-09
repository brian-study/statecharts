# PostgreSQL Persistence Layer for Statecharts

This module provides durable PostgreSQL-backed storage for statecharts, implementing all three core storage protocols:

- **WorkingMemoryStore** - Persist session state across restarts
- **EventQueue** - Durable event queue with exactly-once delivery
- **StatechartRegistry** - Store chart definitions in the database

## Quick Start

```clojure
(ns myapp.core
  (:require
   [com.fulcrologic.statecharts.chart :as chart]
   [com.fulcrologic.statecharts.elements :refer [state transition on-entry assign script]]
   [com.fulcrologic.statecharts.data-model.operations :as ops]
   [com.fulcrologic.statecharts.persistence.pg :as pg-sc]
   [pg.pool :as pool]))

;; 1. Create connection pool
(def pool
  (pool/pool {:host "localhost"
              :port 5432
              :database "myapp"
              :user "postgres"
              :password "postgres"}))

;; 2. Create tables (idempotent)
(pg-sc/create-tables! pool)

;; 3. Create environment
(def env (pg-sc/pg-env {:pool pool}))

;; 4. Define and register a chart
(def order-chart
  (chart/statechart {:initial :pending}
    (state {:id :pending}
      (transition {:event :pay :target :paid}))
    (state {:id :paid}
      (transition {:event :ship :target :shipped}))
    (state {:id :shipped})))

(pg-sc/register! env :order/process order-chart)

;; 5. Start a session
(pg-sc/start! env :order/process :order-123)

;; 6. Send events
(pg-sc/send! env {:event :pay :target :order-123})

;; 7. Process events (or use start-event-loop!)
(def stop! (pg-sc/start-event-loop! env 100))

;; Later: stop the loop
(stop!)
```

## Working with Session Data

Statecharts can store local data that persists with the session. There are two styles:

### Declarative Style (Recommended)

Return operations from your expressions - they get processed automatically:

```clojure
(require '[com.fulcrologic.statecharts.data-model.operations :as ops])

(def cart-chart
  (chart/statechart {:initial :empty}

    ;; Initialize data on entry
    (state {:id :empty}
      (on-entry {}
        (assign {:location [:cart]
                 :expr (constantly {:items [] :total 0})}))
      (transition {:event :add-item :target :has-items}))

    ;; Update data by returning operations
    (state {:id :has-items}
      (transition {:event :add-item :target :has-items}
        (script {:expr (fn [env data]
                         (let [item (get-in data [:_event :data :item])
                               price (get-in data [:_event :data :price])]
                           ;; Return a vector of operations
                           [(ops/assign [:cart :items]
                                        (conj (:items (:cart data)) item))
                            (ops/assign [:cart :total]
                                        (+ (:total (:cart data)) price))]))}))

      (transition {:event :clear :target :empty}
        (script {:expr (fn [env data]
                         ;; Delete data
                         [(ops/delete [:cart])])}))

      (transition {:event :checkout :target :checking-out}))

    (state {:id :checking-out})))
```

### Available Operations

```clojure
;; Assign a value to a path
(ops/assign [:key] value)
(ops/assign [:nested :path] value)

;; Assign multiple values at once
(ops/assign
  [:counter] 0
  [:user :name] "Jo"
  [:items] [])

;; Delete keys/paths
(ops/delete [:temp-key])
(ops/delete [:key1] [:key2] [:nested :path])
```

### Imperative Style

For side effects within expressions, use the environment functions:

```clojure
(require '[com.fulcrologic.statecharts.environment :as env])

(script {:expr (fn [e data]
                 ;; Side-effect assignment
                 (env/assign! e [:counter] 0)
                 (env/assign! e [:user :name] "Jo")

                 ;; Side-effect deletion
                 (env/delete! e [:temp-data])

                 ;; No return value needed
                 nil)})
```

### Reading Data

In expressions, data is passed as the second argument:

```clojure
(script {:expr (fn [env data]
                 ;; Access your data directly
                 (let [counter (:counter data)
                       user-name (get-in data [:user :name])
                       cart-items (get-in data [:cart :items])]

                   ;; Access event data
                   (let [event-name (get-in data [:_event :name])
                         event-data (get-in data [:_event :data])]
                     ...)))})

;; In conditions
(transition {:event :checkout
             :cond (fn [env data]
                     (pos? (get-in data [:cart :total])))
             :target :payment})
```

### Data Storage

All session data is stored in the `::sc/data-model` key of working memory:

```clojure
;; Working memory structure (persisted to PostgreSQL)
{::sc/session-id :order-123
 ::sc/statechart-src :order/process
 ::sc/configuration #{:has-items}
 ::sc/data-model {:cart {:items ["Widget" "Gadget"]
                         :total 79.98}
                  :user {:name "Jo"}}}
```

## Sending Events with Data

```clojure
;; Simple event
(pg-sc/send! env {:event :next
                  :target :session-123})

;; Event with data
(pg-sc/send! env {:event :add-item
                  :target :cart-session
                  :data {:item "Widget"
                         :price 29.99}})

;; Delayed event (5 seconds)
(pg-sc/send! env {:event :reminder
                  :target :session-123
                  :delay 5000})

;; Cancelable delayed event
(pg-sc/send! env {:event :timeout
                  :target :session-123
                  :source-session-id :session-123
                  :send-id "my-timeout"
                  :delay 30000})

;; Cancel a pending delayed event
(let [event-queue (::sc/event-queue env)]
  (sp/cancel! event-queue env :session-123 "my-timeout"))
```

## Convenience Helpers

The library includes shorthand functions in `com.fulcrologic.statecharts.convenience`:

```clojure
(require '[com.fulcrologic.statecharts.convenience :refer [on assign-on choice]])

(state {:id :counter}
  ;; Shorthand for transition + assign
  (assign-on :increment [:count]
             (fn [env data] (inc (:count data))))

  ;; Shorthand for transition
  (on :reset :initial)
  (on :done :final (script {:expr log-completion})))

;; Choice pseudo-state
(choice {:id :check-balance}
  (fn [env data] (> (:balance data) 100)) :rich
  (fn [env data] (> (:balance data) 0)) :ok
  :else :broke)
```

## Database Schema

The persistence layer creates three tables:

### statechart_sessions

Stores working memory for each session:

```sql
CREATE TABLE statechart_sessions (
    session_id       TEXT PRIMARY KEY,
    statechart_src   TEXT NOT NULL,
    working_memory   BYTEA NOT NULL,
    version          BIGINT NOT NULL DEFAULT 1,  -- optimistic locking
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### statechart_events

Durable event queue with exactly-once delivery:

```sql
CREATE TABLE statechart_events (
    id                  BIGSERIAL PRIMARY KEY,
    target_session_id   TEXT NOT NULL,
    source_session_id   TEXT,
    send_id             TEXT,          -- for cancellation
    invoke_id           TEXT,
    event_name          TEXT NOT NULL,
    event_type          TEXT DEFAULT 'external',
    event_data          BYTEA,
    deliver_at          TIMESTAMPTZ NOT NULL DEFAULT now(),  -- delayed events
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at          TIMESTAMPTZ,   -- exactly-once delivery
    claimed_by          TEXT,          -- worker node tracking
    processed_at        TIMESTAMPTZ
);
```

### statechart_definitions

Stores registered chart definitions:

```sql
CREATE TABLE statechart_definitions (
    src              TEXT PRIMARY KEY,
    definition       BYTEA NOT NULL,
    version          BIGINT NOT NULL DEFAULT 1,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Optimistic Locking

Sessions use optimistic locking to handle concurrent modifications safely:

```clojure
;; If two workers try to update the same session simultaneously,
;; one will succeed and one will get an optimistic lock failure

(require '[com.fulcrologic.statecharts.persistence.pg.working-memory-store :as wms])

;; Retry wrapper for handling conflicts
(wms/with-optimistic-retry
  {:max-retries 3
   :backoff-ms 100}
  (fn []
    ;; Your operation that might conflict
    (process-event! ...)))

;; Check if an exception is a lock failure
(try
  (sp/save-working-memory! store env session-id wmem)
  (catch Exception e
    (when (wms/optimistic-lock-failure? e)
      ;; Handle conflict - reload and retry
      )))
```

## Exactly-Once Event Delivery

The event queue uses PostgreSQL's `SELECT FOR UPDATE SKIP LOCKED` to ensure each event is processed exactly once, even with multiple workers:

```clojure
;; Multiple workers can safely poll the same queue
(def env1 (pg-sc/pg-env {:pool pool :node-id "worker-1"}))
(def env2 (pg-sc/pg-env {:pool pool :node-id "worker-2"}))

;; Start event loops on both - events are distributed automatically
(def stop1! (pg-sc/start-event-loop! env1 100))
(def stop2! (pg-sc/start-event-loop! env2 100))
```

## Immediate Event Processing

When using `start-event-loop!`, events sent via `send!` on the same env instance are processed immediately without waiting for the poll interval. This eliminates latency for locally-sent events:

```clojure
;; Start event loop with 100ms poll interval
(def stop! (pg-sc/start-event-loop! env 100))

;; This event is processed immediately (not in 100ms)
(pg-sc/send! env {:event :process :target :session-1})
```

**How it works:**
- The event loop uses a `LinkedBlockingQueue` for wake-up signals
- `send!` signals the queue for non-delayed events
- The loop uses `poll(timeout)` instead of `Thread/sleep`, so it wakes immediately on signal
- Delayed events (`:delay` > 0) don't trigger immediate processing

**Note:** Events sent from other instances (different JVMs) still rely on the poll interval. This optimization only applies to events sent via the same env that started the event loop.

## Maintenance Functions

```clojure
;; Get queue depth
(pg-sc/queue-depth env)
(pg-sc/queue-depth env {:session-id :specific-session})

;; Recover events from crashed workers (claimed but not processed)
(pg-sc/recover-stale-claims! env)        ; default 30 second timeout
(pg-sc/recover-stale-claims! env 60)     ; 60 second timeout

;; Clean up old processed events
(pg-sc/purge-processed-events! env)      ; default 7 days retention
(pg-sc/purge-processed-events! env 30)   ; 30 days retention

;; Registry cache management
(pg-sc/preload-registry-cache! env)      ; load all charts into memory
(pg-sc/clear-registry-cache! env)        ; clear cache (after external DB changes)
```

## Environment Options

```clojure
(pg-sc/pg-env
  {:pool pool                              ; Required: pg2 connection pool
   :node-id "worker-1"                     ; Optional: unique worker ID (auto-generated if omitted)
   :data-model my-custom-data-model        ; Optional: custom DataModel implementation
   :execution-model my-execution-model     ; Optional: custom ExecutionModel
   :invocation-processors [...]})          ; Optional: custom invocation processors
```

## Complete Example: Order Processing

```clojure
(ns myapp.orders
  (:require
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.chart :as chart]
   [com.fulcrologic.statecharts.elements :refer [state transition on-entry assign script final]]
   [com.fulcrologic.statecharts.data-model.operations :as ops]
   [com.fulcrologic.statecharts.persistence.pg :as pg-sc]
   [pg.pool :as pool]))

(def order-chart
  (chart/statechart {:initial :pending}

    (state {:id :pending}
      (on-entry {}
        (assign {:location [:order]
                 :expr (fn [env data]
                         ;; Initialize from invocation data
                         (get-in data [:_event :data]))}))
      (transition {:event :submit :target :validating}))

    (state {:id :validating}
      (on-entry {}
        (script {:expr (fn [env data]
                         (let [order (:order data)
                               valid? (and (:items order)
                                           (seq (:items order))
                                           (:customer-id order))]
                           [(ops/assign [:validation-result]
                                        (if valid? :valid :invalid))]))}))
      (transition {:event :validated
                   :cond (fn [_ data] (= :valid (:validation-result data)))
                   :target :awaiting-payment})
      (transition {:event :validated
                   :cond (fn [_ data] (= :invalid (:validation-result data)))
                   :target :failed}))

    (state {:id :awaiting-payment}
      (on-entry {}
        (script {:expr (fn [env data]
                         (println "Waiting for payment on order"
                                  (get-in data [:order :id]))
                         nil)}))
      (transition {:event :payment-received :target :paid}
        (script {:expr (fn [env data]
                         [(ops/assign [:payment] (get-in data [:_event :data]))])}))
      (transition {:event :payment-failed :target :awaiting-payment})
      (transition {:event :cancel :target :cancelled}))

    (state {:id :paid}
      (transition {:event :ship :target :shipped}
        (script {:expr (fn [env data]
                         [(ops/assign [:shipment] (get-in data [:_event :data]))])})))

    (state {:id :shipped}
      (transition {:event :deliver :target :delivered}))

    (final {:id :delivered})
    (final {:id :cancelled})
    (final {:id :failed})))

;; Setup
(def pool (pool/pool {:host "localhost" :port 5432
                      :database "orders" :user "app" :password "secret"}))
(pg-sc/create-tables! pool)
(def env (pg-sc/pg-env {:pool pool :node-id "order-worker-1"}))
(pg-sc/register! env :orders/process order-chart)

;; Start event loop
(def stop! (pg-sc/start-event-loop! env 100))

;; Create an order
(defn create-order! [order-id customer-id items]
  (pg-sc/start! env :orders/process order-id)
  (pg-sc/send! env {:event :submit
                    :target order-id
                    :data {:id order-id
                           :customer-id customer-id
                           :items items}}))

;; Process payment
(defn record-payment! [order-id payment-info]
  (pg-sc/send! env {:event :payment-received
                    :target order-id
                    :data payment-info}))

;; Usage
(create-order! :order-001 :customer-42 [{:sku "WIDGET" :qty 2}])
(record-payment! :order-001 {:amount 59.98 :method :credit-card})
```

## Testing

For testing, you can use an in-memory approach or a test database:

```clojure
;; Option 1: Use test database
(def test-pool (pool/pool {:database "statecharts_test" ...}))

(use-fixtures :each
  (fn [f]
    (pg-sc/create-tables! test-pool)
    (schema/truncate-tables! test-pool)
    (try (f)
      (finally
        (schema/truncate-tables! test-pool)))))

;; Option 2: Use the in-memory simple environment for unit tests
(require '[com.fulcrologic.statecharts.simple :as simple])
(def test-env (simple/simple-env))
```

## Binary Serialization (nippy)

The persistence layer uses [nippy](https://github.com/taoensso/nippy) for binary serialization to PostgreSQL `BYTEA` columns. This provides:

- **Perfect type preservation** - All Clojure types (keywords, symbols, sets, etc.) roundtrip exactly
- **Fast serialization** - Binary format is faster than JSON encoding/decoding
- **Simple implementation** - No type tagging or conversion logic needed

```clojure
;; All of this round-trips correctly through PostgreSQL
{:config #{:feature-a :feature-b}
 :user {:name "Jo"
        :roles #{:admin :user}}
 :items [:a :b :c]
 :metadata nil}
```

**Note:** Since data is stored as binary, you cannot query into the structure using SQL. If you need to query working memory fields, consider adding dedicated indexed columns for those fields.

## Observability

The persistence layer uses structured logging via `taoensso.timbre`. All log messages include relevant context as maps for easy parsing.

### Log Levels

| Level | What's Logged |
|-------|---------------|
| `info` | Session start, chart registration, event loop start/stop, maintenance operations |
| `debug` | Event queued/processed, session created/deleted, cache operations, claims |
| `warn` | Chart validation warnings, optimistic lock failures |
| `error` | Event handler exceptions, event loop errors |
| `trace` | Session updates, cache hits, SQL execution |

### Key Log Events

**Session Lifecycle:**
```clojure
;; info - session started
{:session-id :order-123, :statechart-src :orders/process, :initial-configuration #{:pending}}

;; debug - session created/deleted
{:session-id :order-123, :statechart-src :orders/process}
```

**Event Processing:**
```clojure
;; debug - event queued
{:event :pay, :target :order-123, :delay-ms nil, :node-id "worker-1"}

;; debug - events claimed
{:count 5, :node-id "worker-1", :session-filter nil}

;; debug - event processed
{:event-id 42, :event ":pay", :target "order-123", :duration-ms 12.5, :node-id "worker-1"}

;; error - handler exception (includes stack trace)
{:event-id 42, :event-name ":pay", :target "order-123", :node-id "worker-1"}
```

**Event Loop:**
```clojure
;; info - loop started
{:node-id "worker-1", :poll-interval-ms 100, :session-filter nil}

;; info - loop stopped
{:node-id "worker-1"}
```

**Maintenance:**
```clojure
;; info - stale claims recovered
{:count 3, :timeout-seconds 30}

;; info - old events purged
{:count 1000, :retention-days 7}
```

**Registry:**
```clojure
;; info - chart registered
{:src :orders/process, :states 5}

;; debug - cache miss
{:src :orders/process}

;; info - cache preloaded
{:count 10}
```

### Configuring Log Levels

```clojure
(require '[taoensso.timbre :as log])

;; Set persistence layer to debug
(log/set-level! :debug)

;; Or configure per-namespace
(log/merge-config!
 {:min-level [[#{"com.fulcrologic.statecharts.persistence.*"} :debug]
              [#{"*"} :info]]})
```

### Metrics Extraction

The structured log data can be parsed for metrics:
- **Event throughput**: Count `Event processed` messages
- **Processing latency**: Extract `:duration-ms` from processed events
- **Queue depth**: Use `queue-depth` function directly
- **Error rate**: Count error-level messages
- **Lock contention**: Count `Optimistic lock failure` warnings
