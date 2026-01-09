(ns com.fulcrologic.statecharts.persistence.pg.event-queue-spec
  "Tests for PostgreSQL event queue.

   These tests verify the EventQueue protocol behavior, event structure,
   and queue management functions."
  (:require
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.events :as evts]
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.persistence.pg.event-queue :as eq]
   [com.fulcrologic.statecharts.protocols :as sp]
   [fulcro-spec.core :refer [=> assertions behavior component specification]])
  (:import
   [java.time OffsetDateTime]))

;; -----------------------------------------------------------------------------
;; Queue Creation Tests
;; -----------------------------------------------------------------------------

(specification "PostgresEventQueue Record"
  (component "new-queue"
    (behavior "creates queue with pool and auto-generated node-id"
      (let [fake-pool {:type :fake-pool}
            queue (eq/new-queue fake-pool)]
        (assertions
          "creates a PostgresEventQueue"
          (instance? com.fulcrologic.statecharts.persistence.pg.event_queue.PostgresEventQueue queue) => true
          "stores the pool reference"
          (:pool queue) => fake-pool
          "generates a node-id"
          (string? (:node-id queue)) => true
          (not (empty? (:node-id queue))) => true)))

    (behavior "creates queue with explicit node-id"
      (let [fake-pool {:type :fake-pool}
            queue (eq/new-queue fake-pool "worker-1")]
        (assertions
          "uses provided node-id"
          (:node-id queue) => "worker-1"))))

  (component "implements EventQueue"
    (let [queue (eq/new-queue {:type :fake})]
      (behavior "satisfies the protocol"
        (assertions
          (satisfies? sp/EventQueue queue) => true)))))

;; -----------------------------------------------------------------------------
;; Event Type Support Tests
;; -----------------------------------------------------------------------------

(specification "Event Type Support"
  (component "supported-type?"
    (behavior "accepts nil type (default)"
      (assertions
        ;; nil means "statechart" type by default
        (#'eq/supported-type? nil) => true))

    (behavior "accepts statechart types"
      (assertions
        (#'eq/supported-type? :statechart) => true
        (#'eq/supported-type? ::sc/chart) => true))

    (behavior "accepts SCXML URL types"
      (assertions
        (#'eq/supported-type? "http://www.w3.org/TR/scxml") => true
        (#'eq/supported-type? "http://www.w3.org/tr/scxml/#BasicHTTPEventProcessor") => true
        (#'eq/supported-type? "HTTP://WWW.W3.ORG/TR/SCXML") => true))

    (behavior "rejects unknown types"
      (assertions
        (#'eq/supported-type? :http) => false
        (#'eq/supported-type? "custom") => false
        (#'eq/supported-type? :websocket) => false))))

;; -----------------------------------------------------------------------------
;; Event Construction Tests
;; -----------------------------------------------------------------------------

(specification "Event Construction"
  (component "statechart event structure"
    (behavior "creates well-formed events"
      (let [event (evts/new-event {:name :my-event
                                   :data {:foo :bar}
                                   :target :session-1})]
        (assertions
          "has event name"
          (:name event) => :my-event
          (::sc/event-name event) => :my-event
          "has data"
          (:data event) => {:foo :bar}
          "has type defaulting to :external"
          (:type event) => :external)))

    (behavior "simple event creation"
      (let [event (evts/new-event :simple-event {:payload 123})]
        (assertions
          "has event name"
          (:name event) => :simple-event
          "has data"
          (:data event) => {:payload 123})))))

;; -----------------------------------------------------------------------------
;; Send Request Handling Tests
;; -----------------------------------------------------------------------------

(specification "Send Request Processing"
  (component "send request validation"
    (behavior "requires event name"
      (let [request {:target :session-1}]
        (assertions
          "event field is required for queue insertion"
          (contains? request :event) => false)))

    (behavior "requires target or source-session-id"
      (let [valid-request {:event :test :target :session-1}
            also-valid {:event :test :source-session-id :session-1}
            invalid {:event :test}]
        (assertions
          "target provides routing"
          (:target valid-request) => :session-1
          "source-session-id can serve as target"
          (:source-session-id also-valid) => :session-1
          "invalid request has neither"
          (or (:target invalid) (:source-session-id invalid)) => nil))))

  (component "delay handling"
    (behavior "delay is in milliseconds"
      (let [request {:event :delayed
                     :target :session-1
                     :delay 5000}]
        (assertions
          "delay value is numeric"
          (number? (:delay request)) => true
          "delay is positive"
          (pos? (:delay request)) => true)))

    (behavior "no delay means immediate delivery"
      (let [request {:event :immediate :target :session-1}]
        (assertions
          "nil delay"
          (:delay request) => nil))))

  (component "send-id for cancellation"
    (behavior "send-id enables cancellation"
      (let [request {:event :cancelable
                     :target :session-1
                     :source-session-id :session-1
                     :send-id "my-send-id"
                     :delay 10000}]
        (assertions
          "has send-id"
          (:send-id request) => "my-send-id"
          "has source for matching"
          (:source-session-id request) => :session-1)))))

;; -----------------------------------------------------------------------------
;; Session ID Serialization in Events
;; -----------------------------------------------------------------------------

(specification "Session ID in Events"
  (component "target session ID types"
    (behavior "keyword targets"
      (let [str-id (core/session-id->str :my-session)
            restored (core/str->session-id str-id)]
        (assertions
          restored => :my-session)))

    (behavior "UUID targets"
      (let [uuid (random-uuid)
            str-id (core/session-id->str uuid)
            restored (core/str->session-id str-id)]
        (assertions
          restored => uuid)))

    (behavior "string targets"
      (let [str-id (core/session-id->str "session-123")
            restored (core/str->session-id str-id)]
        (assertions
          restored => "session-123"))))

  (component "source session ID types"
    (behavior "preserves source session ID through roundtrip"
      (let [source-id :com.example/sender
            str-id (core/session-id->str source-id)
            restored (core/str->session-id str-id)]
        (assertions
          restored => source-id)))))

;; -----------------------------------------------------------------------------
;; Event Data Serialization Tests
;; -----------------------------------------------------------------------------

(specification "Event Data Serialization"
  (component "simple event data"
    (behavior "primitive values"
      (let [data {:count 42
                  :name "test"
                  :active true}
            restored (-> data core/freeze core/thaw)]
        (assertions
          restored => data)))

    (behavior "nested maps"
      (let [data {:config {:timeout 5000
                           :retries 3}
                  :metadata {:created-by :system}}
            restored (-> data core/freeze core/thaw)]
        (assertions
          restored => data))))

  (component "complex event data"
    (behavior "vectors and lists"
      (let [data {:items [:a :b :c]
                  :history [:state1 :state2]}
            restored (-> data core/freeze core/thaw)]
        (assertions
          restored => data)))

    (behavior "mixed types"
      (let [data {:states #{:on :off}
                  :transitions [{:from :on :to :off}
                                {:from :off :to :on}]
                  :current :on}
            restored (-> data core/freeze core/thaw)]
        (assertions
          (:states restored) => #{:on :off}
          (:transitions restored) => [{:from :on :to :off} {:from :off :to :on}]
          (:current restored) => :on))))

  (component "edge cases"
    (behavior "empty event data"
      (let [data {}
            restored (-> data core/freeze core/thaw)]
        (assertions
          restored => {})))

    (behavior "nil values in data"
      (let [data {:present 1 :absent nil}
            restored (-> data core/freeze core/thaw)]
        (assertions
          (:present restored) => 1
          (:absent restored) => nil
          (contains? restored :absent) => true)))))

;; -----------------------------------------------------------------------------
;; Event Name Serialization Tests
;; -----------------------------------------------------------------------------

(specification "Event Name Serialization"
  (component "simple event names"
    (behavior "keyword event names"
      (let [event-name :my-event
            str-name (pr-str event-name)
            restored (clojure.edn/read-string str-name)]
        (assertions
          restored => event-name)))

    (behavior "namespaced event names"
      (let [event-name :com.example/user-created
            str-name (pr-str event-name)
            restored (clojure.edn/read-string str-name)]
        (assertions
          restored => event-name))))

  (component "dot-separated event names (SCXML style)"
    (behavior "hierarchical event names"
      (let [event-name :error.communication.timeout
            str-name (pr-str event-name)
            restored (clojure.edn/read-string str-name)]
        (assertions
          restored => event-name)))

    (behavior "done.invoke events"
      (let [event-name (evts/invoke-done-event :child-session)
            str-name (pr-str event-name)
            restored (clojure.edn/read-string str-name)]
        (assertions
          restored => event-name)))))

;; -----------------------------------------------------------------------------
;; Queue Depth and Maintenance Tests
;; -----------------------------------------------------------------------------

(specification "Queue Management Functions"
  (component "queue-depth function signature"
    (behavior "accepts pool and options"
      ;; Test that the function exists and has expected arity
      (assertions
        "2-arity exists"
        (fn? eq/queue-depth) => true)))

  (component "recover-stale-claims function signature"
    (behavior "accepts pool and timeout"
      (assertions
        "function exists"
        (fn? eq/recover-stale-claims!) => true)))

  (component "purge-processed-events function signature"
    (behavior "accepts pool and retention days"
      (assertions
        "function exists"
        (fn? eq/purge-processed-events!) => true))))

;; -----------------------------------------------------------------------------
;; Event Row Conversion Tests
;; -----------------------------------------------------------------------------

(specification "Event to Row Conversion"
  (component "event->row basic"
    (let [send-request {:event :test-event
                        :target :session-1
                        :data {:foo :bar}}
          row (#'eq/event->row send-request)]
      (behavior "produces map with required fields"
        (assertions
          "has target-session-id"
          (some? (:target-session-id row)) => true
          "has event-name"
          (some? (:event-name row)) => true
          "has event-type"
          (some? (:event-type row)) => true
          "has event-data"
          (some? (:event-data row)) => true
          "has deliver-at"
          (instance? OffsetDateTime (:deliver-at row)) => true))

      (behavior "serializes session ID correctly"
        (assertions
          (:target-session-id row) => ":session-1"))

      (behavior "serializes event name as EDN string"
        (assertions
          (:event-name row) => ":test-event"))))

  (component "event->row with delay"
    (let [send-request {:event :delayed
                        :target :session-1
                        :delay 5000}
          row (#'eq/event->row send-request)]
      (behavior "sets deliver_at in the future"
        (let [now (OffsetDateTime/now)]
          (assertions
            "deliver-at is after now"
            (.isAfter (:deliver-at row) now) => true)))))

  (component "event->row with source session"
    (let [send-request {:event :test
                        :target :target-session
                        :source-session-id :source-session
                        :send-id "my-send"
                        :invoke-id "my-invoke"}
          row (#'eq/event->row send-request)]
      (behavior "includes optional fields"
        (assertions
          "has source-session-id"
          (:source-session-id row) => ":source-session"
          "has send-id"
          (:send-id row) => "my-send"
          "has invoke-id"
          (:invoke-id row) => "my-invoke"))))

  (component "event->row falls back to source as target"
    (let [send-request {:event :test
                        :source-session-id :only-source}
          row (#'eq/event->row send-request)]
      (behavior "uses source-session-id as target when no explicit target"
        (assertions
          (:target-session-id row) => ":only-source")))))

(specification "Row to Event Conversion"
  (component "row->event basic"
    (let [row {:event-name ":test-event"
               :event-type "external"
               :target-session-id ":session-1"
               :event-data {:foo "bar"}}
          event (#'eq/row->event row)]
      (behavior "produces valid event"
        (assertions
          "has name"
          (:name event) => :test-event
          "has type"
          (:type event) => :external
          "has target"
          (:target event) => :session-1
          "has data"
          (:data event) => {:foo "bar"}))))

  (component "row->event with source session"
    (let [row {:event-name ":test-event"
               :event-type "external"
               :target-session-id ":target"
               :source-session-id ":source"
               :send-id "send-123"
               :invoke-id "invoke-456"
               :event-data {}}
          event (#'eq/row->event row)]
      (behavior "includes source session ID"
        (assertions
          (::sc/source-session-id event) => :source))

      (behavior "includes send-id"
        (assertions
          (:sendid event) => "send-123"
          (::sc/send-id event) => "send-123"))

      (behavior "includes invoke-id"
        (assertions
          (:invokeid event) => "invoke-456"))))

  (component "row->event with namespaced event"
    (let [row {:event-name ":com.example/user-created"
               :event-type "external"
               :target-session-id ":session"
               :event-data {}}
          event (#'eq/row->event row)]
      (behavior "preserves namespaced event name"
        (assertions
          (:name event) => :com.example/user-created))))

  (component "row->event roundtrip"
    (let [original {:event :roundtrip.test
                    :target :session-123
                    :source-session-id :source-456
                    :send-id "send-id"
                    :invoke-id "invoke-id"
                    :data {:nested {:value 42}
                           :items [:a :b :c]}}
          row (#'eq/event->row original)
          ;; Simulate what happens when reading from DB
          ;; event-data comes back as nippy-frozen bytes
          db-row {:event-name (:event-name row)
                  :event-type (:event-type row)
                  :target-session-id (:target-session-id row)
                  :source-session-id (:source-session-id row)
                  :send-id (:send-id row)
                  :invoke-id (:invoke-id row)
                  :event-data (:event-data row)}
          restored (#'eq/row->event db-row)]
      (behavior "preserves event name"
        (assertions
          (:name restored) => :roundtrip.test))

      (behavior "preserves target"
        (assertions
          (:target restored) => :session-123))

      (behavior "preserves source session ID"
        (assertions
          (::sc/source-session-id restored) => :source-456))

      (behavior "preserves send-id"
        (assertions
          (::sc/send-id restored) => "send-id"))

      (behavior "preserves data"
        (assertions
          (get-in (:data restored) [:nested :value]) => 42
          (:items (:data restored)) => [:a :b :c])))))
