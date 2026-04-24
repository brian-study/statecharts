(ns com.fulcrologic.statecharts.invocation.statechart-spec
  "Tests for statechart invocation processor.

   FORK NOTE: This fork scopes child session-ids to the parent via
   `(str parent-session-id \".\" invokeid)` (see commit 7d018d7) to prevent
   cross-parent collisions when multiple parent sessions invoke the same chart
   with the same invokeid. Upstream uses bare invokeid as the child session-id.
   Tests here assert the scoped behavior."
  (:require
    [com.fulcrologic.statecharts :as sc]
    [com.fulcrologic.statecharts.chart :as chart]
    [com.fulcrologic.statecharts.elements :refer [final invoke state transition]]
    [com.fulcrologic.statecharts.invocation.statechart :as sut]
    [com.fulcrologic.statecharts.protocols :as sp]
    [com.fulcrologic.statecharts.simple :as simple]
    [fulcro-spec.core :refer [=> assertions behavior component specification]]))

;; =============================================================================
;; Test Charts
;; =============================================================================

(def simple-child
  "Simple child that completes immediately"
  (chart/statechart {}
    (state {:id :child/start}
      (transition {:target :child/done}))
    (final {:id :child/done})))

(def waiting-child
  "Child that waits for event"
  (chart/statechart {}
    (state {:id :child/waiting}
      (transition {:event :proceed :target :child/done}))
    (final {:id :child/done})))

;; =============================================================================
;; Slice 1: Basic Protocol Implementation
;; =============================================================================

(specification "StatechartInvocationProcessor - Protocol Methods"
  (component "supports-invocation-type?"
    (let [processor (sut/new-invocation-processor)]
      (behavior "returns true for :statechart type"
        (assertions
          (sp/supports-invocation-type? processor :statechart) => true))

      (behavior "returns true for ::sc/chart type"
        (assertions
          (sp/supports-invocation-type? processor ::sc/chart) => true))

      (behavior "returns true for W3C SCXML URL (case-insensitive)"
        (assertions
          (sp/supports-invocation-type? processor "http://www.w3.org/TR/scxml/") => true
          (sp/supports-invocation-type? processor "HTTP://WWW.W3.ORG/TR/SCXML/") => true))

      (behavior "returns false for other types"
        (assertions
          (sp/supports-invocation-type? processor :future) => false
          (sp/supports-invocation-type? processor :http) => false))))

  (component "start-invocation! creates child session"
    (let [processor  (sut/new-invocation-processor)
          env        (simple/simple-env)
          wmstore    (::sc/working-memory-store env)
          parent-env (assoc env ::sc/vwmem (volatile! {::sc/session-id :parent-123}))
          scoped-id  (str :parent-123 "." :my-child)]

      ;; Register child chart
      (simple/register! env ::simple-child simple-child)

      ;; Create parent working memory with session
      (let [parent-wmem {::sc/session-id :parent-123}]
        (sp/save-working-memory! wmstore env :parent-123 parent-wmem))

      ;; Start invocation
      (sp/start-invocation! processor parent-env
        {:invokeid :my-child
         :src      ::simple-child
         :params   {:foo :bar}})

      (behavior "child working memory exists"
        (let [child-wmem (sp/get-working-memory wmstore env scoped-id)]
          (assertions
            "child session created"
            (some? child-wmem) => true
            "child session-id is parent-scoped"
            (::sc/session-id child-wmem) => scoped-id
            "child knows its parent"
            (::sc/parent-session-id child-wmem) => :parent-123
            "child has invocation params"
            (::sc/invocation-data child-wmem) => {:foo :bar})))

      (behavior "start-invocation! returns true"
        (let [result (sp/start-invocation! processor parent-env
                       {:invokeid :another-child
                        :src      ::simple-child
                        :params   {}})]
          (assertions
            result => true)))))

  (component "start-invocation! with missing chart"
    (let [processor  (sut/new-invocation-processor)
          env        (simple/simple-env)
          queue      (::sc/event-queue env)
          parent-env (assoc env ::sc/vwmem (volatile! {::sc/session-id :parent-456}))]

      ;; Try to invoke non-existent chart
      (sp/start-invocation! processor parent-env
        {:invokeid :my-child
         :src      ::missing-chart
         :params   {}})

      (behavior "sends error.platform event to parent"
        ;; Check the queue for error event
        (let [sends         @(:session-queues queue)
              parent-events (get sends :parent-456)
              error-event   (first (filter #(= :error.platform (:name %)) parent-events))]
          (assertions
            "error event exists"
            (some? error-event) => true
            "error includes diagnostic data"
            (contains? (:data error-event) :message) => true)))

      (let [result (sp/start-invocation! processor parent-env
                     {:invokeid :test
                      :src      ::nonexistent
                      :params   {}})]
        (assertions
          "returns false on failure"
          result => false))))

  (component "stop-invocation! removes child session"
    (let [processor  (sut/new-invocation-processor)
          env        (simple/simple-env)
          wmstore    (::sc/working-memory-store env)
          proc       (::sc/processor env)
          parent-env (assoc env ::sc/vwmem (volatile! {::sc/session-id :parent-789}))
          scoped-id  (str :parent-789 "." :child-789)]

      ;; Register and start child at the scoped id that stop-invocation! will look up
      (simple/register! env ::simple-child simple-child)
      (let [child-wmem (sp/start! proc env ::simple-child {::sc/session-id scoped-id})]
        (sp/save-working-memory! wmstore env scoped-id child-wmem))

      (behavior "child exists before stop"
        (assertions
          (some? (sp/get-working-memory wmstore env scoped-id)) => true))

      ;; Stop invocation (processor resolves scoped-id from parent-env + invokeid)
      (sp/stop-invocation! processor parent-env {:invokeid :child-789})

      (behavior "child removed after stop"
        (assertions
          (sp/get-working-memory wmstore env scoped-id) => nil))

      (behavior "stop-invocation! returns true"
        (assertions
          (sp/stop-invocation! processor parent-env {:invokeid :nonexistent}) => true))))

  (component "forward-event! sends to child"
    (let [processor  (sut/new-invocation-processor)
          env        (simple/simple-env)
          queue      (::sc/event-queue env)
          parent-env (assoc env ::sc/vwmem (volatile! {::sc/session-id :parent-999}))
          scoped-id  (str :parent-999 "." :child-999)]

      ;; Forward event
      (sp/forward-event! processor parent-env
        {:invokeid :child-999
         :type     ::sc/chart
         :event    {:name :test-event
                    :data {:x 1}}})

      (behavior "event delivered to scoped child session"
        (let [sends        @(:session-queues queue)
              child-events (get sends scoped-id)
              forwarded    (first (filter #(= :test-event (:name %)) child-events))]
          (assertions
            "event exists"
            (some? forwarded) => true
            "event has correct name"
            (:name forwarded) => :test-event
            "event includes data"
            (:data forwarded) => {:x 1}
            "event knows source"
            (::sc/source-session-id forwarded) => :parent-999)))

      (behavior "forward-event! returns true"
        (assertions
          (sp/forward-event! processor parent-env
            {:invokeid :child-999
             :event    {:name :another}}) => true)))))

;; =============================================================================
;; Slice 2: End-to-End Integration
;; =============================================================================

(specification "StatechartInvocationProcessor - Integration"
  (component "Parent-child lifecycle"
    (let [env          (simple/simple-env)
          parent-chart (chart/statechart {}
                         (state {:id :parent/active}
                           (invoke {:id   :my-child
                                    :type :statechart
                                    :src  ::simple-child})
                           (transition {:event  :done.invoke.my-child
                                        :target :parent/complete}))
                         (final {:id :parent/complete}))]

      ;; Register charts
      (simple/register! env ::simple-child simple-child)
      (simple/register! env ::parent-chart parent-chart)

      ;; Start parent
      (simple/start! env ::parent-chart :parent-session)

      (behavior "parent session exists"
        (let [wmstore     (::sc/working-memory-store env)
              parent-wmem (sp/get-working-memory wmstore env :parent-session)]
          (assertions
            (some? parent-wmem) => true
            "parent in active state"
            (contains? (::sc/configuration parent-wmem) :parent/active) => true)))

      (behavior "child session exists"
        ;; The invoke element above supplies `:id :my-child`, which per
        ;; elements.cljc's docstring IS the session id of the invoked chart
        ;; when explicitly provided. Auto-generated invokeids get scoped to
        ;; parent, but user-supplied ones are used verbatim — required so the
        ;; documented `:child-session-id` routing option in the Fulcro
        ;; integration still addresses the child at the expected id.
        (let [wmstore    (::sc/working-memory-store env)
              explicit-id (str :my-child)
              child-wmem (sp/get-working-memory wmstore env explicit-id)]
          (assertions
            "child created at the user-supplied invoke id (not parent-scoped)"
            (some? child-wmem) => true
            "child knows parent"
            (::sc/parent-session-id child-wmem) => :parent-session))))))

;; =============================================================================
;; Slice 3: Edge Cases and Error Conditions
;; =============================================================================

(specification "StatechartInvocationProcessor - Edge Cases"
  (component "Constructor creates processor"
    (behavior "new-invocation-processor returns processor instance"
      (let [proc (sut/new-invocation-processor)]
        (assertions
          (some? proc) => true
          (satisfies? sp/InvocationProcessor proc) => true))))

  (component "Processor handles empty params"
    (let [processor (sut/new-invocation-processor)
          env       (simple/simple-env)]

      (simple/register! env ::simple-child simple-child)

      (behavior "accepts nil params"
        (let [parent-env (assoc env ::sc/vwmem (volatile! {::sc/session-id :test}))]
          (assertions
            (sp/start-invocation! processor parent-env
              {:invokeid :child1
               :src      ::simple-child
               :params   nil}) => true)))

      (behavior "accepts empty map params"
        (let [parent-env (assoc env ::sc/vwmem (volatile! {::sc/session-id :test}))]
          (assertions
            (sp/start-invocation! processor parent-env
              {:invokeid :child2
               :src      ::simple-child
               :params   {}}) => true))))))
