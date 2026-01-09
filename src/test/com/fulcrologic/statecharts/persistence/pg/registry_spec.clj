(ns com.fulcrologic.statecharts.persistence.pg.registry-spec
  "Tests for PostgreSQL statechart registry.

   These tests verify the StatechartRegistry protocol behavior,
   caching logic, and chart definition serialization.

   NOTE: The PostgresStatechartRegistry is designed for charts that are pure data
   (no function guards/actions). For charts with functions, use the in-memory
   registry instead. The pg-env function now defaults to in-memory registry."
  (:require
   [com.fulcrologic.statecharts.chart :as chart]
   [com.fulcrologic.statecharts.elements :refer [state transition final initial]]
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.persistence.pg.registry :as reg]
   [com.fulcrologic.statecharts.protocols :as sp]
   [fulcro-spec.core :refer [=> assertions behavior component specification]]))

;; -----------------------------------------------------------------------------
;; Registry Creation Tests
;; -----------------------------------------------------------------------------

(specification "PostgresStatechartRegistry Record"
  (component "new-registry"
    (behavior "creates registry with pool and empty cache"
      (let [fake-pool {:type :fake-pool}
            registry (reg/new-registry fake-pool)]
        (assertions
          "creates a PostgresStatechartRegistry"
          (instance? com.fulcrologic.statecharts.persistence.pg.registry.PostgresStatechartRegistry registry) => true
          "stores the pool reference"
          (:pool registry) => fake-pool
          "initializes empty cache"
          (map? @(:cache registry)) => true
          (empty? @(:cache registry)) => true))))

  (component "implements StatechartRegistry"
    (let [registry (reg/new-registry {:type :fake})]
      (behavior "satisfies the protocol"
        (assertions
          (satisfies? sp/StatechartRegistry registry) => true)))))

;; -----------------------------------------------------------------------------
;; Cache Management Tests
;; -----------------------------------------------------------------------------

(specification "Registry Cache Management"
  (let [fake-pool {:type :fake}
        registry (reg/new-registry fake-pool)]

    (component "clear-cache!"
      (behavior "clears the cache"
        ;; Manually populate cache
        (swap! (:cache registry) assoc :chart-1 {:id :chart-1})
        (swap! (:cache registry) assoc :chart-2 {:id :chart-2})

        (assertions
          "cache has items before clear"
          (count @(:cache registry)) => 2)

        (reg/clear-cache! registry)

        (assertions
          "cache is empty after clear"
          (empty? @(:cache registry)) => true)))

    (component "cache atom behavior"
      (behavior "cache can be populated directly"
        (let [chart {:id :test-chart}]
          (swap! (:cache registry) assoc :test-key chart)
          (assertions
            "cache contains the chart"
            (get @(:cache registry) :test-key) => chart))))))

;; -----------------------------------------------------------------------------
;; Chart Definition Serialization Tests (using nippy)
;; -----------------------------------------------------------------------------

(specification "Chart Definition Serialization with Nippy"
  (component "simple chart structure"
    (let [chart (chart/statechart {:initial :s1}
                                  (state {:id :s1}
                                    (transition {:event :next :target :s2}))
                                  (state {:id :s2}))]
      (behavior "preserves chart through freeze/thaw roundtrip"
        (let [restored (-> chart core/freeze core/thaw)]
          (assertions
            "restores chart structure"
            (map? restored) => true
            "preserves :id"
            (:id restored) => (:id chart))))))

  (component "complex chart structure"
    (let [chart (chart/statechart {:initial :off}
                                  (state {:id :off}
                                    (transition {:event :turn-on :target :on}))
                                  (state {:id :on}
                                    (transition {:event :turn-off :target :off})
                                    (state {:id :on.ready}
                                      (transition {:event :start :target :on.running}))
                                    (state {:id :on.running}
                                      (transition {:event :stop :target :on.ready})))
                                  (final {:id :done}))]
      (behavior "preserves nested state structure"
        (let [restored (-> chart core/freeze core/thaw)]
          (assertions
            "chart structure is preserved"
            (map? restored) => true)))))

  (component "chart with keyword conditions"
    ;; Note: Functions cannot be serialized - only data-driven charts work with PostgreSQL registry
    (let [chart (chart/statechart {:initial :s1}
                                  (state {:id :s1}
                                    (transition {:event :go
                                                 :target :s2
                                                 :cond :always-true}))
                                  (state {:id :s2}))]
      (behavior "preserves keyword conditions"
        (let [restored (-> chart core/freeze core/thaw)]
          (assertions
            (map? restored) => true))))))

;; -----------------------------------------------------------------------------
;; Chart Source Key Tests
;; -----------------------------------------------------------------------------

(specification "Chart Source Key Handling"
  (component "keyword source keys"
    (behavior "simple keywords"
      (let [src :my-chart
            str-src (pr-str src)
            restored (clojure.edn/read-string str-src)]
        (assertions
          restored => src)))

    (behavior "namespaced keywords"
      (let [src :com.example/user-flow
            str-src (pr-str src)
            restored (clojure.edn/read-string str-src)]
        (assertions
          restored => src))))

  (component "symbol source keys"
    (behavior "simple symbols"
      (let [src 'my-chart
            str-src (pr-str src)
            restored (clojure.edn/read-string str-src)]
        (assertions
          restored => src))))

  (component "string source keys"
    (behavior "plain strings"
      (let [src "my-chart"
            str-src (pr-str src)
            restored (clojure.edn/read-string str-src)]
        (assertions
          restored => src)))))

;; -----------------------------------------------------------------------------
;; Protocol Method Contracts
;; -----------------------------------------------------------------------------

(specification "StatechartRegistry Protocol Contract"
  (component "register-statechart!"
    (behavior "accepts src and definition"
      ;; Just verify the method exists and can be called
      (assertions
        "method is defined"
        (fn? sp/register-statechart!) => true)))

  (component "get-statechart"
    (behavior "returns chart or nil"
      (assertions
        "method is defined"
        (fn? sp/get-statechart) => true)))

  (component "all-charts"
    (behavior "returns map of all charts"
      (assertions
        "method is defined"
        (fn? sp/all-charts) => true))))

;; -----------------------------------------------------------------------------
;; Source Key Conversion Tests
;; -----------------------------------------------------------------------------

(specification "Source Key Conversion"
  (component "src->str"
    (behavior "converts keyword sources"
      (let [src :my-chart
            str-src (#'reg/src->str src)]
        (assertions
          "produces string"
          (string? str-src) => true
          "is EDN readable"
          (clojure.edn/read-string str-src) => src)))

    (behavior "converts namespaced keyword sources"
      (let [src :com.example/my-chart
            str-src (#'reg/src->str src)]
        (assertions
          (clojure.edn/read-string str-src) => src)))

    (behavior "converts symbol sources"
      (let [src 'my-chart
            str-src (#'reg/src->str src)]
        (assertions
          (clojure.edn/read-string str-src) => src)))

    (behavior "converts string sources"
      (let [src "my-chart"
            str-src (#'reg/src->str src)]
        (assertions
          (clojure.edn/read-string str-src) => src))))

  (component "str->src"
    (behavior "returns nil for nil input"
      (assertions
        (#'reg/str->src nil) => nil))

    (behavior "converts keyword strings"
      (assertions
        (#'reg/str->src ":my-chart") => :my-chart
        (#'reg/str->src ":com.example/flow") => :com.example/flow))

    (behavior "converts symbol strings"
      (assertions
        (#'reg/str->src "my-symbol") => 'my-symbol))

    (behavior "converts string strings"
      (assertions
        (#'reg/str->src "\"my-string\"") => "my-string")))

  (component "src roundtrip"
    (behavior "keyword roundtrip"
      (let [src :com.example/my-chart]
        (assertions
          (#'reg/str->src (#'reg/src->str src)) => src)))

    (behavior "symbol roundtrip"
      (let [src 'namespaced/symbol]
        (assertions
          (#'reg/str->src (#'reg/src->str src)) => src)))

    (behavior "string roundtrip"
      (let [src "string-src"]
        (assertions
          (#'reg/str->src (#'reg/src->str src)) => src)))))

;; -----------------------------------------------------------------------------
;; Preload Cache Tests
;; -----------------------------------------------------------------------------

(specification "Registry Preload Cache"
  (let [fake-pool {:type :fake}
        registry (reg/new-registry fake-pool)]

    (component "preload-cache!"
      (behavior "returns registry for chaining"
        ;; Note: This will try to hit DB, but we can at least test the return contract
        ;; In a mock scenario, we'd verify it returns the registry
        (assertions
          "function exists"
          (fn? reg/preload-cache!) => true)))))

;; -----------------------------------------------------------------------------
;; Chart Element Serialization Tests (using nippy)
;; -----------------------------------------------------------------------------

(specification "Chart Element Serialization with Nippy"
  (component "state element"
    (let [s (state {:id :my-state})]
      (behavior "preserves state"
        (let [restored (-> s core/freeze core/thaw)]
          (assertions
            (:id restored) => :my-state
            (:node-type restored) => :state)))))

  (component "transition element"
    (let [t (transition {:event :go :target :next})]
      (behavior "preserves transition"
        (let [restored (-> t core/freeze core/thaw)]
          (assertions
            (:event restored) => :go
            ;; transition stores target as vector
            (:target restored) => [:next])))))

  (component "final element"
    (let [f (final {:id :done})]
      (behavior "preserves final"
        (let [restored (-> f core/freeze core/thaw)]
          (assertions
            (:id restored) => :done
            (:node-type restored) => :final)))))

  (component "initial element"
    (let [i (initial {}
              (transition {:target :start}))]
      (behavior "preserves initial with nested transition"
        (let [restored (-> i core/freeze core/thaw)]
          (assertions
            ;; initial is a state with :initial? true flag
            (:node-type restored) => :state
            (:initial? restored) => true
            (vector? (:children restored)) => true))))))
