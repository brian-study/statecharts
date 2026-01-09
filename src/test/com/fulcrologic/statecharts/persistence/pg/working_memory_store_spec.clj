(ns com.fulcrologic.statecharts.persistence.pg.working-memory-store-spec
  "Tests for PostgreSQL working memory store.

   These tests verify the WorkingMemoryStore protocol behavior and optimistic
   locking logic using stubbed database functions."
  (:require
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.persistence.pg.working-memory-store :as wms]
   [com.fulcrologic.statecharts.protocols :as sp]
   [fulcro-spec.core :refer [=> assertions behavior component specification when-mocking]]))

;; -----------------------------------------------------------------------------
;; Optimistic Locking Tests
;; -----------------------------------------------------------------------------

(specification "Optimistic Lock Failure"
  (component "optimistic-lock-failure"
    (behavior "creates exception with correct data"
      (let [ex (wms/optimistic-lock-failure :session-1 5)]
        (assertions
          "is an ExceptionInfo"
          (instance? clojure.lang.ExceptionInfo ex) => true
          "has correct message"
          (.getMessage ex) => "Optimistic lock failure: session was modified by another process"
          "has session-id in data"
          (:session-id (ex-data ex)) => :session-1
          "has expected-version in data"
          (:expected-version (ex-data ex)) => 5
          "has correct type"
          (:type (ex-data ex)) => ::wms/optimistic-lock-failure))))

  (component "optimistic-lock-failure?"
    (behavior "detects optimistic lock failures"
      (let [lock-ex (wms/optimistic-lock-failure :s1 1)
            other-ex (ex-info "Other error" {:type :other})]
        (assertions
          "returns true for lock failures"
          (wms/optimistic-lock-failure? lock-ex) => true
          "returns false for other exceptions"
          (wms/optimistic-lock-failure? other-ex) => false
          "returns false for non-ExceptionInfo"
          (wms/optimistic-lock-failure? (Exception. "test")) => false)))))

(specification "Optimistic Retry Helper"
  (component "with-optimistic-retry"
    (behavior "returns result on success"
      (let [call-count (atom 0)
            result (wms/with-optimistic-retry
                     (fn []
                       (swap! call-count inc)
                       :success))]
        (assertions
          "returns the result"
          result => :success
          "only calls once on success"
          @call-count => 1)))

    (behavior "retries on optimistic lock failure"
      (let [call-count (atom 0)
            result (wms/with-optimistic-retry
                     {:max-retries 3 :backoff-ms 1}
                     (fn []
                       (swap! call-count inc)
                       (if (< @call-count 3)
                         (throw (wms/optimistic-lock-failure :s1 1))
                         :success)))]
        (assertions
          "returns eventual success"
          result => :success
          "retried correct number of times"
          @call-count => 3)))

    (behavior "throws after max retries exceeded"
      (let [call-count (atom 0)]
        (try
          (wms/with-optimistic-retry
            {:max-retries 2 :backoff-ms 1}
            (fn []
              (swap! call-count inc)
              (throw (wms/optimistic-lock-failure :s1 1))))
          (catch clojure.lang.ExceptionInfo e
            (assertions
              "threw optimistic lock failure"
              (wms/optimistic-lock-failure? e) => true
              "attempted max-retries times"
              @call-count => 2)))))

    (behavior "does not retry non-lock exceptions"
      (let [call-count (atom 0)]
        (try
          (wms/with-optimistic-retry
            {:max-retries 3 :backoff-ms 1}
            (fn []
              (swap! call-count inc)
              (throw (ex-info "Other error" {:type :other}))))
          (catch clojure.lang.ExceptionInfo e
            (assertions
              "did not retry"
              @call-count => 1
              "threw original exception"
              (:type (ex-data e)) => :other)))))))

;; -----------------------------------------------------------------------------
;; Protocol Behavior Tests (Structural)
;; -----------------------------------------------------------------------------

(specification "PostgresWorkingMemoryStore Record"
  (let [fake-pool {:type :fake-pool}
        store (wms/new-store fake-pool)]

    (component "new-store"
      (behavior "creates a valid store"
        (assertions
          "creates a PostgresWorkingMemoryStore"
          (instance? com.fulcrologic.statecharts.persistence.pg.working_memory_store.PostgresWorkingMemoryStore store) => true
          "stores the pool reference"
          (:pool store) => fake-pool)))

    (component "implements WorkingMemoryStore"
      (behavior "satisfies the protocol"
        (assertions
          "implements get-working-memory"
          (satisfies? sp/WorkingMemoryStore store) => true)))))

;; -----------------------------------------------------------------------------
;; Version Tracking Tests
;; -----------------------------------------------------------------------------

(specification "Version Tracking in Working Memory"
  (component "version attachment and retrieval"
    (let [wmem {::sc/session-id :test
                ::sc/configuration #{:s1}}]

      (behavior "new working memory has no version"
        (assertions
          (core/get-version wmem) => nil))

      (behavior "version can be attached"
        (let [versioned (core/attach-version wmem 1)]
          (assertions
            "preserves data"
            (::sc/session-id versioned) => :test
            (::sc/configuration versioned) => #{:s1}
            "has version"
            (core/get-version versioned) => 1)))

      (behavior "version survives nippy roundtrip conceptually"
        ;; Note: actual version is stored in DB, not in frozen bytes
        ;; This tests that the data survives, version is re-attached on read
        (let [versioned (core/attach-version wmem 5)
              ;; Simulate: freeze (loses metadata)
              frozen (core/freeze versioned)
              ;; Simulate: thaw from DB
              restored (core/thaw frozen)
              ;; Simulate: re-attach version from DB column
              with-version (core/attach-version restored 5)]
          (assertions
            "data is preserved"
            (::sc/session-id with-version) => :test
            "version is restored"
            (core/get-version with-version) => 5))))))

;; -----------------------------------------------------------------------------
;; Working Memory Structure Tests
;; -----------------------------------------------------------------------------

(specification "Working Memory Nippy Serialization"
  (component "typical working memory structure"
    (let [wmem {::sc/session-id :my-session
                ::sc/statechart-src :my-chart
                ::sc/configuration #{:s1 :uber}
                ::sc/initialized-states #{:s1 :uber}
                ::sc/history-value {:h1 #{:prev-state}}
                ::sc/running? true
                ::sc/data-model {:counter 0
                                 :items [:a :b :c]
                                 :nested {:deep {:value 42}}}}]

      (behavior "survives freeze/thaw roundtrip"
        (let [restored (-> wmem core/freeze core/thaw)]
          (assertions
            "preserves session-id"
            (::sc/session-id restored) => :my-session
            "preserves statechart-src"
            (::sc/statechart-src restored) => :my-chart
            "preserves configuration set"
            (::sc/configuration restored) => #{:s1 :uber}
            "preserves initialized-states set"
            (::sc/initialized-states restored) => #{:s1 :uber}
            "preserves history-value with nested set"
            (::sc/history-value restored) => {:h1 #{:prev-state}}
            "preserves running flag"
            (::sc/running? restored) => true
            "preserves data-model"
            (get-in restored [::sc/data-model :nested :deep :value]) => 42)))))

  (component "edge cases in working memory"
    (behavior "handles empty configuration"
      (let [wmem {::sc/configuration #{}}
            restored (-> wmem core/freeze core/thaw)]
        (assertions
          (::sc/configuration restored) => #{})))

    (behavior "handles nil data-model"
      (let [wmem {::sc/session-id :s1 ::sc/data-model nil}
            restored (-> wmem core/freeze core/thaw)]
        (assertions
          (::sc/data-model restored) => nil)))

    (behavior "handles complex nested data"
      (let [wmem {::sc/data-model {:deep {:sets #{#{:a :b} #{:c :d}}
                                          :mixed [#{:e} {:f :g}]}}}
            restored (-> wmem core/freeze core/thaw)]
        (assertions
          "preserves set of sets"
          (get-in restored [::sc/data-model :deep :sets]) => #{#{:a :b} #{:c :d}}
          "preserves vector with mixed types"
          (get-in restored [::sc/data-model :deep :mixed]) => [#{:e} {:f :g}])))))

;; -----------------------------------------------------------------------------
;; Session ID Handling Tests
;; -----------------------------------------------------------------------------

(specification "Session ID Types in Store"
  (component "various session ID types"
    (behavior "keyword session IDs"
      (let [session-id :my-session
            str-id (core/session-id->str session-id)
            restored (core/str->session-id str-id)]
        (assertions
          "round-trips correctly"
          restored => session-id)))

    (behavior "namespaced keyword session IDs"
      (let [session-id :com.example/session-123
            str-id (core/session-id->str session-id)
            restored (core/str->session-id str-id)]
        (assertions
          "round-trips correctly"
          restored => session-id)))

    (behavior "UUID session IDs"
      (let [session-id (random-uuid)
            str-id (core/session-id->str session-id)
            restored (core/str->session-id str-id)]
        (assertions
          "round-trips correctly"
          restored => session-id)))

    (behavior "string session IDs"
      (let [session-id "session-abc-123"
            str-id (core/session-id->str session-id)
            restored (core/str->session-id str-id)]
        (assertions
          "round-trips correctly"
          restored => session-id)))))
