(ns com.fulcrologic.statecharts.persistence.pg.core-spec
  "Tests for PostgreSQL persistence core utilities.

   Tests session ID serialization, nippy-based freeze/thaw, and version metadata.
   Note: SQL execution functions require a live database connection and are
   tested in integration_test.clj."
  (:require
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [fulcro-spec.core :refer [=> assertions behavior component specification]]))

(specification "Session ID Serialization"
  (component "session-id->str"
    (behavior "handles string session IDs"
      (assertions
        "passes through unchanged"
        (core/session-id->str "my-session") => "my-session"
        "handles empty string"
        (core/session-id->str "") => ""
        "handles strings with special characters"
        (core/session-id->str "session/with/slashes") => "session/with/slashes"
        "handles strings with spaces"
        (core/session-id->str "session with spaces") => "session with spaces"))

    (behavior "handles keyword session IDs"
      (assertions
        "simple keywords become :keyword strings"
        (core/session-id->str :my-session) => ":my-session"
        "namespaced keywords preserve namespace"
        (core/session-id->str :ns/my-session) => ":ns/my-session"
        "deeply namespaced keywords work"
        (core/session-id->str :com.example/session) => ":com.example/session"))

    (behavior "handles symbol session IDs"
      (assertions
        "simple symbols"
        (core/session-id->str 'my-session) => "my-session"
        "namespaced symbols"
        (core/session-id->str 'ns/my-session) => "ns/my-session"))

    (behavior "handles UUID session IDs"
      (let [uuid (java.util.UUID/fromString "550e8400-e29b-41d4-a716-446655440000")]
        (assertions
          "converts to standard UUID string"
          (core/session-id->str uuid) => "550e8400-e29b-41d4-a716-446655440000")))

    (behavior "handles numeric session IDs"
      (assertions
        "integers"
        (core/session-id->str 42) => "42"
        "longs"
        (core/session-id->str 9999999999999) => "9999999999999"
        "negative numbers"
        (core/session-id->str -1) => "-1")))

  (component "str->session-id"
    (behavior "handles nil input"
      (assertions
        "returns nil"
        (core/str->session-id nil) => nil))

    (behavior "handles keyword strings"
      (assertions
        "simple keywords"
        (core/str->session-id ":my-session") => :my-session
        "namespaced keywords"
        (core/str->session-id ":ns/my-session") => :ns/my-session
        "deeply namespaced keywords"
        (core/str->session-id ":com.example/session") => :com.example/session))

    (behavior "handles UUID strings"
      (let [uuid-str "550e8400-e29b-41d4-a716-446655440000"
            uuid (java.util.UUID/fromString uuid-str)]
        (assertions
          "parses valid UUID strings"
          (core/str->session-id uuid-str) => uuid)))

    (behavior "handles plain strings"
      (assertions
        "returns string as-is when not keyword or UUID"
        (core/str->session-id "my-session") => "my-session"
        "handles empty string"
        (core/str->session-id "") => ""
        "handles strings that look like numbers but aren't UUIDs"
        (core/str->session-id "12345") => "12345")))

  (component "session-id roundtrip"
    (behavior "preserves keyword identity"
      (assertions
        "simple keyword"
        (-> :test-session core/session-id->str core/str->session-id) => :test-session
        "namespaced keyword"
        (-> :ns/test core/session-id->str core/str->session-id) => :ns/test))

    (behavior "preserves UUID identity"
      (let [uuid (random-uuid)]
        (assertions
          (-> uuid core/session-id->str core/str->session-id) => uuid)))

    (behavior "preserves string identity"
      (assertions
        (-> "plain-string" core/session-id->str core/str->session-id) => "plain-string"))))

(specification "Nippy Serialization (freeze/thaw)"
  (component "freeze"
    (behavior "function exists and returns bytes"
      (assertions
        (fn? core/freeze) => true
        (bytes? (core/freeze {:a 1})) => true)))

  (component "thaw"
    (behavior "returns nil for nil input"
      (assertions
        (core/thaw nil) => nil))

    (behavior "function exists"
      (assertions
        (fn? core/thaw) => true)))

  (component "roundtrip"
    (behavior "preserves primitive values"
      (assertions
        (-> 42 core/freeze core/thaw) => 42
        (-> "hello" core/freeze core/thaw) => "hello"
        (-> true core/freeze core/thaw) => true
        (-> nil core/freeze core/thaw) => nil))

    (behavior "preserves collections"
      (assertions
        (-> [1 2 3] core/freeze core/thaw) => [1 2 3]
        (-> #{:a :b :c} core/freeze core/thaw) => #{:a :b :c}
        (-> {:a 1 :b 2} core/freeze core/thaw) => {:a 1 :b 2}
        (-> '(1 2 3) core/freeze core/thaw) => '(1 2 3)))

    (behavior "preserves nested structures"
      (let [data {:level1 {:level2 {:level3 #{:a :b}}}}]
        (assertions
          (-> data core/freeze core/thaw) => data)))

    (behavior "preserves working memory structure"
      (let [wmem {:com.fulcrologic.statecharts/session-id :test-session
                  :com.fulcrologic.statecharts/statechart-src :my-chart
                  :com.fulcrologic.statecharts/configuration #{:s1 :s2 :uber}
                  :com.fulcrologic.statecharts/initialized-states #{:s1 :uber}
                  :com.fulcrologic.statecharts/history-value {:h1 #{:s1}}
                  :com.fulcrologic.statecharts/running? true
                  :data {:counter 42
                         :items ["a" "b" "c"]
                         :nested {:set-field #{:x :y}
                                  :keyword-field :active}}}]
        (assertions
          (-> wmem core/freeze core/thaw) => wmem)))))

(specification "Version Metadata"
  (component "attach-version"
    (behavior "adds version to metadata"
      (let [wmem {:foo :bar}
            versioned (core/attach-version wmem 5)]
        (assertions
          "preserves original data"
          versioned => {:foo :bar}
          "adds version metadata"
          (core/get-version versioned) => 5)))

    (behavior "handles nil working memory"
      (assertions
        (core/attach-version nil 5) => nil))

    (behavior "handles zero version"
      (let [versioned (core/attach-version {:data 1} 0)]
        (assertions
          (core/get-version versioned) => 0)))

    (behavior "overwrites existing version"
      (let [v1 (core/attach-version {:data 1} 1)
            v2 (core/attach-version v1 2)]
        (assertions
          (core/get-version v2) => 2))))

  (component "get-version"
    (behavior "returns nil for unversioned data"
      (assertions
        (core/get-version {:foo :bar}) => nil
        (core/get-version nil) => nil))

    (behavior "returns version from metadata"
      (let [versioned (core/attach-version {:data 1} 42)]
        (assertions
          (core/get-version versioned) => 42)))))

(specification "SQL Execution Functions"
  (component "execute!"
    (behavior "function exists"
      (assertions
        (fn? core/execute!) => true)))

  (component "execute-one!"
    (behavior "function exists"
      (assertions
        (fn? core/execute-one!) => true)))

  (component "affected-row-count"
    (behavior "handles nil and numbers"
      (assertions
        (core/affected-row-count nil) => 0
        (core/affected-row-count 3) => 3))

    (behavior "handles summary maps from mutation queries"
      (assertions
        (core/affected-row-count {:updated 2}) => 2
        (core/affected-row-count {:deleted 5}) => 5
        (core/affected-row-count {:inserted 1}) => 1
        (core/affected-row-count {:next.jdbc/update-count 4}) => 4))

    (behavior "handles row sequences"
      (assertions
        (core/affected-row-count [{:id 1} {:id 2}]) => 2
        (core/affected-row-count []) => 0))))
