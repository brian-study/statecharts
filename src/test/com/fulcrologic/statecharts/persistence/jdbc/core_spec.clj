(ns com.fulcrologic.statecharts.persistence.jdbc.core-spec
  "Tests for PostgreSQL persistence core utilities.

   Tests session ID serialization, nippy-based freeze/thaw, and version metadata.
   Note: SQL execution functions require a live database connection and are
   tested in integration_test.clj."
  (:require
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [fulcro-spec.core :refer [=> assertions behavior component specification]]))

(specification "Session ID Serialization"
  (component "session-id->str"
    (behavior "handles string session IDs"
      (assertions
        "strings are quoted via pr-str so the decoder can distinguish them from UUID/keyword bare forms"
        (core/session-id->str "my-session") => "\"my-session\""
        "handles empty string"
        (core/session-id->str "") => "\"\""
        "handles strings with special characters"
        (core/session-id->str "session/with/slashes") => "\"session/with/slashes\""
        "handles strings with spaces"
        (core/session-id->str "session with spaces") => "\"session with spaces\""))

    (behavior "handles keyword session IDs"
      (assertions
        "simple keywords become :keyword strings"
        (core/session-id->str :my-session) => ":my-session"
        "namespaced keywords preserve namespace"
        (core/session-id->str :ns/my-session) => ":ns/my-session"
        "deeply namespaced keywords work"
        (core/session-id->str :com.example/session) => ":com.example/session"))

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
        (core/session-id->str -1) => "-1"
        "BigInt is tagged (so type can be recovered on read)"
        (core/session-id->str 42N) => "42N"
        "BigDecimal is tagged"
        (core/session-id->str 3.14M) => "3.14M"
        "Ratio is written with a slash"
        (core/session-id->str (/ 1 2)) => "1/2"))

    (behavior "handles symbols via the :else fallback (symbols are not in ::sc/id)"
      ;; ::sc/id is [:or uuid? number? keyword? string?] — symbols aren't in
      ;; the spec. The :else branch uses (str x), so symbols go through but
      ;; do NOT round-trip (they come back as strings or numbers depending
      ;; on content). Pinned here so the degradation is explicit.
      (assertions
        "simple symbols degrade to bare name"
        (core/session-id->str 'my-session) => "my-session"
        "namespaced symbols degrade to bare name"
        (core/session-id->str 'ns/my-session) => "ns/my-session")))

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
        "quoted string form (the new write shape) round-trips"
        (core/str->session-id "\"my-session\"") => "my-session"
        "legacy bare non-UUID non-keyword strings still decode as strings"
        (core/str->session-id "my-session") => "my-session"
        "handles empty string (legacy bare)"
        (core/str->session-id "") => ""))

    (behavior "handles numeric strings (SCXML numeric session-ids)"
      (assertions
        "bare integer → long"
        (core/str->session-id "42") => 42
        "bare long"
        (core/str->session-id "9999999999999") => 9999999999999
        "bare negative integer"
        (core/str->session-id "-1") => -1
        "bare double → double"
        (core/str->session-id "3.14") => 3.14
        "strings that need to stay as strings must be written via pr-str (quoted)"
        (core/str->session-id "\"42\"") => "42"))

    (behavior "handles tagged numeric subtypes (BigInt / BigDecimal / Ratio)"
      (assertions
        "BigInt (42N)"
        (core/str->session-id "42N") => 42N
        "negative BigInt"
        (core/str->session-id "-42N") => -42N
        "BigDecimal (3.14M)"
        (core/str->session-id "3.14M") => 3.14M
        "negative BigDecimal"
        (core/str->session-id "-3.14M") => -3.14M
        "Ratio"
        (core/str->session-id "1/2") => (/ 1 2)
        "negative-numerator Ratio"
        (core/str->session-id "-3/7") => (/ -3 7)))))

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
        (-> "plain-string" core/session-id->str core/str->session-id) => "plain-string"
        "string that looks like a number stays a string via pr-str marker"
        (-> "42" core/session-id->str core/str->session-id) => "42"))

    (behavior "preserves numeric identity"
      (assertions
        "integer"
        (-> 42 core/session-id->str core/str->session-id) => 42
        "long"
        (-> (long 9999999999999) core/session-id->str core/str->session-id) => 9999999999999
        "double"
        (-> 3.14 core/session-id->str core/str->session-id) => 3.14
        "negative number"
        (-> -1 core/session-id->str core/str->session-id) => -1
        "BigInt keeps its type"
        (-> 42N core/session-id->str core/str->session-id) => 42N
        "BigDecimal keeps its type"
        (-> 3.14M core/session-id->str core/str->session-id) => 3.14M
        "Ratio keeps its type"
        (-> (/ 1 2) core/session-id->str core/str->session-id) => (/ 1 2))))

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

    (behavior "handles next.jdbc update-count maps from mutation queries"
      (assertions
        ;; next.jdbc returns `{:next.jdbc/update-count N}` for mutations
        ;; without RETURNING. Legacy pg2-era keys (`:updated`/`:deleted`/
        ;; `:inserted`) are no longer produced by next.jdbc and were
        ;; removed from affected-row-count; a map without the
        ;; next.jdbc key returns 0.
        (core/affected-row-count {:next.jdbc/update-count 4}) => 4
        (core/affected-row-count {:some-other-key 42}) => 0))

    (behavior "handles row sequences"
      (assertions
        (core/affected-row-count [{:id 1} {:id 2}]) => 2
        (core/affected-row-count []) => 0))))
