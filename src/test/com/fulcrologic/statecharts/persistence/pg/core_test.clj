(ns com.fulcrologic.statecharts.persistence.pg.core-test
  "Unit tests for PostgreSQL persistence core utilities.
   These tests do not require a database connection."
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.statecharts.persistence.pg.core :as core]))

;; -----------------------------------------------------------------------------
;; Session ID Serialization
;; -----------------------------------------------------------------------------

(deftest session-id->str-test
  (testing "string session IDs"
    (is (= "my-session" (core/session-id->str "my-session"))))

  (testing "keyword session IDs"
    (is (= ":my-session" (core/session-id->str :my-session)))
    (is (= ":ns/my-session" (core/session-id->str :ns/my-session))))

  (testing "symbol session IDs"
    (is (= "my-session" (core/session-id->str 'my-session))))

  (testing "UUID session IDs"
    (let [uuid (random-uuid)]
      (is (= (str uuid) (core/session-id->str uuid)))))

  (testing "numeric session IDs"
    (is (= "42" (core/session-id->str 42)))))

(deftest str->session-id-test
  (testing "nil input"
    (is (nil? (core/str->session-id nil))))

  (testing "keyword strings"
    (is (= :my-session (core/str->session-id ":my-session")))
    (is (= :ns/my-session (core/str->session-id ":ns/my-session"))))

  (testing "UUID strings"
    (let [uuid (random-uuid)]
      (is (= uuid (core/str->session-id (str uuid))))))

  (testing "plain strings"
    (is (= "my-session" (core/str->session-id "my-session")))))

(deftest session-id-roundtrip-test
  (testing "keyword roundtrip"
    (is (= :my-session (-> :my-session core/session-id->str core/str->session-id)))
    (is (= :ns/my-session (-> :ns/my-session core/session-id->str core/str->session-id))))

  (testing "UUID roundtrip"
    (let [uuid (random-uuid)]
      (is (= uuid (-> uuid core/session-id->str core/str->session-id))))))

;; -----------------------------------------------------------------------------
;; Nippy Serialization (freeze/thaw)
;; -----------------------------------------------------------------------------

(deftest freeze-thaw-simple-values-test
  (testing "nil"
    (is (nil? (core/thaw nil)))
    (is (nil? (-> nil core/freeze core/thaw))))

  (testing "numbers"
    (is (= 42 (-> 42 core/freeze core/thaw)))
    (is (= 3.14 (-> 3.14 core/freeze core/thaw))))

  (testing "strings"
    (is (= "hello" (-> "hello" core/freeze core/thaw))))

  (testing "booleans"
    (is (= true (-> true core/freeze core/thaw)))
    (is (= false (-> false core/freeze core/thaw)))))

(deftest freeze-thaw-clojure-types-test
  (testing "keywords are preserved"
    (is (= :my-key (-> :my-key core/freeze core/thaw)))
    (is (= :ns/my-key (-> :ns/my-key core/freeze core/thaw))))

  (testing "symbols are preserved"
    (is (= 'my-sym (-> 'my-sym core/freeze core/thaw)))
    (is (= 'ns/my-sym (-> 'ns/my-sym core/freeze core/thaw))))

  (testing "sets are preserved"
    (is (= #{1 2 3} (-> #{1 2 3} core/freeze core/thaw)))
    (is (= #{:a :b :c} (-> #{:a :b :c} core/freeze core/thaw))))

  (testing "vectors are preserved"
    (is (= [1 2 3] (-> [1 2 3] core/freeze core/thaw))))

  (testing "lists are preserved"
    (is (= '(1 2 3) (-> '(1 2 3) core/freeze core/thaw))))

  (testing "maps are preserved"
    (is (= {:a 1 :b 2} (-> {:a 1 :b 2} core/freeze core/thaw)))))

(deftest freeze-thaw-complex-structures-test
  (testing "nested maps with various types"
    (let [data {:config {:states #{:a :b}
                         :items [1 2 3]
                         :active? true}
                :metadata {:created-by 'user
                           :tags #{:foo :bar}}}]
      (is (= data (-> data core/freeze core/thaw)))))

  (testing "complex working memory structure"
    (let [wmem {:com.fulcrologic.statecharts/session-id :test-session
                :com.fulcrologic.statecharts/statechart-src :my-chart
                :com.fulcrologic.statecharts/configuration #{:s1 :s2 :uber}
                :com.fulcrologic.statecharts/initialized-states #{:s1 :uber}
                :com.fulcrologic.statecharts/history-value {:h1 [:s1]}
                :com.fulcrologic.statecharts/running? true
                :data-model {:counter 42
                             :items ["a" "b" "c"]
                             :nested {:set-field #{:x :y}
                                      :keyword-field :active}}}]
      (is (= wmem (-> wmem core/freeze core/thaw))))))

(deftest freeze-returns-bytes-test
  (testing "freeze returns a byte array"
    (let [result (core/freeze {:foo :bar})]
      (is (bytes? result)))))

(deftest thaw-requires-binary-protocol-test
  "Tests that thaw requires byte[] input and rejects strings.
   pg2 must be configured with :binary-decode? true."

  (testing "thaw handles byte array"
    (let [data {:foo :bar :baz [1 2 3] :keywords #{:a :b :c}}
          frozen (core/freeze data)
          thawed (core/thaw frozen)]
      (is (= data thawed))))

  (testing "thaw handles nil"
    (is (nil? (core/thaw nil))))

  (testing "thaw throws on string input (misconfigured pool)"
    (let [base64-encoder (java.util.Base64/getEncoder)
          data {:foo :bar}
          frozen (core/freeze data)
          base64-str (.encodeToString base64-encoder frozen)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"pg2 pool must have :binary-decode\?"
                            (core/thaw base64-str))))))

;; -----------------------------------------------------------------------------
;; Version Metadata
;; -----------------------------------------------------------------------------

(deftest version-metadata-test
  (testing "attach-version adds metadata"
    (let [wmem {:foo :bar}
          versioned (core/attach-version wmem 5)]
      (is (= {:foo :bar} versioned))
      (is (= 5 (core/get-version versioned)))))

  (testing "attach-version with nil returns nil"
    (is (nil? (core/attach-version nil 5))))

  (testing "get-version on unversioned returns nil"
    (is (nil? (core/get-version {:foo :bar})))))
