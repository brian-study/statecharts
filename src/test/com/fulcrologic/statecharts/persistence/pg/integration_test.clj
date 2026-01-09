(ns com.fulcrologic.statecharts.persistence.pg.integration-test
  "Integration tests for PostgreSQL persistence layer.

   These tests require a running PostgreSQL instance.

   To run these tests:
   1. Ensure PostgreSQL is running on localhost:5432
   2. Create a test database: createdb statecharts_test
   3. Run with: clj -M:test -m kaocha.runner --focus :integration

   Environment variables for custom configuration:
   - PG_TEST_HOST (default: localhost)
   - PG_TEST_PORT (default: 5432)
   - PG_TEST_DATABASE (default: statecharts_test)
   - PG_TEST_USER (default: postgres)
   - PG_TEST_PASSWORD (default: postgres)"
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.chart :as chart]
   [com.fulcrologic.statecharts.elements :as ele :refer [state transition]]
   [com.fulcrologic.statecharts.persistence.pg :as pg-sc]
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.persistence.pg.event-queue :as pg-eq]
   [com.fulcrologic.statecharts.persistence.pg.registry :as pg-reg]
   [com.fulcrologic.statecharts.persistence.pg.schema :as schema]
   [com.fulcrologic.statecharts.persistence.pg.working-memory-store :as pg-wms]
   [com.fulcrologic.statecharts.protocols :as sp]
   [pg.core :as pg]
   [pg.pool :as pool]))

;; -----------------------------------------------------------------------------
;; Test Configuration
;; -----------------------------------------------------------------------------

(def ^:private test-config
  {:host (or (System/getenv "PG_TEST_HOST") "localhost")
   :port (parse-long (or (System/getenv "PG_TEST_PORT") "5432"))
   :database (or (System/getenv "PG_TEST_DATABASE") "statecharts_test")
   :user (or (System/getenv "PG_TEST_USER") "postgres")
   :password (or (System/getenv "PG_TEST_PASSWORD") "postgres")})

(def ^:dynamic *pool* nil)

;; -----------------------------------------------------------------------------
;; Test Fixtures
;; -----------------------------------------------------------------------------

(defn with-pool [f]
  (let [pool (pool/pool test-config)]
    (try
      (binding [*pool* pool]
        (f))
      (finally
        (pool/close pool)))))

(defn with-clean-tables [f]
  (schema/create-tables! *pool*)
  (schema/truncate-tables! *pool*)
  (try
    (f)
    (finally
      (schema/truncate-tables! *pool*))))

(use-fixtures :once with-pool)
(use-fixtures :each with-clean-tables)

;; -----------------------------------------------------------------------------
;; Working Memory Store Tests
;; -----------------------------------------------------------------------------

(deftest working-memory-store-basic-test
  (let [store (pg-wms/new-store *pool*)
        session-id :test-session
        wmem {::sc/session-id session-id
              ::sc/statechart-src :my-chart
              ::sc/configuration #{:s1 :uber}
              ::sc/initialized-states #{:s1 :uber}
              ::sc/history-value {}
              ::sc/running? true}]

    (testing "get-working-memory returns nil for non-existent session"
      (is (nil? (sp/get-working-memory store {} :non-existent))))

    (testing "save and retrieve working memory"
      (sp/save-working-memory! store {} session-id wmem)
      (let [retrieved (sp/get-working-memory store {} session-id)]
        (is (= wmem retrieved))
        (is (= 1 (core/get-version retrieved)))))

    (testing "update with optimistic locking"
      (let [retrieved (sp/get-working-memory store {} session-id)
            updated (assoc retrieved ::sc/configuration #{:s2 :uber})]
        (sp/save-working-memory! store {} session-id updated)
        (let [retrieved2 (sp/get-working-memory store {} session-id)]
          (is (= #{:s2 :uber} (::sc/configuration retrieved2)))
          (is (= 2 (core/get-version retrieved2))))))

    (testing "delete working memory"
      (sp/delete-working-memory! store {} session-id)
      (is (nil? (sp/get-working-memory store {} session-id))))))

(deftest working-memory-store-optimistic-lock-test
  (let [store (pg-wms/new-store *pool*)
        session-id :lock-test
        wmem {::sc/session-id session-id
              ::sc/statechart-src :my-chart
              ::sc/configuration #{:s1}
              ::sc/initialized-states #{:s1}
              ::sc/history-value {}
              ::sc/running? true}]

    (sp/save-working-memory! store {} session-id wmem)

    (testing "concurrent modification throws"
      (let [v1 (sp/get-working-memory store {} session-id)
            v2 (sp/get-working-memory store {} session-id)]
        ;; First update succeeds
        (sp/save-working-memory! store {} session-id (assoc v1 ::sc/configuration #{:s2}))
        ;; Second update with stale version fails
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"Optimistic lock failure"
                              (sp/save-working-memory! store {} session-id
                                                       (assoc v2 ::sc/configuration #{:s3}))))))))

;; -----------------------------------------------------------------------------
;; Registry Tests
;; -----------------------------------------------------------------------------

(deftest registry-basic-test
  (let [registry (pg-reg/new-registry *pool*)
        chart (chart/statechart {:initial :s1}
                                (state {:id :s1}
                                       (transition {:event :next :target :s2}))
                                (state {:id :s2}))]

    (testing "get-statechart returns nil for unregistered"
      (is (nil? (sp/get-statechart registry :unknown))))

    (testing "register and retrieve chart"
      (sp/register-statechart! registry :my-chart chart)
      (is (= chart (sp/get-statechart registry :my-chart))))

    (testing "all-charts returns all registered"
      (sp/register-statechart! registry :another-chart chart)
      (let [all (sp/all-charts registry)]
        (is (= 2 (count all)))
        (is (contains? all :my-chart))
        (is (contains? all :another-chart))))

    (testing "cache is populated"
      (pg-reg/clear-cache! registry)
      (sp/get-statechart registry :my-chart) ; This should populate cache
      (is (contains? @(:cache registry) :my-chart)))))

;; -----------------------------------------------------------------------------
;; Event Queue Tests
;; -----------------------------------------------------------------------------

(deftest event-queue-basic-test
  (let [queue (pg-eq/new-queue *pool* "test-worker")
        processed (atom [])]

    (testing "send! inserts event"
      (is (true? (sp/send! queue {}
                           {:event :test-event
                            :target :session-1
                            :data {:foo :bar}}))))

    (testing "receive-events! processes and marks complete"
      (sp/receive-events! queue {}
                          (fn [env event]
                            (swap! processed conj event)))
      (is (= 1 (count @processed)))
      (is (= :test-event (:name (first @processed)))))

    (testing "processed event is not redelivered"
      (reset! processed [])
      (sp/receive-events! queue {} (fn [_ e] (swap! processed conj e)))
      (is (empty? @processed)))))

(deftest event-queue-delayed-test
  (let [queue (pg-eq/new-queue *pool* "test-worker")
        processed (atom [])]

    (testing "delayed event not immediately visible"
      (sp/send! queue {}
                {:event :delayed-event
                 :target :session-1
                 :delay 5000}) ; 5 second delay

      (sp/receive-events! queue {} (fn [_ e] (swap! processed conj e)))
      (is (empty? @processed)))

    (testing "cancel! removes pending delayed event"
      (sp/send! queue {}
                {:event :cancelable
                 :target :session-1
                 :source-session-id :session-1
                 :send-id "cancel-me"
                 :delay 10000})

      (sp/cancel! queue {} :session-1 "cancel-me")

      ;; Wait and verify it doesn't get delivered
      (Thread/sleep 100)
      (sp/receive-events! queue {} (fn [_ e] (swap! processed conj e)))
      (is (empty? @processed)))))

;; -----------------------------------------------------------------------------
;; Full Environment Integration Tests
;; -----------------------------------------------------------------------------

(deftest pg-env-integration-test
  (let [env (pg-sc/pg-env {:pool *pool*})
        chart (chart/statechart {:initial :s1}
                                (state {:id :s1}
                                       (transition {:event :next :target :s2}))
                                (state {:id :s2}))]

    (testing "register and start chart"
      (pg-sc/register! env :test-chart chart)
      (is (true? (pg-sc/start! env :test-chart :test-session))))

    (testing "session is persisted"
      (let [wmem (sp/get-working-memory (::sc/working-memory-store env) {} :test-session)]
        (is (some? wmem))
        (is (contains? (::sc/configuration wmem) :s1))))

    (testing "send event and process"
      (pg-sc/send! env {:event :next
                        :target :test-session})

      (let [event-queue (::sc/event-queue env)]
        (sp/receive-events! event-queue env
                            (fn [env event]
                              (require '[com.fulcrologic.statecharts.event-queue.event-processing :as ep])
                              ((resolve 'ep/standard-statechart-event-handler) env event)))))

    (testing "state has transitioned"
      (let [wmem (sp/get-working-memory (::sc/working-memory-store env) {} :test-session)]
        (is (contains? (::sc/configuration wmem) :s2))))))
