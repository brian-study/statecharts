(ns com.fulcrologic.statecharts.persistence.jdbc.known-issues-test
  "Red tests for known issues surfaced by the deep review of the JDBC persistence
   layer. Each test asserts the CORRECT behaviour and currently FAILS, documenting
   a real gap in the library. See the review notes for the full writeup.

   Run only these tests:
     clojure -M:dev:test:clj-tests :known-failing

   Skipped by default in both :unit and :integration runs."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.persistence.jdbc :as pg-sc]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.event-queue :as pg-eq]
   [com.fulcrologic.statecharts.persistence.jdbc.job-store :as job-store]
   [com.fulcrologic.statecharts.persistence.jdbc.schema :as schema]
   [com.fulcrologic.statecharts.protocols :as sp]
   [next.jdbc.connection :as jdbc.connection])
  (:import
   [com.zaxxer.hikari HikariDataSource]
   [java.util UUID]))

;; -----------------------------------------------------------------------------
;; Shared Fixtures (mirror integration_test.clj)
;; -----------------------------------------------------------------------------

(def ^:private test-config
  {:dbtype   "postgres"
   :dbname   (or (System/getenv "PG_TEST_DATABASE") "statecharts_test")
   :host     (or (System/getenv "PG_TEST_HOST") "localhost")
   :port     (parse-long (or (System/getenv "PG_TEST_PORT") "5432"))
   :username (or (System/getenv "PG_TEST_USER") "postgres")
   :password (or (System/getenv "PG_TEST_PASSWORD") "postgres")})

(def ^:dynamic *pool* nil)

(defn with-pool [f]
  (let [ds (jdbc.connection/->pool HikariDataSource test-config)]
    (try
      (binding [*pool* ds]
        (f))
      (finally
        (.close ^HikariDataSource ds)))))

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
;; Finding #1 (CRITICAL) — Optimistic locking disabled under process-event!
;; -----------------------------------------------------------------------------
;;
;; `with-processing-context` (algorithms/v20150901_impl.cljc:33) does
;; `(vswap! vwmem (fn [m] (merge {defaults} m)))`. Clojure's `merge` inherits
;; metadata from the leftmost non-nil arg, so `m#`'s `::version` metadata (set
;; by fetch-session) is dropped. Every save through `process-event!` therefore
;; sees `expected-version=nil` and takes the silent ON CONFLICT DO UPDATE
;; branch. Concurrent writers silently last-writer-wins; optimistic-lock-failure
;; never fires.

(deftest ^:known-failing processing-context-merge-preserves-metadata-test
  (testing "merge of defaults on the LEFT with wmem on the RIGHT should keep wmem's metadata"
    (let [wmem   (with-meta {:a 1 :b 2} {::core/version 42})
          merged (merge {:c 3} wmem)]
      (is (= {::core/version 42} (meta merged))
          "with-processing-context's (merge {defaults} wmem) strips wmem's metadata — root cause of finding #1"))))

;; -----------------------------------------------------------------------------
;; Finding #3 (HIGH) — create-job! can return nil under terminal-transition race
;; -----------------------------------------------------------------------------
;;
;; `create-job!` does `(or (try-insert!) (find-active) (try-insert!))`. If an
;; active job exists (try-insert! → nil), then terminates before SELECT
;; (find-active → nil), then a new active job appears before the retry insert
;; (retry try-insert! → nil), the `or` yields nil. Caller stringifies to "".

(deftest ^:known-failing create-job-never-returns-nil-under-race-test
  (testing "create-job! must always return a UUID, even when every DB call models a conflict"
    (with-redefs [core/execute-sql! (fn [& _] [])]
      (let [result (job-store/create-job!
                    :fake-pool
                    {:id           (UUID/randomUUID)
                     :session-id   :race-session
                     :invokeid     :race-invoke
                     :job-type     "http"
                     :payload      {}
                     :max-attempts 3})]
        (is (some? result)
            "create-job! returns nil under the modeled race; caller then stores job-id as empty string")))))

;; -----------------------------------------------------------------------------
;; Finding #4 (HIGH) — cancel-by-session! orphans in-flight handler work
;; -----------------------------------------------------------------------------
;;
;; cancel-by-session! transitions `running → cancelled` and clears the lease.
;; Worker's subsequent `complete!` fails the status-guard and returns false.
;; `get-undispatched-terminal-jobs` only scans `status IN ('succeeded','failed')`,
;; so the reconciler will never emit a terminal event for the cancelled job.
;; Parent session wedges waiting for done.invoke.X or error.invoke.X.

(deftest ^:known-failing ^:integration cancel-by-session-job-is-reconcilable-test
  (testing "after cancel-by-session!, the job should still be reachable by the reconciler"
    (let [session-id :cancel-orphan-session
          invokeid   :cancel-orphan-invoke
          job-id     (UUID/randomUUID)]
      (job-store/create-job! *pool* {:id           job-id
                                     :session-id   session-id
                                     :invokeid     invokeid
                                     :job-type     "http"
                                     :payload      {}
                                     :max-attempts 3})
      ;; Worker claims the job
      (job-store/claim-jobs! *pool* {:owner-id "w1" :lease-duration-seconds 30 :limit 1})

      ;; Session gets cancelled while worker is in-flight
      (job-store/cancel-by-session! *pool* session-id)

      ;; Worker tries to complete — rejected (correct).
      (is (false? (job-store/complete! *pool* job-id "w1"
                                       {:result "done"}
                                       "done.invoke.cancel-orphan-invoke"
                                       {:ok true}))
          "complete! on cancelled job is correctly rejected")

      ;; But the reconciler should still see the job so it can synthesise a
      ;; terminal event (error.invoke.X / cancelled) for the parent session.
      (let [undispatched (job-store/get-undispatched-terminal-jobs *pool* 10)]
        (is (some #(= job-id (:id %)) undispatched)
            "cancelled jobs should be visible to the reconciler for terminal event dispatch")))))

;; -----------------------------------------------------------------------------
;; Finding #5 (MEDIUM) — datasource-closed? doesn't handle non-HikariCP DataSources
;; -----------------------------------------------------------------------------
;;
;; `datasource-closed?` reflects for an `isClosed` method and returns false if
;; absent. A bare `javax.sql.DataSource` has no isClosed method — the event loop
;; will never auto-stop, and a failing DataSource pegs CPU at poll cadence.

(deftest ^:known-failing datasource-closed-detects-all-datasource-shapes-test
  (testing "datasource-closed? should signal closed for any DataSource that cannot issue connections"
    (let [closed?-fn @#'pg-sc/datasource-closed?
          ;; A DataSource whose getConnection always throws — effectively closed.
          ;; Does not expose an isClosed method.
          perma-closed-ds (reify javax.sql.DataSource
                            (getConnection [_]
                              (throw (java.sql.SQLException. "pool closed"))))]
      (is (true? (closed?-fn perma-closed-ds))
          "datasource-closed? should recognise a DataSource whose getConnection consistently fails as closed"))))

;; -----------------------------------------------------------------------------
;; Finding #8 (LOW) — Event :type namespace stripped on queue round-trip
;; -----------------------------------------------------------------------------
;;
;; `event->row` serialises `:event-type` as `(name (or type :external))`, which
;; drops the namespace from keyword types like `::sc/chart`. `row->event`
;; reconstructs via `(keyword event-type)`, so `::sc/chart` → `:chart`.

(deftest ^:known-failing ^:integration event-type-namespace-preserved-test
  (testing "namespaced keyword event types should round-trip through the queue"
    (let [queue     (pg-eq/new-queue *pool* "ns-roundtrip")
          received  (atom nil)
          target-id :ns-roundtrip-session]
      (sp/send! queue {} {:event  :test-event
                          :type   ::sc/chart
                          :target target-id
                          :data   {}})
      (sp/receive-events! queue {}
                          (fn [_env event] (reset! received event)))
      (is (= ::sc/chart (:type @received))
          ":type should preserve namespace through queue round-trip; currently reduced to :chart"))))
