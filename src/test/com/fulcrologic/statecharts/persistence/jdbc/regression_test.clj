(ns com.fulcrologic.statecharts.persistence.jdbc.regression-test
  "Regression tests for issues surfaced by the deep review of the JDBC
   persistence layer and fixed in 2.0.2. Each test pins behaviour at its
   correct post-fix state; if one of these turns red, the corresponding fix
   regressed."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.algorithms.v20150901-impl :as impl]
   [com.fulcrologic.statecharts.chart :as chart]
   [com.fulcrologic.statecharts.elements :refer [state]]
   [com.fulcrologic.statecharts.persistence.jdbc :as pg-sc]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.event-queue :as pg-eq]
   [com.fulcrologic.statecharts.persistence.jdbc.job-store :as job-store]
   [com.fulcrologic.statecharts.persistence.jdbc.schema :as schema]
   [com.fulcrologic.statecharts.protocols :as sp]
   [com.fulcrologic.statecharts.registry.local-memory-registry :as mem-reg]
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

(deftest processing-env-preserves-wmem-metadata-test
  (testing "processing-env must preserve wmem's metadata (e.g. the ::version that JDBC stores attach)"
    (let [chart       (chart/statechart {:initial :s1} (state {:id :s1}))
          registry    (mem-reg/new-registry)
          _           (sp/register-statechart! registry :test-chart chart)
          env         {::sc/statechart-registry registry}
          wmem        (core/attach-version {::sc/session-id :x ::sc/statechart-src :test-chart} 42)
          p-env       (impl/processing-env env :test-chart wmem)
          vwmem-value (some-> p-env ::sc/vwmem deref)]
      (is (= 42 (core/get-version vwmem-value))
          "processing-env drops wmem's ::version meta via (merge {defaults} wmem); the JDBC store then never triggers optimistic locking"))))

;; -----------------------------------------------------------------------------
;; Finding #3 (HIGH) — create-job! can return nil under terminal-transition race
;; -----------------------------------------------------------------------------
;;
;; `create-job!` does `(or (try-insert!) (find-active) (try-insert!))`. If an
;; active job exists (try-insert! → nil), then terminates before SELECT
;; (find-active → nil), then a new active job appears before the retry insert
;; (retry try-insert! → nil), the `or` yields nil. Caller stringifies to "".

(deftest create-job-never-returns-nil-under-race-test
  (testing "create-job! must not silently return nil under adversarial races"
    ;; After the fix, an exhausted race throws ex-info rather than returning nil.
    ;; Either behaviour (non-nil return or throw) satisfies the invariant "caller
    ;; never sees a nil job-id that it would then stringify to ''".
    (with-redefs [core/execute-sql! (fn [& _] [])]
      (let [params {:id           (UUID/randomUUID)
                    :session-id   :race-session
                    :invokeid     :race-invoke
                    :job-type     "http"
                    :payload      {}
                    :max-attempts 3}
            {:keys [result error]} (try
                                     {:result (job-store/create-job! :fake-pool params)}
                                     (catch Exception e {:error e}))]
        (is (or (some? result) (some? error))
            "create-job! should return a UUID or throw; never return nil")
        (when error
          (is (re-find #"(?i)exhausted" (.getMessage ^Exception error))
              "race-exhaustion error is informative"))))))

;; -----------------------------------------------------------------------------
;; Finding #4 (HIGH) — cancel-by-session! orphans in-flight handler work
;; -----------------------------------------------------------------------------
;;
;; cancel-by-session! transitions `running → cancelled` and clears the lease.
;; Worker's subsequent `complete!` fails the status-guard and returns false.
;; `get-undispatched-terminal-jobs` only scans `status IN ('succeeded','failed')`,
;; so the reconciler will never emit a terminal event for the cancelled job.
;; Parent session wedges waiting for done.invoke.X or error.invoke.X.

(deftest ^:integration cancel-by-session-job-is-reconcilable-test
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

(deftest datasource-closed-detects-all-datasource-shapes-test
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

(deftest ^:integration event-type-namespace-preserved-test
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
