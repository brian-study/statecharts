(ns com.fulcrologic.statecharts.persistence.jdbc.regression-test
  "Regression tests for issues surfaced by the deep review of the JDBC
   persistence layer and fixed in 2.0.2. Each test pins behaviour at its
   correct post-fix state; if one of these turns red, the corresponding fix
   regressed."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.algorithms.v20150901-async-impl :as async-impl]
   [com.fulcrologic.statecharts.algorithms.v20150901-impl :as impl]
   [com.fulcrologic.statecharts.chart :as chart]
   [com.fulcrologic.statecharts.data-model.working-memory-data-model :as wmdm]
   [com.fulcrologic.statecharts.elements :refer [state]]
   [com.fulcrologic.statecharts.invocation.durable-job :as durable-job]
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
  (testing "sync processing-env must preserve wmem's metadata (::version that JDBC stores attach)"
    (let [chart       (chart/statechart {:initial :s1} (state {:id :s1}))
          registry    (mem-reg/new-registry)
          _           (sp/register-statechart! registry :test-chart chart)
          env         {::sc/statechart-registry registry}
          wmem        (core/attach-version {::sc/session-id :x ::sc/statechart-src :test-chart} 42)
          p-env       (impl/processing-env env :test-chart wmem)
          vwmem-value (some-> p-env ::sc/vwmem deref)]
      (is (= 42 (core/get-version vwmem-value))
          "sync processing-env must propagate wmem's ::version meta; JDBC store's optimistic locking depends on it"))))

(deftest async-processing-env-preserves-wmem-metadata-test
  (testing "async processing-env must preserve wmem's metadata (mirror of sync fix)"
    (let [chart       (chart/statechart {:initial :s1} (state {:id :s1}))
          registry    (mem-reg/new-registry)
          _           (sp/register-statechart! registry :test-chart chart)
          env         {::sc/statechart-registry registry}
          wmem        (core/attach-version {::sc/session-id :x ::sc/statechart-src :test-chart} 42)
          p-env       (async-impl/processing-env env :test-chart wmem)
          vwmem-value (some-> p-env ::sc/vwmem deref)]
      (is (= 42 (core/get-version vwmem-value))
          "async processing-env has the same merge-strips-meta bug as sync did; fulcro integration uses this path"))))

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
;; Finding #3 follow-up — start-invocation! must honour the InvocationProcessor
;; protocol when create-job! throws.
;; -----------------------------------------------------------------------------
;;
;; protocols.cljc docstring: start-invocation! "Returns `true` if the
;; invocation was successfully started, or `false` if the invocation failed to
;; start (e.g., chart not found, invalid function). Implementations SHOULD
;; send an :error.platform event to the parent session when returning `false`."
;;
;; With the v2.0.2 change to make create-job! throw on exhaustion, the
;; DurableJobInvocationProcessor stopped honouring this contract — the
;; exception leaked out past the SCXML processor instead of becoming an
;; error.platform event on the parent.

(deftest durable-job-start-invocation-survives-create-job-failure-test
  (testing "start-invocation! returns false and emits error.platform when create-job! throws"
    (let [sent-events (atom [])
          event-queue (reify sp/EventQueue
                        (send! [_ _env send-request]
                          (swap! sent-events conj send-request)
                          true)
                        (cancel! [_ _env _session-id _send-id] true)
                        (receive-events! [_ _env _handler] nil)
                        (receive-events! [_ _env _handler _options] nil))
          data-model  (wmdm/new-flat-model)
          session-id  :parent-session
          env         {::sc/session-id  session-id
                       ::sc/event-queue event-queue
                       ::sc/data-model  data-model
                       ::sc/vwmem       (volatile! {::sc/session-id session-id
                                                    ::sc/data-model {}})}
          processor   (durable-job/->DurableJobInvocationProcessor :fake-pool nil)]
      (with-redefs [job-store/create-job!
                    (fn [& _]
                      (throw (ex-info "create-job! exhausted race retries"
                                      {:session-id session-id :invokeid :test-invoke})))]
        (let [result (sp/start-invocation! processor env
                       {:invokeid :test-invoke
                        :src      :some-handler
                        :params   {:quiz-id 42}})]
          (is (false? result)
              "start-invocation! must return false on create-job! failure")
          (let [platform-errors (filter #(= :error.platform (:event %)) @sent-events)]
            (is (seq platform-errors)
                "start-invocation! must emit :error.platform to the parent session")
            (is (= session-id (:target (first platform-errors)))
                ":error.platform must target the parent session")))))))

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

;; -----------------------------------------------------------------------------
;; Batch 2 — 6 findings surfaced by the follow-up review (targeted at v2.0.4)
;; -----------------------------------------------------------------------------

;; -----------------------------------------------------------------------------
;; Finding #A (P1) — claim-events! loses FIFO order after UPDATE … RETURNING
;; -----------------------------------------------------------------------------
;;
;; `claim-events!` wraps the ORDER BY in a subquery, but PostgreSQL does NOT
;; preserve that order through UPDATE … RETURNING. Two ready events for the
;; same session can therefore be returned in arbitrary order, breaking the
;; queue's advertised FIFO semantics.

(deftest ^:integration claim-events-preserves-deliver-at-order-test
  (testing "events for the same session are returned in (deliver_at, id) order after UPDATE … RETURNING"
    (let [queue (pg-eq/new-queue *pool* "claim-fifo")
          session-id :fifo-session
          received (atom [])]
      ;; Insert 5 events in reverse delivery order so any planner-level
      ;; reordering inside UPDATE … RETURNING is observable.
      (doseq [i [5 4 3 2 1]]
        (sp/send! queue {}
                  {:event  (keyword (str "event-" i))
                   :target session-id
                   :data   {:ix i}}))
      (sp/receive-events! queue {}
                          (fn [_env event]
                            (swap! received conj (:name event))))
      ;; With a single claim of all 5 events, we should observe them in
      ;; insertion (== deliver_at) order regardless of UPDATE … RETURNING's
      ;; physical order.
      (is (= [:event-5 :event-4 :event-3 :event-2 :event-1] @received)
          "claim-events! must hand events to the handler in FIFO order"))))

;; -----------------------------------------------------------------------------
;; Finding #B (P1) — receive-events! keeps processing after a handler throws
;; -----------------------------------------------------------------------------
;;
;; After the catch block releases the failed event, the doseq continues and
;; later events in the same batch run BEFORE the released event is retried.
;; For a single session, that permanently reorders the event stream after any
;; transient handler error.

(deftest ^:integration receive-events-stops-processing-after-handler-failure-test
  (testing "later events in the same batch are not processed after an earlier event's handler throws"
    (let [queue (pg-eq/new-queue *pool* "fail-stops-batch")
          session-id :failure-order-session
          processed (atom [])
          attempts (atom 0)]
      (sp/send! queue {} {:event :first  :target session-id :data {}})
      (sp/send! queue {} {:event :second :target session-id :data {}})
      ;; First call: first-event handler throws. Second event MUST NOT run in
      ;; the same batch. receive-events! should stop and leave the remaining
      ;; events for the next claim cycle.
      (sp/receive-events! queue {}
                          (fn [_env event]
                            (swap! attempts inc)
                            (when (= :first (:name event))
                              (throw (ex-info "transient" {})))
                            (swap! processed conj (:name event))))
      (is (= [] @processed)
          ":second must not be processed while :first's released claim is pending retry")
      (is (= 1 @attempts)
          "handler is only called once per batch once a failure stops the loop"))))

;; -----------------------------------------------------------------------------
;; Finding #C (P2) — queue-depth throws on the :session-id filter
;; -----------------------------------------------------------------------------
;;
;; `(update :where conj [:= :target-session-id …])` turns `[:is :processed-at
;; nil]` into `[:is :processed-at nil [:= :target-session-id …]]`, which
;; HoneySQL formats as the illegal SQL `processed_at IS NULL IS (…)`.

(deftest ^:integration queue-depth-session-filter-is-valid-sql-test
  (testing "queue-depth with :session-id must format to valid SQL and return the correct count"
    (let [queue (pg-eq/new-queue *pool* "queue-depth-filter")
          s1 :qd-filter-s1
          s2 :qd-filter-s2]
      (doseq [i (range 3)]
        (sp/send! queue {} {:event :a :target s1 :data {:ix i}}))
      (doseq [i (range 2)]
        (sp/send! queue {} {:event :b :target s2 :data {:ix i}}))
      (is (= 3 (pg-eq/queue-depth *pool* {:session-id s1})))
      (is (= 2 (pg-eq/queue-depth *pool* {:session-id s2})))
      (is (= 5 (pg-eq/queue-depth *pool*))))))

;; -----------------------------------------------------------------------------
;; Finding #D (P2) — dispatch-terminal-event! drops :invoke-id
;; -----------------------------------------------------------------------------
;;
;; The InvocationProcessor contract depends on completion/error events
;; carrying the originating `:invoke-id` so `handle-external-invocations!` can
;; run finalize and autoforward. The worker's terminal dispatch omits it.

(deftest dispatch-terminal-event-includes-invoke-id-test
  (testing "terminal events emitted by the job worker carry the originating :invoke-id"
    (let [dispatch-fn @#'com.fulcrologic.statecharts.jobs.worker/dispatch-terminal-event!
          sent        (atom nil)
          event-queue (reify sp/EventQueue
                        (send! [_ _env send-request]
                          (reset! sent send-request)
                          true)
                        (cancel! [_ _env _sid _sendid] true)
                        (receive-events! [_ _env _h] nil)
                        (receive-events! [_ _env _h _opts] nil))
          job {:id (UUID/randomUUID)
               :session-id :job-parent
               :invokeid :durable-job-invocation
               :terminal-event-name ":done.invoke.durable-job-invocation"
               :terminal-event-data {:result 42}}]
      (dispatch-fn event-queue {} job {})
      (is (= :durable-job-invocation
             (or (:invoke-id @sent) (:invokeid @sent)))
          "terminal send! must include the invoke-id for finalize/autoforward handling"))))

;; -----------------------------------------------------------------------------
;; Finding #E (P2) — durable-job job-type loses keyword namespace
;; -----------------------------------------------------------------------------
;;
;; `start-invocation!` stores `job-type` via `(name src)`, so
;; `:my.app/send-email` becomes `"send-email"`. The worker resolves handlers
;; from `(keyword job-type)` / raw string — namespaced handler keys miss, and
;; two namespaces sharing the same local name would collide in storage.

(deftest durable-job-preserves-namespaced-src-test
  (testing "start-invocation! stores :src such that (keyword stored) round-trips namespaced keywords"
    (let [sent-type  (atom nil)
          event-queue (reify sp/EventQueue
                        (send! [_ _env _sr] true)
                        (cancel! [_ _env _sid _sendid] true)
                        (receive-events! [_ _env _h] nil)
                        (receive-events! [_ _env _h _opts] nil))
          data-model (wmdm/new-flat-model)
          env        {::sc/session-id :parent-ns
                      ::sc/event-queue event-queue
                      ::sc/data-model data-model
                      ::sc/vwmem (volatile! {::sc/session-id :parent-ns
                                             ::sc/data-model {}})}
          processor  (durable-job/->DurableJobInvocationProcessor :fake-pool nil)]
      (with-redefs [job-store/create-job!
                    (fn [_pool params]
                      (reset! sent-type (:job-type params))
                      (UUID/randomUUID))]
        (sp/start-invocation! processor env
                              {:invokeid :ns-invoke
                               :src      :my.app/send-email
                               :params   {}})
        (is (string? @sent-type))
        (is (= :my.app/send-email (keyword @sent-type))
            "(keyword stored-job-type) must round-trip to the original namespaced keyword")))))

;; -----------------------------------------------------------------------------
;; Finding #F (P2) — event_queue.event->row strips / crashes on non-string invoke-id
;; -----------------------------------------------------------------------------
;;
;; `(name invoke-id)` throws for UUID/numeric invoke IDs and strips namespaces
;; from keyword IDs. row->event hands back the raw string, so typed round-trip
;; is lost.

(deftest ^:integration event-invoke-id-roundtrip-preserves-type-test
  (testing ":invoke-id round-trips through the queue for keyword (namespaced), UUID, and numeric IDs"
    (let [queue    (pg-eq/new-queue *pool* "invoke-id-roundtrip")
          target   :invoke-id-session
          kw-id    :my.invoke/finalize-me
          uuid-id  (UUID/randomUUID)
          number-id 42
          received (atom [])]
      (doseq [iid [kw-id uuid-id number-id]]
        (sp/send! queue {} {:event :e :target target :invoke-id iid :data {}}))
      (sp/receive-events! queue {}
                          (fn [_env event]
                            (swap! received conj (:invokeid event))))
      (is (= [kw-id uuid-id number-id] @received)
          ":invoke-id must come back with its original type after the queue round-trip"))))

;; -----------------------------------------------------------------------------
;; Finding #F follow-up — legacy invoke-id rows must decode as bare strings
;; -----------------------------------------------------------------------------
;;
;; Pre-2.0.4 rows stored invoke-id via `(name x)` — e.g. a keyword :my-invoke
;; ended up as the bare string "my-invoke". The 2.0.4 decoder's
;; `(try (edn/read-string s) (catch Exception _ s))` doesn't fall back for bare
;; strings: `(edn/read-string "my-invoke")` returns the SYMBOL `my-invoke`, not
;; a string, so no exception fires. The pre-2.0.4 behaviour was to hand back
;; the raw string. A legacy-shaped row must still decode to the string.
(deftest ^:integration legacy-invoke-id-decodes-as-string-test
  (testing "rows written pre-2.0.4 with bare-string invoke-id still decode as strings"
    (let [queue     (pg-eq/new-queue *pool* "legacy-invoke-id")
          target-id :legacy-invoke-id-session
          received  (atom nil)]
      ;; Directly insert a row shaped like pre-2.0.4: invoke_id stored via
      ;; `(name x)` — no leading ':' or '"' or '#'. Bypass the queue's writer.
      (core/execute! *pool*
        {:insert-into :statechart-events
         :values [{:target-session-id (core/session-id->str target-id)
                   :source-session-id nil
                   :send-id           nil
                   :invoke-id         "legacy-bare-id"
                   :event-name        (pr-str :legacy-event)
                   :event-type        "external"
                   :event-data        (core/freeze {})}]})
      (sp/receive-events! queue {}
                          (fn [_env event] (reset! received event)))
      (is (string? (:invokeid @received))
          "legacy bare-string invoke-id must decode to a string, not a symbol")
      (is (= "legacy-bare-id" (:invokeid @received))
          "legacy invoke-id value must be preserved unchanged"))))
