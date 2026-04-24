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
   [com.fulcrologic.statecharts.jobs.worker :as worker]
   [com.fulcrologic.statecharts.persistence.jdbc :as pg-sc]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.event-queue :as pg-eq]
   [com.fulcrologic.statecharts.persistence.jdbc.job-store :as job-store]
   [com.fulcrologic.statecharts.persistence.jdbc.schema :as schema]
   [com.fulcrologic.statecharts.protocols :as sp]
   [com.fulcrologic.statecharts.registry.local-memory-registry :as mem-reg]
   [next.jdbc.connection :as jdbc.connection]
   [promesa.core :as p])
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
;; Finding #5 (MEDIUM) — superseded by #K
;; -----------------------------------------------------------------------------
;;
;; The original #5 fix added a getConnection probe fallback to treat
;; non-Hikari DataSources as "closed" when their connections throw. That's
;; wrong: a single failed getConnection call can't distinguish a permanent
;; shutdown from a transient outage. #K (below) inverts the expectation —
;; bare DataSources return false, and the caller can use `stop!` explicitly.

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
;; -----------------------------------------------------------------------------
;; Finding #G (P1) — invokeid->str strips the first char from non-keyword values
;; -----------------------------------------------------------------------------
;;
;; `(subs (str invokeid) 1)` only makes sense for keywords (where `(str :k)` has
;; a leading ':'). For string/UUID/number invoke-ids (allowed by ::sc/id and
;; possible via idlocation), this corrupts storage: "report" → "eport", 42 → "2".
;; The worker then builds done.invoke.*/error.invoke.* from the corrupted value
;; and the parent never receives its terminal event.

(deftest invokeid-roundtrip-preserves-all-supported-types-test
  (testing "invokeid->str / str->invokeid round-trip preserves value and type"
    ;; handle-external-invocations! matches terminal events to <invoke>
    ;; elements by `=` against the original idlocation value, so a string
    ;; invoke-id that's silently coerced to a keyword on readback will miss
    ;; finalize! / autoforward bookkeeping.
    (let [uuid   (UUID/randomUUID)
          inputs [:kw
                  :my-ns/kw
                  :42                      ; digit-starting keyword (ambiguous with Long)
                  :3.14                    ; digit-starting keyword (ambiguous with Double)
                  :-dashy                  ; sign-starting keyword (ambiguous with -number)
                  "report"
                  "looks-like-a/ns"
                  42
                  3.14
                  uuid]]
      (doseq [original inputs]
        (let [stored    (job-store/invokeid->str original)
              read-back (job-store/str->invokeid stored)]
          (is (= original read-back)
              (str "round-trip for " (pr-str original)
                   " must preserve value+type; stored as " (pr-str stored)
                   ", read back as " (pr-str read-back))))))))

;; -----------------------------------------------------------------------------
;; Finding #L (P1) — cancel-by-session! lost atomicity in 2.0.8
;; -----------------------------------------------------------------------------
;;
;; The 2.0.8 rewrite from a single UPDATE to SELECT + per-row UPDATE is
;; functionally correct for typed invokeids but drops atomicity: if the DB
;; fails mid-loop, some jobs end up cancelled and others don't. Cancellation
;; must be all-or-nothing.

(deftest ^:integration cancel-by-session-atomic-on-midway-failure-test
  (testing "cancel-by-session! is atomic — a mid-loop UPDATE failure rolls back every cancellation"
    (let [session-id :atomic-cancel-session
          job-ids (mapv (fn [i]
                          (let [job-id (UUID/randomUUID)]
                            (job-store/create-job! *pool*
                              {:id           job-id
                               :session-id   session-id
                               :invokeid     (keyword (str "inv-" i))
                               :job-type     "http"
                               :payload      {}
                               :max-attempts 3})
                            job-id))
                        (range 3))
          original-execute-sql! @#'core/execute-sql!
          update-call-count (atom 0)]
      (try
        (with-redefs [core/execute-sql!
                      (fn [ds sql & params]
                        (if (clojure.string/starts-with? sql "UPDATE statechart_jobs")
                          (do
                            (swap! update-call-count inc)
                            (if (= 2 @update-call-count)
                              (throw (ex-info "simulated DB error mid-loop" {}))
                              (apply original-execute-sql! ds sql params)))
                          (apply original-execute-sql! ds sql params)))]
          (is (thrown? Exception (job-store/cancel-by-session! *pool* session-id))
              "simulated DB error must propagate"))
        (catch Exception _ nil))
      ;; All three jobs should still be in their original state — nothing
      ;; partially cancelled.
      (let [statuses (mapv (fn [id]
                             (:status
                               (core/execute-sql-one! *pool*
                                 "SELECT status FROM statechart_jobs WHERE id = ?"
                                 [id])))
                           job-ids)]
        (is (every? #(= "pending" %) statuses)
            (str "all jobs must remain pending after failed cancel-by-session!; got " statuses))))))

;; -----------------------------------------------------------------------------
;; Finding #M (P2) — event_queue and job_store disagree on legacy bare rows
;; -----------------------------------------------------------------------------
;;
;; Pre-2.0.4 event_queue rows and pre-2.0.8 job_store rows both stored bare
;; keyword-typed invoke-ids (e.g. "my-invoke"). Post-fix decoders must agree —
;; both modules should reconstruct these as keywords so downstream matching
;; (handle-external-invocations! via `=`) works uniformly.

(deftest ^:integration event-queue-legacy-bare-invoke-id-decodes-as-keyword-test
  (testing "a bare-string invoke_id row in statechart_events decodes as a keyword via receive-events!"
    (let [queue     (pg-eq/new-queue *pool* "legacy-bare-ns-invoke-id")
          target-id :legacy-bare-invoke-session
          received  (atom nil)]
      ;; Directly insert a row shaped like pre-2.0.4: invoke_id stored via
      ;; `(name x)` — bare string, no leading marker.
      (core/execute! *pool*
        {:insert-into :statechart-events
         :values [{:target-session-id (core/session-id->str target-id)
                   :source-session-id nil
                   :send-id           nil
                   :invoke-id         "my-invoke"
                   :event-name        (pr-str :legacy-event)
                   :event-type        "external"
                   :event-data        (core/freeze {})}]})
      (sp/receive-events! queue {}
                          (fn [_env event] (reset! received event)))
      (is (keyword? (:invokeid @received))
          "bare legacy invoke_id should decode as a keyword — job_store's str->invokeid does the same, so handle-external-invocations! matches consistently")
      (is (= :my-invoke (:invokeid @received))
          "legacy :my-invoke stored bare must come back as :my-invoke (not the string \"my-invoke\")"))))

(deftest invokeid->str-preserves-non-keyword-values-test
  (testing "invokeid->str must not corrupt non-keyword invoke-ids (no leading-char stripping)"
    (is (not (= "eport" (job-store/invokeid->str "report")))
        "string invoke-id must not have first char stripped")
    (is (not (= "2" (job-store/invokeid->str 42)))
        "numeric invoke-id must not have first char stripped")
    (is (= "my-invoke" (job-store/invokeid->str :my-invoke))
        "keyword invoke-id serialization is preserved for back-compat (bare form)")
    (is (= "my-ns/my-invoke" (job-store/invokeid->str :my-ns/my-invoke))
        "namespaced keyword invoke-id serialization is preserved for back-compat")))

;; -----------------------------------------------------------------------------
;; Finding #H (P2) — invoke-data-keys throws on UUID/number invoke-ids
;; -----------------------------------------------------------------------------
;;
;; `(name invokeid)` only accepts strings/keywords/symbols. For a UUID or
;; numeric invoke-id (valid ::sc/id shapes), start-invocation! throws AFTER
;; create-job! has already inserted the pending row — leaving an orphaned job.

(deftest invoke-data-keys-handles-any-id-shape-test
  (testing "invoke-data-keys must accept all ::sc/id shapes without throwing"
    (doseq [iid ["report" 42 (UUID/randomUUID) :kw :my-ns/kw]]
      (let [{:keys [job-id-key job-kind-key]} (durable-job/invoke-data-keys iid)]
        (is (keyword? job-id-key)
            (str ":job-id-key must be a keyword for invokeid=" (pr-str iid)))
        (is (keyword? job-kind-key)
            (str ":job-kind-key must be a keyword for invokeid=" (pr-str iid)))))))

;; -----------------------------------------------------------------------------
;; Finding #H follow-up — invoke-data-keys must preserve the pre-2.0.6 key shape
;; for qualified-keyword invokeids
;; -----------------------------------------------------------------------------
;;
;; The 2.0.6 fix also re-shaped the qualified-keyword path from
;;   (name :my-ns/my-invoke)            = "my-invoke"            → :my-invoke/job-id
;; to
;;   (str (namespace x) "." (name x))   = "my-ns.my-invoke"       → :my-ns.my-invoke/job-id
;; This is a silent breaking change — any chart that reads its durable-job
;; data-model keys (guard/action code, RAD integration) would fail to find
;; the stored job-id after upgrade.
;;
;; Pin the pre-2.0.6 shape. The UUID/number crash fix is independent of this.

(deftest invoke-data-keys-qualified-keyword-shape-unchanged-test
  (testing "qualified-keyword invokeids produce the same data-model keys as pre-2.0.6"
    (let [{:keys [job-id-key job-kind-key]} (durable-job/invoke-data-keys :my-ns/my-invoke)]
      (is (= :my-invoke/job-id job-id-key)
          "qualified-keyword :job-id-key uses (name invokeid) as namespace — must not change")
      (is (= :my-invoke/job-kind job-kind-key)
          "qualified-keyword :job-kind-key uses (name invokeid) as namespace — must not change"))
    (let [{:keys [job-id-key]} (durable-job/invoke-data-keys :simple-invoke)]
      (is (= :simple-invoke/job-id job-id-key)
          "simple-keyword shape is stable"))))

;; -----------------------------------------------------------------------------
;; Finding #I follow-up — future invocation done send also carries :invoke-id
;; -----------------------------------------------------------------------------
;;
;; Preventive: #I fixed the error path; the done path was fixed in the same
;; change for consistency. Pin both.

(deftest future-invocation-done-event-includes-invoke-id-test
  (testing "future-backed invocation done.invoke.* send includes :invoke-id"
    (let [sent        (atom nil)
          event-queue (reify sp/EventQueue
                        (send! [_ _env send-request]
                          (reset! sent send-request)
                          true)
                        (cancel! [_ _env _sid _sendid] true)
                        (receive-events! [_ _env _h] nil)
                        (receive-events! [_ _env _h _opts] nil))
          env         {::sc/session-id  :future-parent
                       ::sc/event-queue event-queue
                       ::sc/vwmem       (volatile! {::sc/session-id :future-parent})}
          processor   ((requiring-resolve 'com.fulcrologic.statecharts.invocation.future/new-future-processor))
          ok-fn       (fn [_params] {:result :done})]
      (sp/start-invocation! processor env
        {:invokeid :my-future-invoke
         :src      ok-fn
         :params   {}})
      (let [deadline (+ (System/currentTimeMillis) 2000)]
        (while (and (nil? @sent) (< (System/currentTimeMillis) deadline))
          (Thread/sleep 10)))
      (is (some? @sent) "future invocation should have sent a done event")
      (is (= :my-future-invoke (or (:invoke-id @sent) (:invokeid @sent)))
          ":done.invoke.* send must carry :invoke-id for invocation matching"))))

;; -----------------------------------------------------------------------------
;; Finding #I (P2) — future invocation error send omits :invoke-id
;; -----------------------------------------------------------------------------
;;
;; handle-external-invocations! matches terminal events to <invoke> elements by
;; :invokeid. The error.invoke.* send from future.clj lacks it, so finalize
;; content and autoforward are skipped on the failure path.

(deftest future-invocation-error-event-includes-invoke-id-test
  (testing "future-backed invocation error.invoke.* send includes :invoke-id"
    (let [sent        (atom nil)
          event-queue (reify sp/EventQueue
                        (send! [_ _env send-request]
                          (reset! sent send-request)
                          true)
                        (cancel! [_ _env _sid _sendid] true)
                        (receive-events! [_ _env _h] nil)
                        (receive-events! [_ _env _h _opts] nil))
          env         {::sc/session-id   :future-parent
                       ::sc/event-queue  event-queue
                       ::sc/vwmem        (volatile! {::sc/session-id :future-parent})}
          processor   ((requiring-resolve 'com.fulcrologic.statecharts.invocation.future/new-future-processor))
          throwing-fn (fn [_params] (throw (ex-info "boom" {})))]
      (sp/start-invocation! processor env
        {:invokeid :my-future-invoke
         :src      throwing-fn
         :params   {}})
      ;; Give the future a moment to run & send its error event.
      (let [deadline (+ (System/currentTimeMillis) 2000)]
        (while (and (nil? @sent) (< (System/currentTimeMillis) deadline))
          (Thread/sleep 10)))
      (is (some? @sent) "future invocation should have sent an error event")
      (is (or (:invoke-id @sent) (:invokeid @sent))
          ":error.invoke.* send must carry :invoke-id for invocation matching"))))

;; -----------------------------------------------------------------------------
;; Finding #J (P2) — start-event-loop! hides the wake-signal from send!
;; -----------------------------------------------------------------------------
;;
;; env-with-signal is only visible inside start-event-loop!'s closure. Callers
;; keep their original env, so `pg/send! env …` sees `::wake-signal = nil` and
;; can't wake the loop. Same-JVM events still wait up to poll-interval-ms.

(deftest ^:integration start-event-loop-exposes-wake-signal-test
  (testing "start-event-loop! returns an env that carries the wake-signal"
    (let [env    (pg-sc/pg-env {:datasource *pool*})
          result (pg-sc/start-event-loop! env 10000)]
      (try
        (let [loop-env (or (:env result) env)
              signal   (or (::pg-sc/wake-signal loop-env)
                           (:wake-signal result))]
          (is (some? signal)
              "start-event-loop! must expose the wake-signal via :env (or equivalent) so send! can wake the loop"))
        (finally ((:stop! result)))))))

;; -----------------------------------------------------------------------------
;; Finding #K (P1) — datasource-closed? treats transient failures as permanent
;; -----------------------------------------------------------------------------
;;
;; For DataSources that don't implement isClosed(), any getConnection exception
;; is treated as a permanent shutdown. A transient outage or temporary pool
;; exhaustion will flip running=false and stop the event loop forever instead
;; of retrying on the next poll.

(deftest datasource-closed-does-not-flag-transient-failures-test
  (testing "datasource-closed? must not treat transient getConnection failures as permanent"
    (let [closed?-fn @#'pg-sc/datasource-closed?
          transient-ds (reify javax.sql.DataSource
                         (getConnection [_]
                           (throw (java.sql.SQLException. "connection refused — transient"))))]
      (is (false? (closed?-fn transient-ds))
          "bare DataSource without isClosed must NOT be reported as closed — we can't distinguish transient outages from shutdown"))))

;; (legacy-invoke-id-decodes-as-string-test from 2.0.5 superseded by finding
;; #M — the two decoders are now harmonised on keyword fallback for legacy
;; bare rows. See event-queue-legacy-bare-invoke-id-decodes-as-keyword-test.)

;; -----------------------------------------------------------------------------
;; Finding #N (P1) — future cancellation must not emit error.invoke.*
;; -----------------------------------------------------------------------------
;;
;; stop-invocation! on a future interrupts its thread. Blocking operations
;; inside the body (Thread/sleep, blocking I/O) surface InterruptedException
;; in the catch Throwable — which then emits an error.invoke.* send back to
;; the parent. Cancellation is a normal control-flow exit, not an invocation
;; failure; spurious errors can drive transitions that should never fire
;; after the state has already been left.

(deftest future-cancellation-does-not-emit-error-invoke-test
  (testing "future-cancel must not turn into an :error.invoke.* event"
    (let [sent        (atom [])
          event-queue (reify sp/EventQueue
                        (send! [_ _env req] (swap! sent conj req) true)
                        (cancel! [_ _env _sid _sendid] true)
                        (receive-events! [_ _env _h] nil)
                        (receive-events! [_ _env _h _opts] nil))
          env         {::sc/session-id  :future-cancel-parent
                       ::sc/event-queue event-queue
                       ::sc/vwmem       (volatile! {::sc/session-id :future-cancel-parent})}
          processor   ((requiring-resolve 'com.fulcrologic.statecharts.invocation.future/new-future-processor))
          started     (promise)
          slow-fn     (fn [_params]
                        (deliver started true)
                        (Thread/sleep 5000)
                        {:done :ok})]
      (sp/start-invocation! processor env
        {:invokeid :cancel-me :src slow-fn :params {}})
      (deref started 2000 :timeout)
      (sp/stop-invocation! processor env {:invokeid :cancel-me})
      ;; Give the future's catch block a moment to run.
      (Thread/sleep 300)
      (let [errors (filter (fn [req]
                             (let [evt (:event req)]
                               (and (keyword? evt)
                                    (clojure.string/starts-with? (name evt) "error.invoke"))))
                           @sent)]
        (is (empty? errors)
            (str "cancellation must not enqueue error.invoke.* events; got " (pr-str errors)))))))

;; -----------------------------------------------------------------------------
;; Finding #O (P2) — pg-env hardcodes the sync processor
;; -----------------------------------------------------------------------------
;;
;; pg-env accepts :data-model, :execution-model, :invocation-processors — but
;; installs (alg/new-processor) unconditionally. Paired with an async
;; execution model (lambda-async or any promise-returning expression runner),
;; the sync processor evaluates unresolved promises as truthy values — guards
;; take false transitions and the chart can finish before async work
;; resolves. Accept a caller-supplied :processor.

(deftest pg-env-accepts-custom-processor-test
  (testing "pg-env installs a caller-supplied :processor rather than forcing the sync algorithm"
    (let [custom-processor (reify sp/Processor
                             (start! [_ _ _ _] nil)
                             (exit! [_ _ _ _] nil)
                             (process-event! [_ _ _ _] nil))
          env (pg-sc/pg-env {:datasource :fake-pool
                             :processor  custom-processor})]
      (is (identical? custom-processor (::sc/processor env))
          "pg-env must honour :processor so async execution models can be paired with the async algorithm")))
  (testing "pg-env rejects a :processor that doesn't implement sp/Processor"
    (is (thrown? AssertionError
                 (pg-sc/pg-env {:datasource :fake-pool :processor "not-a-processor"}))
        "type-check should fire at construction time, not later when the chart tries to run")))

;; -----------------------------------------------------------------------------
;; Finding #Q (P2) — orphaned statechart_definitions table/DDL
;; -----------------------------------------------------------------------------
;;
;; JdbcStatechartRegistry was deleted in 2.0.10 but schema.clj still creates
;; the `statechart_definitions` table and the README still documents it.
;; Dead DDL + misleading docs: a fresh install creates an unused table.

(deftest ^:integration create-tables-does-not-create-definitions-table-test
  (testing "the statechart_definitions table is not created by create-tables! after registry removal"
    (schema/drop-tables! *pool*)
    (schema/create-tables! *pool*)
    (let [tables (into #{}
                       (map :tablename)
                       (core/execute-sql! *pool*
                         "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"))]
      (is (not (contains? tables "statechart_definitions"))
          (str "statechart_definitions should not be created — JdbcStatechartRegistry was deleted in 2.0.10. "
               "Tables present: " tables))
      (is (contains? tables "statechart_jobs")
          "sanity check: other tables still created")
      (is (contains? tables "statechart_events")))))

;; -----------------------------------------------------------------------------
;; Finding #R (P3) — future cancellation detection misses wrapped interrupts
;; -----------------------------------------------------------------------------
;;
;; If the future body catches InterruptedException internally and rethrows
;; a wrapping exception (e.g. ExecutionException, CompletionException, or a
;; domain-specific wrapper), the 2.0.10 catch only checks the top-level
;; exception type and misclassifies cancellation as a real failure.

(deftest future-cancellation-with-wrapped-interrupt-is-suppressed-test
  (testing "cancellation is suppressed even when the body wraps InterruptedException"
    (let [sent        (atom [])
          event-queue (reify sp/EventQueue
                        (send! [_ _env req] (swap! sent conj req) true)
                        (cancel! [_ _env _sid _sendid] true)
                        (receive-events! [_ _env _h] nil)
                        (receive-events! [_ _env _h _opts] nil))
          env         {::sc/session-id  :wrap-parent
                       ::sc/event-queue event-queue
                       ::sc/vwmem       (volatile! {::sc/session-id :wrap-parent})}
          processor   ((requiring-resolve 'com.fulcrologic.statecharts.invocation.future/new-future-processor))
          started     (promise)
          wrapping-fn (fn [_params]
                        (deliver started true)
                        (try
                          (Thread/sleep 5000)
                          {:unreachable true}
                          (catch InterruptedException e
                            ;; Wrap like many real codebases do — the raw
                            ;; InterruptedException is no longer visible at
                            ;; the top level.
                            (throw (java.util.concurrent.ExecutionException.
                                     "wrapping interrupt"
                                     e)))))]
      (sp/start-invocation! processor env
        {:invokeid :wrapped-cancel :src wrapping-fn :params {}})
      (deref started 2000 :timeout)
      (sp/stop-invocation! processor env {:invokeid :wrapped-cancel})
      (Thread/sleep 300)
      (let [errors (filter (fn [req]
                             (let [evt (:event req)]
                               (and (keyword? evt)
                                    (clojure.string/starts-with? (name evt) "error.invoke"))))
                           @sent)]
        (is (empty? errors)
            (str "cancellation wrapped in ExecutionException must not emit error.invoke.*; got "
                 (pr-str errors)))))))

;; -----------------------------------------------------------------------------
;; Finding #S (P1) — receive-events! ACKs async handlers before they finish
;; -----------------------------------------------------------------------------
;;
;; When a handler returns a Promesa promise (async processor, or any handler
;; that returns one), the current code calls mark-processed! immediately
;; without awaiting resolution. A crash after this point loses the event
;; permanently, and later events for the same session run against stale
;; working memory.

(deftest ^:integration receive-events-awaits-async-handler-promise-test
  (testing "receive-events! must not mark a row processed while its handler promise is pending; must mark processed on resolve; must release claim on reject"
    (let [queue (pg-eq/new-queue *pool* "async-ack-ok")
          target-id :async-ack-session
          gate      (promise)]
      (sp/send! queue {} {:event :happy :target target-id :data {}})
      (let [worker (future
                     (sp/receive-events! queue {}
                       (fn [_env _event]
                         (p/create
                           (fn [resolve _reject]
                             (future
                               @gate
                               (resolve :ok)))))))]
        ;; Give the handler time to start and block on the gate.
        (Thread/sleep 200)
        (let [row (core/execute-sql-one! *pool*
                    "SELECT processed_at FROM statechart_events WHERE target_session_id = ?"
                    [(core/session-id->str target-id)])]
          (is (nil? (:processed-at row))
              "while the handler's promise is pending, the row must not be marked processed"))
        (deliver gate :go)
        @worker
        (let [row (core/execute-sql-one! *pool*
                    "SELECT processed_at FROM statechart_events WHERE target_session_id = ?"
                    [(core/session-id->str target-id)])]
          (is (some? (:processed-at row))
              "once the handler's promise resolves, the row must be marked processed"))))

    ;; Reject path: the claim should be released (for retry) rather than marked processed.
    (let [queue (pg-eq/new-queue *pool* "async-ack-reject")
          target-id :async-ack-reject-session]
      (sp/send! queue {} {:event :sad :target target-id :data {}})
      (sp/receive-events! queue {}
        (fn [_env _event]
          (p/create
            (fn [_resolve reject]
              (reject (ex-info "handler promise rejected" {}))))))
      (let [row (core/execute-sql-one! *pool*
                  "SELECT processed_at, claimed_at FROM statechart_events WHERE target_session_id = ?"
                  [(core/session-id->str target-id)])]
        (is (nil? (:processed-at row))
            "a rejected handler promise must NOT be marked processed")
        (is (nil? (:claimed-at row))
            "a rejected handler promise must release its claim for retry")))))

;; -----------------------------------------------------------------------------
;; Finding #T (P2) — session-id round-trip mis-types strings that look like
;;                   UUIDs or keywords
;; -----------------------------------------------------------------------------
;;
;; session-id->str stores strings bare, but str->session-id eagerly parses
;; the bare form as UUID or keyword. A user that uses a string session-id
;; whose value happens to look like a UUID or keyword gets a different type
;; back — downstream `=` checks then fail.

(deftest session-id-roundtrip-preserves-type-test
  (testing "session-id->str / str->session-id round-trip preserves caller's type for all supported shapes"
    (let [uuid           (UUID/randomUUID)
          uuid-as-string (str (UUID/randomUUID))
          inputs [:kw
                  :my-ns/kw
                  uuid                             ; UUID
                  "just-a-string"                  ; plain string
                  uuid-as-string                   ; string that happens to look like a UUID
                  ":not-actually-a-keyword"        ; string that happens to look like a keyword
                  "42"]]                           ; numeric-looking string
      (doseq [original inputs]
        (let [stored    (core/session-id->str original)
              read-back (core/str->session-id stored)]
          (is (= original read-back)
              (str "round-trip for " (pr-str original)
                   " must preserve value+type; stored=" (pr-str stored)
                   ", read-back=" (pr-str read-back))))))))

;; -----------------------------------------------------------------------------
;; Finding #U (P2) — parse-event-type misdecodes quoted string types
;; -----------------------------------------------------------------------------
;;
;; SCXML transport types are strings (e.g. URIs). event->row stores them via
;; pr-str, producing `"\"http://…\""`. parse-event-type only EDN-reads values
;; starting with `:`, so the stored URI is rebuilt as the ugly keyword
;; `:"http://…"` instead of the original string.

(deftest ^:integration event-queue-string-type-roundtrips-as-string-test
  (testing "a string transport type round-trips as a string through the queue"
    (let [queue     (pg-eq/new-queue *pool* "string-event-type")
          received  (atom nil)
          target-id :string-type-session]
      (sp/send! queue {} {:event  :e
                          :type   "http://www.w3.org/tr/scxml"
                          :target target-id
                          :data   {}})
      (sp/receive-events! queue {}
                          (fn [_env event] (reset! received event)))
      (is (string? (:type @received))
          "quoted string type should round-trip as a string, not a keyword")
      (is (= "http://www.w3.org/tr/scxml" (:type @received))
          "round-trip value preserved"))))

;; -----------------------------------------------------------------------------
;; Finding #V (P2) — dispatch-terminal-event! ignores a nil session-state result
;; -----------------------------------------------------------------------------
;;
;; if-let fails when get-session-state-fn returns nil, and the code falls
;; through to the "no checker installed" branch and dispatches anyway.
;; A nil result should be treated as "not ready" — the guard is most
;; needed exactly when the parent is gone/stale.

(deftest dispatch-terminal-event-treats-nil-session-state-as-not-ready-test
  (testing "when get-session-state-fn returns nil, no terminal event is dispatched"
    (let [dispatch-fn @#'worker/dispatch-terminal-event!
          sent        (atom [])
          event-queue (reify sp/EventQueue
                        (send! [_ _env req] (swap! sent conj req) true)
                        (cancel! [_ _env _sid _sendid] true)
                        (receive-events! [_ _env _h] nil)
                        (receive-events! [_ _env _h _opts] nil))
          job {:id                  (UUID/randomUUID)
               :session-id          :gone-parent
               :invokeid            :some-invoke
               :terminal-event-name ":done.invoke.some-invoke"
               :terminal-event-data {}}
          always-nil-checker (fn [_sid _iid _job-id] nil)
          dispatched? (dispatch-fn event-queue {} job
                                   {:get-session-state-fn always-nil-checker})]
      (is (false? dispatched?)
          "a nil session-state result means 'not ready'; dispatch must be skipped")
      (is (empty? @sent)
          "no event should be sent when the parent is absent/stale"))))

;; -----------------------------------------------------------------------------
;; Finding #W (P1) — numeric session-id types must round-trip through JDBC
;; -----------------------------------------------------------------------------
;;
;; `::sc/id` allows numbers. Pre-fix: (session-id->str 42) = "42" bare;
;; str->session-id "42" = "42" STRING (not Long). Worker hydrates event with
;; :target "42", subsequent save-working-memory! then serializes that string
;; to "\"42\"" (quoted) and misses the row stored under "42". Event delivery
;; silently stops reaching the session.

(deftest numeric-session-id-round-trip-test
  (testing "numeric session-ids round-trip as numbers through session-id->str / str->session-id"
    (doseq [n [42 -1 0 (long 9999999999999) 3.14 -2.5]]
      (let [stored    (core/session-id->str n)
            read-back (core/str->session-id stored)]
        (is (= n read-back)
            (str "numeric session-id " (pr-str n)
                 " must round-trip as a number; stored=" (pr-str stored)
                 " read-back=" (pr-str read-back))))))
  (testing "strings that look like numbers stay strings (disambiguated via pr-str marker)"
    (doseq [s ["42" "-1" "3.14"]]
      (let [stored    (core/session-id->str s)
            read-back (core/str->session-id stored)]
        (is (= s read-back)
            (str "string session-id " (pr-str s) " must not be coerced to a number"))))))

;; -----------------------------------------------------------------------------
;; Finding #Y (P3) — all `number?` subtypes must round-trip, not just Long/Double
;; -----------------------------------------------------------------------------
;;
;; `::sc/id` is `[:or uuid? number? keyword? string?]`. The 2.0.13 fix covered
;; Long/Double but let BigInt (42N), BigDecimal (3.14M), and Ratio (1/2) degrade:
;; BigInt would narrow to Long, BigDecimal and Ratio would read back as strings.
;; `session-id->str` now uses pr-str for numbers (which preserves N / M / ratio
;; tags) and `str->session-id` recognises tagged numeric forms.

(deftest number-subtypes-round-trip-test
  (testing "BigInt / BigDecimal / Ratio session-ids preserve their type"
    (doseq [n [42N
               -42N
               (bigint "99999999999999999999999")
               3.14M
               -3.14M
               0.001M
               (/ 1 2)
               (/ -3 7)
               ;; Scientific-notation BigDecimals — Clojure's pr-str produces
               ;; `E+` / `E-` forms for values outside a narrow magnitude
               ;; window, so the decoder must accept those shapes too.
               1E+10M
               1E+100M
               1E-10M
               9.99E+50M
               1.5E-10M]]
      (let [stored    (core/session-id->str n)
            read-back (core/str->session-id stored)]
        (is (= n read-back)
            (str "numeric session-id " (pr-str n)
                 " (" (.getSimpleName (class n)) ") must round-trip with its type;"
                 " stored=" (pr-str stored)
                 " read-back=" (pr-str read-back)
                 " (" (some-> read-back class .getSimpleName) ")"))
        (is (= (class n) (class read-back))
            (str "type of read-back for " (pr-str n)
                 " must equal input type; got " (pr-str (class read-back)))))))

  (testing "legacy bare non-numeric strings still decode as strings (no false-positive EDN read)"
    (doseq [s ["my-session" "legacy-bare" "foo-bar-baz"]]
      (is (= s (core/str->session-id s))
          (str "bare string " (pr-str s) " must not be misread as a symbol by EDN"))))

  (testing "trailing-garbage after tagged-numeric shape does not pass the regex gate"
    ;; If someone future-refactors re-matches → re-find, the gate's safety
    ;; margin disappears silently. Pin that trailing junk stays string so
    ;; that refactor fails loudly.
    (doseq [s ["42Nabc" "1/2xyz" "3.14Mfoo" "1E+10Mbar"]]
      (is (= s (core/str->session-id s))
          (str "input " (pr-str s) " has trailing junk; must not decode as a number"))))

  (testing "legacy pre-2.0.14 BigInt rows (stored via (str n) as bare \"42\") decode as Long"
    ;; Pre-2.0.14, session-id->str's :else branch produced (str 42N) = "42"
    ;; (no N marker). Post-2.0.14 those legacy rows decode as Long 42, not
    ;; BigInt 42N — a silent type narrow. Documented in CHANGELOG 2.0.15.
    (let [legacy-bigint-row "42"]
      (is (= 42 (core/str->session-id legacy-bigint-row))
          "pre-2.0.14 BigInt row decodes as Long (documented migration)")
      (is (= Long (class (core/str->session-id legacy-bigint-row)))
          "class is Long, not BigInt — users relying on class identity must re-save"))))
