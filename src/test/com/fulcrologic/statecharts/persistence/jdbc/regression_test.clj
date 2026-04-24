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
   [com.fulcrologic.statecharts.data-model.operations :as ops]
   [com.fulcrologic.statecharts.data-model.working-memory-data-model :as wmdm]
   [com.fulcrologic.statecharts.elements :refer [state]]
   [com.fulcrologic.statecharts.invocation.durable-job :as durable-job]
   [com.fulcrologic.statecharts.invocation.statechart :as i.statechart]
   [com.fulcrologic.statecharts.jobs.worker :as worker]
   [com.fulcrologic.statecharts.persistence.jdbc :as pg-sc]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.event-queue :as pg-eq]
   [com.fulcrologic.statecharts.persistence.jdbc.fixtures :as fixtures :refer [*pool*]]
   [com.fulcrologic.statecharts.persistence.jdbc.job-store :as job-store]
   [com.fulcrologic.statecharts.persistence.jdbc.schema :as schema]
   [com.fulcrologic.statecharts.persistence.jdbc.working-memory-store :as pg-wms]
   [com.fulcrologic.statecharts.protocols :as sp]
   [com.fulcrologic.statecharts.registry.local-memory-registry :as mem-reg]
   [promesa.core :as p])
  (:import
   [java.util UUID]))

(use-fixtures :once fixtures/with-pool)
(use-fixtures :each fixtures/with-clean-tables)

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
;; Finding #D (P2) — worker's terminal send must propagate :invoke-id
;; -----------------------------------------------------------------------------
;;
;; The InvocationProcessor contract depends on completion/error events
;; carrying the originating `:invoke-id` so `handle-external-invocations!` can
;; run finalize and autoforward. Worker dispatch used to omit it.

(deftest worker-terminal-send-includes-invoke-id-test
  (testing "terminal events emitted by the job worker carry the originating :invoke-id"
    (let [;; 2.0.20 split `dispatch-terminal-event!` into a readiness
          ;; check + a send; the send is what this test pins.
          send-fn     @#'com.fulcrologic.statecharts.jobs.worker/send-terminal-event!
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
      (send-fn event-queue {} job {})
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
;; Finding #V (P2) — readiness check treats nil session-state as "not ready"
;; -----------------------------------------------------------------------------
;;
;; if-let fails when get-session-state-fn returns nil, and the code fell
;; through to the "no checker installed" branch and dispatched anyway.
;; A nil result must be treated as "not ready" — the guard is most
;; needed exactly when the parent is gone/stale.

(deftest session-ready-for-dispatch-treats-nil-as-not-ready-test
  (testing "when get-session-state-fn returns nil, the readiness check returns false"
    (let [;; 2.0.20 moved the nil-is-not-ready guard into a pure
          ;; `session-ready-for-dispatch?` helper. Pinned here.
          ready? @#'worker/session-ready-for-dispatch?
          job {:id                  (UUID/randomUUID)
               :session-id          :gone-parent
               :invokeid            :some-invoke
               :terminal-event-name ":done.invoke.some-invoke"
               :terminal-event-data {}}
          always-nil-checker (fn [_sid _iid _job-id] nil)]
      (is (false? (ready? {:get-session-state-fn always-nil-checker} job))
          "a nil session-state result means 'not ready'")
      (is (true? (ready? {:get-session-state-fn (fn [_ _ _] {:ready true})} job))
          "a :ready true result means 'ready'")
      (is (false? (ready? {:get-session-state-fn (fn [_ _ _] {:ready false})} job))
          "a :ready false result means 'not ready'")
      (is (true? (ready? {} job))
          "no checker installed at all → dispatch unconditionally"))))

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

;; -----------------------------------------------------------------------------
;; Finding #Z1 (P2) — tagged numeric invoke-ids mis-decoded in event_queue
;; -----------------------------------------------------------------------------
;;
;; event->row persists :invoke-id via pr-str, so 42N, 3.14M, 1/2 are stored
;; as tagged numeric literals. parse-invoke-id only reads plain integers and
;; decimals; tagged forms fall through to (keyword s), and the handler sees
;; :42N / :3.14M / :1/2 instead of the original number. finalize/autoforward
;; matching in handle-external-invocations! uses = against the original
;; idlocation value, so those queued terminal events silently skip bookkeeping.
;; Same fallback regex should match core's tagged-number-re.

(deftest parse-invoke-id-accepts-tagged-numeric-literals-test
  (let [parse @#'pg-eq/parse-invoke-id]
    (testing "BigInt invokeids round-trip via pr-str → parse-invoke-id"
      (doseq [n [42N -42N 0N (bigint "99999999999999999999999")]]
        (let [stored    (pr-str n)
              read-back (parse stored)]
          (is (= n read-back)
              (str "BigInt invokeid " (pr-str n) " must decode as BigInt;"
                   " stored=" (pr-str stored) " read-back=" (pr-str read-back)))
          (is (= (class n) (class read-back))
              (str "class must be preserved; got " (pr-str (class read-back)))))))

    (testing "BigDecimal invokeids round-trip"
      (doseq [n [3.14M -3.14M 0.001M 1E+10M]]
        (let [stored    (pr-str n)
              read-back (parse stored)]
          (is (= n read-back)
              (str "BigDecimal invokeid " (pr-str n) " must decode as BigDecimal;"
                   " read-back=" (pr-str read-back))))))

    (testing "Ratio invokeids round-trip"
      (doseq [n [(/ 1 2) (/ -3 7)]]
        (let [stored    (pr-str n)
              read-back (parse stored)]
          (is (= n read-back)
              (str "Ratio invokeid " (pr-str n) " must decode as Ratio;"
                   " read-back=" (pr-str read-back))))))

    (testing "legacy bare-keyword rows still decode as keywords"
      (is (= :legacy (parse "legacy"))
          "bare non-numeric strings still decode as keywords (legacy invariant)"))))

;; -----------------------------------------------------------------------------
;; Finding #Z2 (P2) — tagged numeric invoke-ids mis-decoded in job_store
;; -----------------------------------------------------------------------------
;;
;; invokeid->str writes non-keyword invoke-ids with pr-str, but str->invokeid
;; only reconstructs UUIDs, quoted strings, and plain longs/doubles. Numeric
;; invoke-ids such as 42N, 3.14M, 1/2 therefore hydrate as keywords and the
;; worker dispatches done.invoke.* / error.invoke.* against the wrong id.
;; handle-external-invocations! compares the decoded id by = and the parent
;; chart never sees the completion event.

(deftest str->invokeid-accepts-tagged-numeric-literals-test
  (testing "BigInt / BigDecimal / Ratio invokeids round-trip through invokeid->str / str->invokeid"
    (doseq [n [42N -42N 0N 3.14M -3.14M 0.001M 1E+10M (/ 1 2) (/ -3 7)]]
      (let [stored    (job-store/invokeid->str n)
            read-back (job-store/str->invokeid stored)]
        (is (= n read-back)
            (str "invokeid " (pr-str n) " must round-trip as its original type;"
                 " stored=" (pr-str stored) " read-back=" (pr-str read-back)))
        (is (= (class n) (class read-back))
            (str "class must be preserved; got " (pr-str (class read-back)))))))

  (testing "plain Long/Double still round-trip (no regression)"
    (doseq [n [42 -1 0 3.14 -2.5]]
      (is (= n (job-store/str->invokeid (job-store/invokeid->str n)))
          (str "plain numeric invokeid " (pr-str n) " still round-trips"))))

  (testing "keyword invokeids still round-trip"
    (doseq [k [:kw :my-ns/kw :42]]
      (is (= k (job-store/str->invokeid (job-store/invokeid->str k)))
          (str "keyword invokeid " (pr-str k) " still round-trips")))))

;; -----------------------------------------------------------------------------
;; Finding #Z3 (P1) — event ACK is not atomic with working-memory persistence
;; -----------------------------------------------------------------------------
;;
;; receive-events! marks the row processed AFTER handler returns, in a
;; separate transaction. Between save-working-memory! (inside handler) and
;; mark-processed! there is a window: a crash / DB error / connection loss
;; leaves the WM advanced but the event not acked. Stale-claim recovery
;; re-delivers the event → handler runs a second time against advanced
;; state → non-idempotent actions run twice → "exactly-once" is violated.
;;
;; Fix: save-working-memory! runs pre-commit hooks from env inside its own
;; transaction, so the queue can hand it a `mark-processed!` hook that
;; commits atomically with the WM update.

(deftest ^:integration save-working-memory-runs-on-save-hooks-test
  (let [chart    (chart/statechart {:initial :s1} (state {:id :s1}))
        registry (mem-reg/new-registry)
        _        (sp/register-statechart! registry :test-chart chart)
        store    (pg-wms/new-store *pool*)
        env      {::sc/statechart-registry registry}
        wmem     (core/attach-version {::sc/session-id :ack-atomic-session
                                       ::sc/statechart-src :test-chart
                                       ::sc/configuration #{:s1}
                                       ::sc/running? true} nil)]

    (testing "save-working-memory! accepts and runs ::sc/on-save-hooks inside its tx"
      (let [hook-called? (atom false)
            hook-tx      (atom nil)
            hook         (fn [tx]
                           (reset! hook-called? true)
                           (reset! hook-tx tx))
            env+hooks    (assoc env ::sc/on-save-hooks [hook])]
        (sp/save-working-memory! store env+hooks :ack-atomic-session wmem)
        (is @hook-called?
            "on-save hook must fire during save-working-memory!")
        (is (some? @hook-tx)
            "hook must receive a JDBC transaction/connection so it can participate in the save's tx")))

    (testing "a throwing hook rolls back the WM write (atomicity)"
      ;; Read current version first so we can assert it DOESN'T change.
      (let [wmem-before  (sp/get-working-memory store env :ack-atomic-session)
            throwing-hook (fn [_tx] (throw (ex-info "simulated mid-commit failure" {})))
            env+bad-hook  (assoc env ::sc/on-save-hooks [throwing-hook])
            next-wmem     (assoc wmem-before ::sc/configuration #{:s2})]
        (is (thrown? Exception
              (sp/save-working-memory! store env+bad-hook :ack-atomic-session next-wmem))
            "hook throw must propagate")
        (let [wmem-after (sp/get-working-memory store env :ack-atomic-session)]
          (is (= (::sc/configuration wmem-before) (::sc/configuration wmem-after))
              "WM must not have advanced — hook throw inside the tx must roll back the save"))))))

;; -----------------------------------------------------------------------------
;; Finding #Z4 (P1) — StatechartInvocationProcessor regresses explicit child-session-id
;; -----------------------------------------------------------------------------
;;
;; 7d018d7 unconditionally scopes every child session to <parent>.<invokeid>.
;; That breaks the documented :child-session-id option in routing.cljc: a
;; route configured with :child-session-id "admin" no longer creates a child
;; at "admin" (the invoke element's :id becomes the invokeid, which is then
;; scoped). Code that addresses the child directly by its explicit session-id
;; (scf/send!, URL sync, restoring an existing child) stops finding it.
;;
;; Fix: when the algorithm marks the invocation as `:explicit-id? true`
;; (user provided :id on the invoke element), the processor uses invokeid
;; directly as the session-id. Auto-generated invokeids remain scoped.

(deftest statechart-invocation-preserves-explicit-session-id-test
  (let [sent-events (atom [])
        saved-wms   (atom {})
        event-queue (reify sp/EventQueue
                      (send! [_ _env req] (swap! sent-events conj req) true)
                      (cancel! [_ _env _sid _sendid] true)
                      (receive-events! [_ _env _h] nil)
                      (receive-events! [_ _env _h _opts] nil))
        registry    (mem-reg/new-registry)
        _           (sp/register-statechart! registry :child-chart
                      (chart/statechart {:initial :c1} (state {:id :c1})))
        wm-store    (reify sp/WorkingMemoryStore
                      (get-working-memory [_ _env sid] (get @saved-wms sid))
                      (save-working-memory! [_ _env sid wmem]
                        (swap! saved-wms assoc sid wmem)
                        true)
                      (delete-working-memory! [_ _env sid]
                        (swap! saved-wms dissoc sid)
                        true))
        processor   (reify sp/Processor
                      (start! [_ _env _src {::sc/keys [session-id] :as _data}]
                        {::sc/session-id session-id
                         ::sc/configuration #{:c1}})
                      (process-event! [_ _env wmem _event] wmem)
                      (exit! [_ _env _wmem _terminal?] nil))
        inv-proc    (i.statechart/new-invocation-processor)
        parent-sid  :parent-sess
        env         {::sc/session-id            parent-sid
                     ::sc/vwmem                 (volatile! {::sc/session-id parent-sid})
                     ::sc/event-queue           event-queue
                     ::sc/working-memory-store  wm-store
                     ::sc/processor             processor
                     ::sc/statechart-registry   registry}]

    (testing "explicit-id? → child session stored at invokeid, not at scoped form"
      (sp/start-invocation! inv-proc env
        {:invokeid     "admin"
         :src          :child-chart
         :params       {}
         :explicit-id? true})
      (is (contains? @saved-wms "admin")
          "child session must be stored under the user-supplied id \"admin\"")
      (is (not (contains? @saved-wms (str parent-sid ".admin")))
          "must NOT create the scoped form when id is explicit"))

    (testing "no explicit-id? → child session stored at scoped form (default behaviour preserved)"
      (reset! saved-wms {})
      (sp/start-invocation! inv-proc env
        {:invokeid "auto-gen.abc-123"
         :src      :child-chart
         :params   {}})
      (is (contains? @saved-wms (str parent-sid "." "auto-gen.abc-123"))
          "without explicit-id? the child is scoped to parent to avoid collisions")
      (is (not (contains? @saved-wms "auto-gen.abc-123"))
          "must NOT store at the bare invokeid"))

    (testing "stop-invocation! finds the explicit-id child"
      ;; set up an explicit-id child
      (reset! saved-wms {"admin" {::sc/session-id "admin"
                                  ::sc/configuration #{:c1}
                                  ::sc/running? true}})
      (sp/stop-invocation! inv-proc env
        {:invokeid     "admin"
         :explicit-id? true})
      (is (not (contains? @saved-wms "admin"))
          "stop-invocation! must delete the explicit-id child by its bare id"))))

;; -----------------------------------------------------------------------------
;; Finding #Z5 — 2.0.17 consolidation must preserve prior per-site decoder behavior
;; -----------------------------------------------------------------------------
;;
;; The 2.0.17 refactor introduced a shared `decode-id` with a uniform `#`-branch
;; and a uniform `parse-uuid` step. That was a behavior change for two edge
;; cases in legacy data:
;;   (1) pre-2.0.11 bare session-id rows starting with `#` (e.g. a string
;;       session-id literally called `"#foo"` or `"#{1 2 3}"`) would previously
;;       return as String — now they'd EDN-read and potentially decode as a
;;       set / other tagged literal / UUID.
;;   (2) pre-2.0.8 bare invoke-id rows that happened to be UUID-shaped (from a
;;       keyword whose name was UUID-shaped, passed through `(name x)`) would
;;       previously decode as a keyword — now they'd decode as a UUID.
;; Both are preserved by gating the `#`-branch and `parse-uuid` on
;; `:legacy-fallback`: `:string` (session-id) enables parse-uuid but not
;; `#`-read; `:keyword` (invoke-id) enables `#`-read but not parse-uuid.

(deftest decode-id-preserves-session-id-legacy-behavior-test
  (testing "pre-2.0.11 bare session-id rows starting with `#` do NOT EDN-read"
    ;; Rare but legal per ::sc/id: a string session-id whose content happens
    ;; to start with `#`. Pre-2.0.17 the session-id decoder had no `#` branch
    ;; so these fell through to the string-fallback. Post-2.0.17 they must
    ;; continue to decode as strings.
    (is (= "#foo" (core/str->session-id "#foo"))
        "bare `#foo` must stay a string (legacy pre-2.0.11 row)")
    (is (= "#{1 2 3}" (core/str->session-id "#{1 2 3}"))
        "bare `#{...}` must stay a string, not decode as a Clojure set")
    (is (= "#uuid \"550e8400-e29b-41d4-a716-446655440000\""
           (core/str->session-id "#uuid \"550e8400-e29b-41d4-a716-446655440000\""))
        "even UUID-tagged-literal-looking bare content stays as string — session-id writer never produced `#` forms"))

  (testing "nil session-id input returns nil (was \"\" via (str nil) before 2.0.17 — pinned)"
    (is (nil? (core/session-id->str nil))
        "nil in → nil out. Pre-2.0.17 (str nil) produced \"\", which silently wrote an empty-string session-id row. nil now propagates instead so the bug surfaces at the NOT NULL constraint.")))

(deftest decode-id-preserves-invoke-id-legacy-behavior-test
  (testing "pre-2.0.8 bare UUID-shaped invoke-id rows decode as keyword, not UUID"
    ;; Pre-2.0.8 `invokeid->str` used `(name x)` which only supported
    ;; keywords/strings/symbols. A keyword with a UUID-shaped name, e.g.
    ;; `(keyword "550e8400-e29b-41d4-a716-446655440000")`, would be stored as
    ;; the bare UUID-shaped string. On readback the decoder must honour the
    ;; original keyword shape so `handle-external-invocations!` matches the
    ;; running invocation by `=` against the keyword idlocation.
    (let [uuid-shaped-bare "550e8400-e29b-41d4-a716-446655440000"]
      (is (= (keyword uuid-shaped-bare) (job-store/str->invokeid uuid-shaped-bare))
          "UUID-shaped bare row must decode as keyword (pre-2.0.8 `(name :kw)` legacy)")))

  (testing "post-2.0.8 #uuid-tagged invoke-id rows still decode as UUIDs"
    ;; Normal path: writer uses pr-str → `#uuid "..."`. Decoder must read it.
    (let [u (random-uuid)
          stored (pr-str u)]
      (is (= u (job-store/str->invokeid stored))
          "`#uuid \"...\"` tagged literal decodes back to the UUID"))))

;; -----------------------------------------------------------------------------
;; Direct tests for the shared encode-id / decode-id option matrix
;; -----------------------------------------------------------------------------

(deftest encode-id-option-matrix-test
  (testing ":uuid-shape :bare vs :tagged"
    (let [u (random-uuid)]
      (is (= (str u) (core/encode-id u {:uuid-shape :bare}))
          ":bare produces just the UUID string")
      (is (= (pr-str u) (core/encode-id u {:uuid-shape :tagged}))
          ":tagged produces #uuid \"...\" form")))

  (testing ":keyword-shape :marked vs :bare-unless-numeric"
    (is (= ":kw" (core/encode-id :kw {:keyword-shape :marked}))
        ":marked → leading colon via pr-str")
    (is (= "kw" (core/encode-id :kw {:keyword-shape :bare-unless-numeric}))
        ":bare-unless-numeric → bare name for non-numeric keyword")
    (is (= ":42" (core/encode-id :42 {:keyword-shape :bare-unless-numeric}))
        ":bare-unless-numeric → marked form for numeric-name keyword (disambiguates from number)")
    (is (= "ns/kw" (core/encode-id :ns/kw {:keyword-shape :bare-unless-numeric}))
        "namespaced keywords keep their namespace in bare form (leading `:` stripped)"))

  (testing ":symbol-shape :marked vs :bare-unless-numeric"
    (is (= "sym" (core/encode-id 'sym {:symbol-shape :marked}))
        "pr-str of a symbol has no marker — same as bare")
    (is (= "sym" (core/encode-id 'sym {:symbol-shape :bare-unless-numeric}))
        "bare name for non-numeric symbol"))

  (testing "nil input returns nil"
    (is (nil? (core/encode-id nil {})))
    (is (nil? (core/encode-id nil {:uuid-shape :tagged})))))

(deftest decode-id-option-matrix-test
  (testing ":legacy-fallback :string enables parse-uuid, not #-read"
    (let [opts {:legacy-fallback :string}]
      (is (= "foo" (core/decode-id "foo" opts))
          "bare non-numeric → string")
      (is (= (java.util.UUID/fromString "550e8400-e29b-41d4-a716-446655440000")
             (core/decode-id "550e8400-e29b-41d4-a716-446655440000" opts))
          ":string mode parses bare UUIDs")
      (is (= "#foo" (core/decode-id "#foo" opts))
          ":string mode does NOT try EDN-read on `#` — legacy bare content stays a string")
      (is (= "#{1 2}" (core/decode-id "#{1 2}" opts))
          ":string mode rejects set literals — would otherwise silently decode as a Clojure set")))

  (testing ":legacy-fallback :keyword enables #-read, not parse-uuid"
    (let [opts {:legacy-fallback :keyword}]
      (is (= :foo (core/decode-id "foo" opts))
          "bare non-numeric → keyword")
      (is (= (keyword "550e8400-e29b-41d4-a716-446655440000")
             (core/decode-id "550e8400-e29b-41d4-a716-446655440000" opts))
          ":keyword mode does NOT parse-uuid — pre-2.0.8 bare UUID-shaped row stays a keyword")
      (let [u (random-uuid)]
        (is (= u (core/decode-id (pr-str u) opts))
            ":keyword mode DOES decode #uuid-tagged literal"))))

  (testing "numeric parsers run in both modes"
    (doseq [opts [{:legacy-fallback :string} {:legacy-fallback :keyword}]]
      (is (= 42 (core/decode-id "42" opts)))
      (is (= 3.14 (core/decode-id "3.14" opts)))
      (is (= 42N (core/decode-id "42N" opts)))
      (is (= 3.14M (core/decode-id "3.14M" opts)))
      (is (= (/ 1 2) (core/decode-id "1/2" opts)))))

  (testing "marked literals (leading `:` / `\"`) decode in both modes"
    (doseq [opts [{:legacy-fallback :string} {:legacy-fallback :keyword}]]
      (is (= :ns/kw (core/decode-id ":ns/kw" opts)))
      (is (= "hello" (core/decode-id "\"hello\"" opts)))))

  (testing "nil input returns nil"
    (is (nil? (core/decode-id nil {:legacy-fallback :string})))
    (is (nil? (core/decode-id nil {:legacy-fallback :keyword})))))

;; -----------------------------------------------------------------------------
;; Finding #AA (P1) — child-session saves must not run parent's ACK hooks
;; -----------------------------------------------------------------------------
;;
;; When the JDBC event queue processes a parent event it adds
;; `::sc/on-save-hooks` to env so the parent event is ACKed atomically with
;; the parent WM save. `start-invocation!` reuses the same env to save the
;; CHILD session's WM — so entering a `:statechart` invoke marks the parent
;; event processed as soon as the child's first save commits, BEFORE the
;; parent actually saves. If the parent save later fails (e.g. optimistic
;; lock conflict), the claim is released but the row is already acked →
;; parent event is lost while the child chart has been started.

(deftest statechart-invocation-strips-parent-ack-hooks-from-child-save-test
  (let [hook-calls (atom [])
        saved-wms  (atom {})
        event-queue (reify sp/EventQueue
                      (send! [_ _env _req] true)
                      (cancel! [_ _env _sid _sendid] true)
                      (receive-events! [_ _env _h] nil)
                      (receive-events! [_ _env _h _opts] nil))
        registry    (mem-reg/new-registry)
        _           (sp/register-statechart! registry :child-chart
                      (chart/statechart {:initial :c1} (state {:id :c1})))
        wm-store    (reify sp/WorkingMemoryStore
                      (get-working-memory [_ _env sid] (get @saved-wms sid))
                      (save-working-memory! [_ env sid wmem]
                        ;; Capture any on-save hooks that would fire during this save.
                        (when-let [hooks (:com.fulcrologic.statecharts/on-save-hooks env)]
                          (doseq [h hooks] (h :fake-tx))
                          (swap! hook-calls conj {:session-id sid :count (count hooks)}))
                        (swap! saved-wms assoc sid wmem)
                        true)
                      (delete-working-memory! [_ _env sid]
                        (swap! saved-wms dissoc sid)
                        true))
        processor   (reify sp/Processor
                      (start! [_ _env _src {::sc/keys [session-id]}]
                        {::sc/session-id session-id ::sc/configuration #{:c1}})
                      (process-event! [_ _env wmem _event] wmem)
                      (exit! [_ _env _wmem _terminal?] nil))
        inv-proc    (i.statechart/new-invocation-processor)
        parent-sid  :parent-session
        parent-hook-calls (atom 0)
        parent-hook (fn [_tx] (swap! parent-hook-calls inc))
        ;; Simulate the env the JDBC event queue hands to the parent handler:
        ;; it includes the parent-event ACK hook.
        env         {::sc/session-id            parent-sid
                     ::sc/vwmem                 (volatile! {::sc/session-id parent-sid})
                     ::sc/event-queue           event-queue
                     ::sc/working-memory-store  wm-store
                     ::sc/processor             processor
                     ::sc/statechart-registry   registry
                     ::sc/on-save-hooks         [parent-hook]}]

    (sp/start-invocation! inv-proc env
      {:invokeid :my-child
       :src      :child-chart
       :params   {}})

    (testing "the parent's ACK hook MUST NOT fire when the child session saves"
      (is (zero? @parent-hook-calls)
          "parent on-save hook must not fire during child save — if it does, the parent event is ACKed before the parent save commits, so a later parent save failure silently loses the event")
      (let [child-session-id (str parent-sid "." (str :my-child))
            child-hook-calls (filter #(= (:session-id %) child-session-id) @hook-calls)]
        (is (empty? child-hook-calls)
            "child save must not carry parent's on-save hooks into its env at all")))))

(deftest statechart-invocation-async-path-strips-parent-ack-hooks-test
  ;; Same contract as the sync path above, but exercises the branch
  ;; where `sp/start!` returns a Promesa promise and `save!` runs inside
  ;; a `p/then` continuation. The parent's hook must still not fire
  ;; when the async child save resolves.
  (let [saved-wms   (atom {})
        event-queue (reify sp/EventQueue
                      (send! [_ _env _req] true)
                      (cancel! [_ _env _sid _sendid] true)
                      (receive-events! [_ _env _h] nil)
                      (receive-events! [_ _env _h _opts] nil))
        registry    (mem-reg/new-registry)
        _           (sp/register-statechart! registry :child-chart
                      (chart/statechart {:initial :c1} (state {:id :c1})))
        wm-store    (reify sp/WorkingMemoryStore
                      (get-working-memory [_ _env sid] (get @saved-wms sid))
                      (save-working-memory! [_ env sid wmem]
                        (when-let [hooks (:com.fulcrologic.statecharts/on-save-hooks env)]
                          (doseq [h hooks] (h :fake-tx)))
                        (swap! saved-wms assoc sid wmem)
                        true)
                      (delete-working-memory! [_ _env sid]
                        (swap! saved-wms dissoc sid)
                        true))
        async-processor (reify sp/Processor
                          ;; `start!` returns a Promesa promise. The
                          ;; invocation processor pipes its result into
                          ;; `save!` via `p/then`.
                          (start! [_ _env _src {::sc/keys [session-id]}]
                            (p/resolved {::sc/session-id session-id ::sc/configuration #{:c1}}))
                          (process-event! [_ _env wmem _event] wmem)
                          (exit! [_ _env _wmem _terminal?] nil))
        inv-proc    (i.statechart/new-invocation-processor)
        parent-sid  :async-parent-session
        parent-hook-calls (atom 0)
        parent-hook (fn [_tx] (swap! parent-hook-calls inc))
        env         {::sc/session-id            parent-sid
                     ::sc/vwmem                 (volatile! {::sc/session-id parent-sid})
                     ::sc/event-queue           event-queue
                     ::sc/working-memory-store  wm-store
                     ::sc/processor             async-processor
                     ::sc/statechart-registry   registry
                     ::sc/on-save-hooks         [parent-hook]}
        result      (sp/start-invocation! inv-proc env
                      {:invokeid :my-child
                       :src      :child-chart
                       :params   {}})]

    ;; Wait for the promise's chained save! to complete.
    (when (p/promise? result) @result)

    (testing "parent hook MUST not fire during async child save"
      (is (zero? @parent-hook-calls)
          "the `(dissoc env ::sc/on-save-hooks)` must survive into the p/then continuation so async child saves don't trigger the parent's ACK hook either"))))

;; -----------------------------------------------------------------------------
;; Finding #AB (P1) — FlatWorkingMemoryDataModel must read legacy key after upgrade
;; -----------------------------------------------------------------------------
;;
;; Commit 9330ee6 (2026-01-11) moved the flat data-model storage key from
;; `::wmdm/data-model` (namespace-local) to `::sc/data-model`. Working memory
;; persisted before that commit has its flat map under the legacy key; after
;; upgrade, `current-data` returns nil and `update!` starts a fresh map, so
;; any existing session on a JDBC backend looks blank until it is recreated.
;; Because pg-env uses the flat model by default, this breaks rolling upgrades
;; and restart recovery for any pre-fix deployment.

(deftest flat-data-model-reads-legacy-data-model-key-test
  (let [legacy-key :com.fulcrologic.statecharts.data-model.working-memory-data-model/data-model
        model      (wmdm/new-flat-model)]

    (testing "current-data returns data stored under the legacy key"
      (let [vwmem (volatile! {::sc/session-id :s1
                              legacy-key       {:counter 42 :label "legacy"}})
            env   {::sc/vwmem vwmem}]
        (is (= {:counter 42 :label "legacy"} (sp/current-data model env))
            "pre-9330ee6 flat-model rows must still surface their data post-upgrade")))

    (testing "new-key data takes precedence when both keys are present"
      (let [vwmem (volatile! {::sc/session-id   :s1
                              legacy-key        {:counter 1 :legacy-only :L}
                              ::sc/data-model   {:counter 2 :new-only :N}})
            env   {::sc/vwmem vwmem}]
        (is (= {:counter 2 :legacy-only :L :new-only :N} (sp/current-data model env))
            "new-key values win on collision; legacy-only keys still visible")))

    (testing "update! migrates legacy data to the new key and drops the legacy key"
      (let [vwmem (volatile! {::sc/session-id :s1
                              legacy-key       {:counter 42}})
            env   {::sc/vwmem vwmem}]
        (sp/update! model env {:ops [(ops/assign :new-field :new-value)]})
        (let [wmem @vwmem]
          (is (= {:counter 42 :new-field :new-value} (::sc/data-model wmem))
              "new-key now contains both legacy data AND the assigned op's value")
          (is (nil? (legacy-key wmem))
              "legacy key is dropped on first update! so repeated updates don't re-merge stale data"))))

    (testing "update! on a session with no data at all still works"
      (let [vwmem (volatile! {::sc/session-id :fresh})
            env   {::sc/vwmem vwmem}]
        (sp/update! model env {:ops [(ops/assign :a 1)]})
        (is (= {:a 1} (::sc/data-model @vwmem))
            "no legacy, no new → fresh assignment lands at new key")))))

;; -----------------------------------------------------------------------------
;; Finding #AC (P1) — deleting a JDBC session must cancel its durable jobs
;; -----------------------------------------------------------------------------
;;
;; `StatechartInvocationProcessor/stop-invocation!` calls
;; `delete-working-memory!` on a child session when its invoke state exits.
;; The JDBC store currently only drops the `statechart_sessions` row —
;; any pending/running durable jobs for that session stay active. Workers
;; keep executing side-effects for a session that no longer exists, and
;; later emit `done.invoke.*` / `error.invoke.*` events that have nowhere
;; valid to go.

(deftest ^:integration delete-working-memory-cancels-durable-jobs-test
  (let [store      (pg-wms/new-store *pool*)
        session-id :deletion-test-session
        _          (sp/save-working-memory! store {} session-id
                     {::sc/session-id session-id
                      ::sc/configuration #{:s1}})
        job-id     (UUID/randomUUID)
        _          (job-store/create-job! *pool*
                     {:id           job-id
                      :session-id   session-id
                      :invokeid     :the-invoke
                      :job-type     "http"
                      :payload      {}
                      :max-attempts 3})]

    (testing "pre-condition: job is pending"
      (let [row (job-store/get-active-job *pool* session-id :the-invoke)]
        (is (some? row) "job exists before session delete")
        (is (= "pending" (:status row))
            "job is in a non-terminal state before session delete")))

    (sp/delete-working-memory! store {} session-id)

    (testing "session row is gone"
      (is (nil? (sp/get-working-memory store {} session-id))
          "WM row deleted"))

    (testing "durable job for the deleted session must be cancelled"
      (let [active (job-store/get-active-job *pool* session-id :the-invoke)]
        (is (nil? active)
            "no active (pending/running) job remains for the deleted session — otherwise the worker would keep running side effects")))

    (testing "cancelled job has terminal event recorded for reconciliation"
      ;; cancel-by-session! writes terminal_event_name on each cancelled
      ;; row so the reconciler can still dispatch error.invoke.<invokeid>
      ;; to whoever was listening. This is load-bearing even though the
      ;; parent session is gone — other charts may have been observing.
      (let [rows (core/execute-sql! *pool*
                   "SELECT status, terminal_event_name FROM statechart_jobs WHERE id = ?"
                   [job-id])
            row  (first rows)]
        (is (= "cancelled" (:status row))
            "job marked cancelled, not left pending/running")
        (is (some? (:terminal-event-name row))
            "terminal_event_name set so reconciliation can dispatch error.invoke.*")))))

(deftest ^:integration delete-working-memory-rolls-back-job-cancellation-on-failure-test
  (let [store      (pg-wms/new-store *pool*)
        session-id :rollback-test-session
        _          (sp/save-working-memory! store {} session-id
                     {::sc/session-id session-id
                      ::sc/configuration #{:s1}})
        job-id     (UUID/randomUUID)
        _          (job-store/create-job! *pool*
                     {:id           job-id
                      :session-id   session-id
                      :invokeid     :rollback-invoke
                      :job-type     "http"
                      :payload      {}
                      :max-attempts 3})
        original-execute! @#'core/execute!
        ;; Let cancel-by-session!'s UPDATEs succeed, but fail the final
        ;; DELETE. The outer tx must roll back so the job returns to
        ;; 'pending' and the session row stays.
        call-count (atom 0)
        failing-execute!
        (fn [& args]
          (let [n (swap! call-count inc)
                sql (first (when (map? (second args)) [(second args)]))]
            ;; Only fail the :delete-from statement.
            (if (and (map? (second args)) (:delete-from (second args)))
              (throw (ex-info "simulated failure after cancellation"
                       {:expected :rollback}))
              (apply original-execute! args))))]

    (testing "mid-delete failure rolls back cancel-by-session! too"
      (with-redefs [core/execute! failing-execute!]
        (is (thrown? Exception
              (sp/delete-working-memory! store {} session-id))
            "mid-delete failure must propagate"))

      ;; After rollback, we expect the session still present and the
      ;; job still 'pending' (its pre-delete state).
      (let [session-row (sp/get-working-memory store {} session-id)
            job-status  (:status (first (core/execute-sql! *pool*
                                          "SELECT status FROM statechart_jobs WHERE id = ?"
                                          [job-id])))]
        (is (some? session-row)
            "session row must still exist — outer tx rolled back")
        (is (= "pending" job-status)
            "job must be back to 'pending' — cancel-by-session!'s UPDATEs must have been rolled back along with the failed delete")))

    ;; Clean up so the fixture's truncate doesn't trip on it.
    (sp/delete-working-memory! store {} session-id)))

;; -----------------------------------------------------------------------------
;; Finding #AD (P2) — terminal job dispatch must atomically claim
;; -----------------------------------------------------------------------------
;;
;; The reconciler fetches undispatched terminal jobs with a plain SELECT,
;; then dispatches, then writes `terminal_event_dispatched_at`. Two
;; reconciler passes (across workers, or after a crash between `send!`
;; and the mark) can see the same row and enqueue `done.invoke.*` /
;; `error.invoke.*` twice. Because `get-session-state-fn` is optional
;; and the parent may not have consumed the first event yet, this can
;; drive duplicate transitions.
;;
;; Fix: `claim-terminal-dispatch!` is an atomic conditional UPDATE
;; (`WHERE terminal_event_dispatched_at IS NULL`) — the first caller
;; wins, the second gets 0 affected rows and skips. Trade-off: a crash
;; between the claim commit and `send!` commit now loses the event
;; (previously it was double-fired). Single-event loss is auditable
;; (dispatched_at set with no corresponding event row); duplicate
;; dispatch caused divergent parent state.

(deftest ^:integration claim-terminal-dispatch-is-atomic-test
  (let [session-id :terminal-dispatch-claim-session
        job-id     (UUID/randomUUID)
        _          (job-store/create-job! *pool*
                     {:id           job-id
                      :session-id   session-id
                      :invokeid     :inv-1
                      :job-type     "http"
                      :payload      {}
                      :max-attempts 3})
        ;; Put the row in terminal state so the reconciler considers it.
        _ (core/execute-sql! *pool*
            (str "UPDATE statechart_jobs"
                 " SET status = 'succeeded',"
                 "     terminal_event_name = 'done.invoke.inv-1',"
                 "     terminal_event_data = ?,"
                 "     lease_owner = NULL,"
                 "     updated_at = now()"
                 " WHERE id = ?")
            [(core/freeze {}) job-id])]

    (testing "the first claim wins; the second sees the row already claimed and returns false"
      (is (true? (job-store/claim-terminal-dispatch! *pool* job-id))
          "first caller atomically claims the dispatch slot")
      (is (false? (job-store/claim-terminal-dispatch! *pool* job-id))
          "second concurrent caller must NOT also dispatch — dispatched_at is already set"))

    (testing "row reflects the claim"
      (let [row (first (core/execute-sql! *pool*
                         "SELECT terminal_event_dispatched_at FROM statechart_jobs WHERE id = ?"
                         [job-id]))]
        (is (some? (:terminal-event-dispatched-at row))
            "terminal_event_dispatched_at is set after a successful claim")))

    (testing "an already-dispatched job is not returned by get-undispatched-terminal-jobs"
      (let [undispatched (job-store/get-undispatched-terminal-jobs *pool* 10)]
        (is (not-any? #(= job-id (:id %)) undispatched)
            "claimed+dispatched jobs are not re-surfaced")))))

;; -----------------------------------------------------------------------------
;; Finding #AE (P1 — 2.0.19 regression) — not-ready session must stay re-claimable
;; -----------------------------------------------------------------------------
;;
;; 2.0.19 moved the dispatch claim BEFORE the send, to prevent duplicate
;; dispatch across racing reconcilers. But the pre-existing
;; `dispatch-terminal-event!` returned `false` (without sending) when
;; `get-session-state-fn` indicated the parent wasn't ready to receive
;; the event — that was the retry mechanism for parents that start
;; observing an invocation before the child has terminated. 2.0.19's
;; claim-first ordering silently marks dispatched_at in this case, and
;; the reconciler never picks the row up again → terminal event
;; permanently lost, parent state machine stuck.
;;
;; Fix: split `dispatch-terminal-event!` into a pure readiness check and
;; a pure send; claim only when ready; release the claim if send throws.
;; Pinned here via the reconciler loop so a future regression at any
;; dispatch call site is caught.

(deftest ^:integration reconcile-not-ready-session-keeps-job-retryable-test
  (let [session-id :not-ready-test-session
        job-id     (UUID/randomUUID)
        _          (job-store/create-job! *pool*
                     {:id           job-id
                      :session-id   session-id
                      :invokeid     :some-invoke
                      :job-type     "http"
                      :payload      {}
                      :max-attempts 3})
        _ (core/execute-sql! *pool*
            (str "UPDATE statechart_jobs"
                 " SET status = 'succeeded',"
                 "     terminal_event_name = ?,"
                 "     terminal_event_data = ?,"
                 "     lease_owner = NULL,"
                 "     updated_at = now()"
                 " WHERE id = ?")
            [(pr-str :done.invoke.some-invoke) (core/freeze {}) job-id])
        sent-events (atom [])
        event-queue (reify sp/EventQueue
                      (send! [_ _env req] (swap! sent-events conj req) true)
                      (cancel! [_ _env _sid _sendid] true)
                      (receive-events! [_ _env _h] nil)
                      (receive-events! [_ _env _h _opts] nil))
        not-ready-fn (fn [_sid _iid _job-id] {:ready false :reason :parent-not-observing-yet})
        reconcile!   @#'worker/reconcile-undispatched!]

    (reconcile! *pool* event-queue {}
      {:limit                10
       :get-session-state-fn not-ready-fn})

    (testing "send! must NOT have been called for the not-ready session"
      (is (empty? @sent-events)
          "no event is enqueued when the parent isn't ready"))

    (testing "the row must remain re-claimable (terminal_event_dispatched_at stays nil)"
      (let [row (first (core/execute-sql! *pool*
                         "SELECT terminal_event_dispatched_at FROM statechart_jobs WHERE id = ?"
                         [job-id]))]
        (is (nil? (:terminal-event-dispatched-at row))
            "if we claim dispatched_at before sending, a not-ready session silently permanently loses the terminal event — dispatched_at must stay nil when the send did not happen")))

    (testing "a subsequent pass with a ready session can still dispatch the same job"
      (reset! sent-events [])
      (let [ready-fn (fn [_sid _iid _job-id] {:ready true})]
        (reconcile! *pool* event-queue {}
          {:limit                10
           :get-session-state-fn ready-fn}))
      (is (= 1 (count @sent-events))
          "once the parent becomes ready the event is dispatched exactly once")
      (let [row (first (core/execute-sql! *pool*
                         "SELECT terminal_event_dispatched_at FROM statechart_jobs WHERE id = ?"
                         [job-id]))]
        (is (some? (:terminal-event-dispatched-at row))
            "after the ready-path dispatch, the row is marked dispatched")))))

;; -----------------------------------------------------------------------------
;; Finding #AF (P1, post-2.0.20 review) — fallback ACK fires after silently-
;; rolled-back save
;; -----------------------------------------------------------------------------
;;
;; `receive-events!` installs `::sc/on-save-hooks` so the event ACK commits
;; atomically with the handler's working-memory save. After the handler
;; returns it ALSO unconditionally runs `(mark-processed! tx event-id)` as a
;; fallback ACK for handlers that completed without ever calling
;; `save-working-memory!` (terminated / missing session case).
;;
;; Bug: the fallback does not know whether the handler attempted a save that
;; rolled back. If the handler calls `save-working-memory!`, catches the
;; resulting exception (e.g. optimistic-lock failure, constraint violation,
;; connection loss), and returns normally, the hook never fired but the
;; fallback still ACKs. The event is silently lost: the handler's effects
;; rolled back, WM is unchanged, but the queue row is marked processed and
;; the next poll cycle excludes it.
;;
;; Fix: gate the fallback on "did the handler actually attempt to save?".
;; An `::sc/save-attempted?` atom in env is flipped by
;; `save-working-memory!` before it opens its tx. After the handler returns,
;; the fallback only fires when the atom is still `false` (no save
;; attempted) — which preserves the terminated-session path while closing
;; the rollback window.

(deftest ^:integration receive-events-does-not-silently-ack-when-save-rolls-back-test
  (testing "fallback mark-processed! must NOT fire if the handler's save-working-memory! call rolled back"
    (let [q          (pg-eq/new-queue *pool* "fallback-ack-worker")
          wms        (pg-wms/new-store *pool*)
          session-id :fallback-ack-session
          _          (sp/save-working-memory! wms {} session-id
                       {::sc/session-id         session-id
                        ::sc/statechart-src     :any-chart
                        ::sc/configuration      #{:s1}
                        ::sc/initialized-states #{:s1}
                        ::sc/history-value      {}
                        ::sc/running?           true
                        ::sc/data-model         {}})
          handler-ran? (atom false)
          handler    (fn [env _event]
                       (reset! handler-ran? true)
                       ;; Simulate a real-world handler that attempts a save that
                       ;; rolls back (here: force OCC failure), then catches the
                       ;; exception and returns normally.
                       (try
                         (let [wmem     (sp/get-working-memory wms env session-id)
                               bad-wmem (core/attach-version wmem 9999)]
                           (sp/save-working-memory! wms env session-id bad-wmem))
                         (catch Exception _ nil))
                       :handler-returned-normally)]
      (sp/send! q {} {:event             :test
                      :target            session-id
                      :source-session-id session-id})
      (sp/receive-events! q {} handler)
      (is (true? @handler-ran?) "handler actually ran")
      (let [row (first (core/execute-sql! *pool*
                         "SELECT processed_at FROM statechart_events WHERE target_session_id = ?"
                         [(core/session-id->str session-id)]))]
        (is (nil? (:processed-at row))
            "event must stay unacked when the handler's save rolled back — the unconditional fallback mark-processed! silently loses the event")))))

(deftest ^:integration receive-events-fallback-ack-still-fires-when-no-save-attempted-test
  (testing "handlers that deliberately don't save (terminated / missing session) must still have their events ACKed via the fallback"
    (let [q          (pg-eq/new-queue *pool* "no-save-worker")
          session-id :no-save-session
          handler    (fn [_env _event]
                       ;; no save-working-memory! call — simulates handler
                       ;; returning on a session that is gone / terminated.
                       :no-op)]
      (sp/send! q {} {:event             :test
                      :target            session-id
                      :source-session-id session-id})
      (sp/receive-events! q {} handler)
      (let [row (first (core/execute-sql! *pool*
                         "SELECT processed_at FROM statechart_events WHERE target_session_id = ?"
                         [(core/session-id->str session-id)]))]
        (is (some? (:processed-at row))
            "when the handler never attempted to save, the fallback ACK must still fire so zombie events for gone sessions don't loop forever")))))

;; -----------------------------------------------------------------------------
;; Finding #AG (P1, post-2.0.20 review) — async start-invocation! leaks unresolved
;; promise; parent ACK can commit before child is durable
;; -----------------------------------------------------------------------------
;;
;; `StatechartInvocationProcessor/start-invocation!` returns `(p/then result
;; save!)` on the async path — a promise that resolves when the child's WM
;; save completes. The caller (statechart processor) may proceed to save the
;; parent's own WM before that chained promise resolves. Since 2.0.16 the
;; parent save carries `::sc/on-save-hooks`, so the parent event is ACKed
;; when the parent save commits — potentially BEFORE the async child save
;; has committed to the DB. If the child save then rejects (DB error, OCC),
;; the parent event is already ACKed, the child was never persisted, and
;; the next poll won't retry.
;;
;; Fix: `start-invocation!` must block on the chained save promise before
;; returning. It always returns `true`/`false`, never a raw promise.

(deftest statechart-invocation-async-child-save-completes-before-return-test
  (testing "start-invocation! on the async path must block until the child save commits"
    (let [saved-wms          (atom {})
          child-save-latch   (java.util.concurrent.CountDownLatch. 1)
          wm-store           (reify sp/WorkingMemoryStore
                               (get-working-memory [_ _env sid] (get @saved-wms sid))
                               (save-working-memory! [_ _env sid wmem]
                                 ;; Simulate slow DB commit on a different thread.
                                 (Thread/sleep 40)
                                 (swap! saved-wms assoc sid wmem)
                                 (.countDown child-save-latch)
                                 true)
                               (delete-working-memory! [_ _env sid]
                                 (swap! saved-wms dissoc sid)
                                 true))
          registry           (mem-reg/new-registry)
          _                  (sp/register-statechart! registry :child-chart
                               (chart/statechart {:initial :c1} (state {:id :c1})))
          async-processor    (reify sp/Processor
                               ;; `start!` returns a promise that resolves asynchronously.
                               (start! [_ _env _src {::sc/keys [session-id]}]
                                 (p/create
                                   (fn [resolve _reject]
                                     (future
                                       (Thread/sleep 10)
                                       (resolve
                                         {::sc/session-id     session-id
                                          ::sc/statechart-src :child-chart
                                          ::sc/configuration  #{:c1}})))))
                               (process-event! [_ _env wmem _event] wmem)
                               (exit! [_ _env _wmem _terminal?] nil))
          inv-proc           (i.statechart/new-invocation-processor)
          parent-sid         :async-child-save-parent
          env                {::sc/session-id           parent-sid
                              ::sc/vwmem                (volatile! {::sc/session-id parent-sid})
                              ::sc/event-queue          (reify sp/EventQueue
                                                          (send! [_ _ _] true)
                                                          (cancel! [_ _ _ _] true)
                                                          (receive-events! [_ _ _] nil)
                                                          (receive-events! [_ _ _ _] nil))
                              ::sc/working-memory-store wm-store
                              ::sc/processor            async-processor
                              ::sc/statechart-registry  registry}
          result             (sp/start-invocation! inv-proc env
                               {:invokeid :my-child
                                :src      :child-chart
                                :params   {}})]
      (testing "start-invocation! returns a boolean, not a raw promise"
        (is (not (p/promise? result))
            "callers that do NOT await the return value (sync processors, wiring code) would proceed to save the parent's WM — which ACKs the parent event — before the child was durable. start-invocation! must block internally."))
      (testing "child save has committed by the time start-invocation! returns"
        (is (pos? (count @saved-wms))
            "child WM must be in the store synchronously with start-invocation! returning; a raw unresolved promise return lets the caller's parent save ACK before the child is persisted")))))

;; -----------------------------------------------------------------------------
;; Finding #AH (P1, post-2.0.20 review) — release-terminal-dispatch-claim! must
;; be guarded so it only clears actual claims
;; -----------------------------------------------------------------------------
;;
;; `release-terminal-dispatch-claim!` currently updates `WHERE id = ?` with no
;; ownership or state guard. Its docstring rationalises this as "the caller
;; owns the claim by construction" — but any stray call (same id from two
;; code paths, or the wrong id) silently rewrites the column. The fix makes
;; the update a no-op unless `terminal_event_dispatched_at IS NOT NULL`, so
;; double-release is a 0-row update and releasing an unclaimed row cannot
;; accidentally corrupt state.

(deftest ^:integration release-terminal-dispatch-claim-only-affects-claimed-rows-test
  (let [session-id :release-guard-session
        job-id     (UUID/randomUUID)]
    (job-store/create-job! *pool* {:id           job-id
                                   :session-id   session-id
                                   :invokeid     :iv
                                   :job-type     "http"
                                   :payload      {}
                                   :max-attempts 3})
    (core/execute-sql! *pool*
      (str "UPDATE statechart_jobs"
           " SET status = 'succeeded',"
           "     terminal_event_name = ?,"
           "     terminal_event_data = ?,"
           "     lease_owner = NULL,"
           "     updated_at = now()"
           " WHERE id = ?")
      [(pr-str :done.invoke.iv) (core/freeze {}) job-id])

    (testing "release on an UNclaimed row affects 0 rows"
      (let [result (job-store/release-terminal-dispatch-claim! *pool* job-id)]
        (is (zero? (core/affected-row-count result))
            "release must be idempotent / guarded: a stray release on a row whose dispatched_at is already nil must not count as a modification (current bug: WHERE id=? matches all; guard needs WHERE dispatched_at IS NOT NULL)")))

    (testing "release on a CLAIMED row reverts the timestamp (1-row update)"
      (is (true? (job-store/claim-terminal-dispatch! *pool* job-id)))
      (let [result (job-store/release-terminal-dispatch-claim! *pool* job-id)]
        (is (= 1 (core/affected-row-count result))
            "release of a claimed row affects exactly 1 row")))

    (testing "double-release is idempotent (second call is a no-op)"
      (let [result (job-store/release-terminal-dispatch-claim! *pool* job-id)]
        (is (zero? (core/affected-row-count result))
            "a second release against an already-cleared row must be a 0-row update — makes misuse silently safe rather than silently corrupt")))))

;; -----------------------------------------------------------------------------
;; Finding #AI (P1, post-2.0.20 review) — delete-working-memory! must purge
;; pending events
;; -----------------------------------------------------------------------------
;;
;; `delete-working-memory!` deletes the session row and cancels in-flight
;; durable jobs, but leaves pending/delayed rows in `statechart_events` with
;; `target_session_id = <deleted-session>`. Workers then claim and process
;; those zombie events against a WM that doesn't exist, wasting cycles and
;; polluting logs (the fallback ACK eventually clears them, but they should
;; be gone from the moment the session is deleted — the tx promise in the
;; docstring implies atomic cleanup). The fix DELETEs pending events in the
;; same tx as the session row.

(deftest ^:integration delete-working-memory-purges-pending-events-test
  (testing "delete-working-memory! must remove queued events for the deleted session in the same tx"
    (let [session-id :delete-events-session
          wms        (pg-wms/new-store *pool*)
          q          (pg-eq/new-queue *pool* "purge-test-worker")]
      (sp/save-working-memory! wms {} session-id
        {::sc/session-id         session-id
         ::sc/statechart-src     :any
         ::sc/configuration      #{}
         ::sc/initialized-states #{}
         ::sc/history-value      {}
         ::sc/running?           true
         ::sc/data-model         {}})
      ;; Queue two events: one immediate, one delayed.
      (sp/send! q {} {:event             :e1
                      :target            session-id
                      :source-session-id session-id})
      (sp/send! q {} {:event             :e2
                      :target            session-id
                      :source-session-id session-id
                      :delay             60000})
      (is (= 2 (pg-eq/queue-depth *pool* {:session-id session-id}))
          "setup: two events queued")

      (sp/delete-working-memory! wms {} session-id)

      (is (zero? (pg-eq/queue-depth *pool* {:session-id session-id}))
          "after delete, no queued events remain for the deleted session — currently pending rows linger and get claimed by workers against a missing WM"))))

;; -----------------------------------------------------------------------------
;; Finding #AJ (P2, post-2.0.20 review) — fetch-session must not crash on
;; corrupted statechart_src
;; -----------------------------------------------------------------------------
;;
;; `working_memory_store/fetch-session` calls `edn/read-string` on
;; `:statechart-src` with no try/catch. A row with malformed EDN (corrupted
;; write, mixed-writer deployment, bad migration) throws and the session is
;; permanently unreadable. The fix wraps the read, logs once, and falls back
;; to the raw string so callers can recover / triage.

(deftest ^:integration fetch-session-survives-corrupted-statechart-src-test
  (testing "get-working-memory must not throw on a malformed :statechart-src"
    (let [session-id    :corrupt-src-session
          wms           (pg-wms/new-store *pool*)]
      (core/execute-sql! *pool*
        (str "INSERT INTO statechart_sessions"
             " (session_id, statechart_src, working_memory, version)"
             " VALUES (?, ?, ?, 1)")
        [(core/session-id->str session-id)
         "{{{ not valid edn"
         (core/freeze {::sc/session-id session-id})])
      (let [outcome (try
                      {:ok (sp/get-working-memory wms {} session-id)}
                      (catch Exception e {:error e}))]
        (is (nil? (:error outcome))
            "corrupted statechart_src row must not crash fetch-session — one bad row currently wedges the entire session")
        (is (some? (:ok outcome))
            "fetch returns a working-memory map (possibly with a string :statechart-src fallback) so callers can surface the corruption")))))

;; -----------------------------------------------------------------------------
;; Finding #AK (P2, post-2.0.20 review) — hook-stripping must be pattern-based,
;; not a single-key denylist
;; -----------------------------------------------------------------------------
;;
;; `start-invocation!` currently does `(dissoc env ::sc/on-save-hooks)` with
;; an explicit comment warning future maintainers to add each new
;; `::sc/on-*-hooks` key to the dissoc. Relying on a human to remember is a
;; silent trap: a missed keyword means child saves run parent-session
;; bookkeeping. The fix sweeps all `::sc/*-hooks` keys in one pass so any
;; future hook is automatically excluded from the child env.

(deftest statechart-invocation-strips-all-sc-hooks-keys-from-child-env-test
  (testing "every ::sc/*-hooks key in the parent env must be stripped before the child save"
    (let [captured-env      (atom nil)
          saved-wms         (atom {})
          event-queue       (reify sp/EventQueue
                              (send! [_ _env _req] true)
                              (cancel! [_ _env _sid _sendid] true)
                              (receive-events! [_ _env _h] nil)
                              (receive-events! [_ _env _h _opts] nil))
          registry          (mem-reg/new-registry)
          _                 (sp/register-statechart! registry :child-chart
                              (chart/statechart {:initial :c1} (state {:id :c1})))
          wm-store          (reify sp/WorkingMemoryStore
                              (get-working-memory [_ _env sid] (get @saved-wms sid))
                              (save-working-memory! [_ env sid wmem]
                                (reset! captured-env env)
                                (swap! saved-wms assoc sid wmem)
                                true)
                              (delete-working-memory! [_ _env sid]
                                (swap! saved-wms dissoc sid)
                                true))
          processor         (reify sp/Processor
                              (start! [_ _env _src {::sc/keys [session-id]}]
                                {::sc/session-id session-id ::sc/configuration #{:c1}})
                              (process-event! [_ _env wmem _event] wmem)
                              (exit! [_ _env _wmem _terminal?] nil))
          inv-proc          (i.statechart/new-invocation-processor)
          parent-sid        :hook-sweep-parent
          future-hook       (fn [_tx] :fired)
          env               {::sc/session-id            parent-sid
                             ::sc/vwmem                 (volatile! {::sc/session-id parent-sid})
                             ::sc/event-queue           event-queue
                             ::sc/working-memory-store  wm-store
                             ::sc/processor             processor
                             ::sc/statechart-registry   registry
                             ::sc/on-save-hooks         [future-hook]
                             ;; Simulate a hypothetical future hook key the
                             ;; current explicit denylist has not been
                             ;; updated for.
                             ::sc/on-delete-hooks       [future-hook]
                             ::sc/pre-save-hooks        [future-hook]}]
      (sp/start-invocation! inv-proc env
        {:invokeid :my-child
         :src      :child-chart
         :params   {}})
      (let [child-env @captured-env]
        (is (some? child-env) "child save ran and captured the env")
        (is (nil? (::sc/on-save-hooks child-env))
            "on-save-hooks already stripped by the 2.0.19 fix")
        (is (nil? (::sc/on-delete-hooks child-env))
            "any ::sc/*-hooks key must be stripped — currently only the explicit on-save-hooks dissoc is done, future hooks silently leak into child saves and trigger parent bookkeeping")
        (is (nil? (::sc/pre-save-hooks child-env))
            "pattern-based sweep must cover pre-save-hooks and any other ::sc/*-hooks key")))))

;; -----------------------------------------------------------------------------
;; Finding #AL (P1, post-2.0.20 review) — cross-worker per-session FIFO
;; -----------------------------------------------------------------------------
;;
;; `event_queue/receive-events!`'s docstring asserts a per-session FIFO
;; invariant within a batch. Across workers it does NOT hold: worker A can
;; claim event 1 for session S, worker B polls and sees event 2 for S as
;; unclaimed, claims it, and processes 2 before A finishes 1. The fix adds a
;; NOT-EXISTS guard so the claim query excludes sessions whose events are
;; currently claimed by anyone. This closes the common window (doesn't fully
;; eliminate the race under simultaneous claim evaluation — users who need
;; strict FIFO should partition by `:session-id` or run a single worker).

(deftest ^:integration claim-skips-sessions-with-in-flight-claims-test
  (testing "once a worker has claimed an event for session S, another worker must not claim a later event for the same S until the first event is processed"
    (let [session-id   :fifo-session
          other-sid    :other-session
          worker-a-id  "worker-a"]
      ;; Seed the queue:
      ;;  - session-id has an in-flight claim by worker-a on event #1
      ;;  - session-id ALSO has an unclaimed event #2
      ;;  - other-sid has an unclaimed event
      (core/execute-sql! *pool*
        (str "INSERT INTO statechart_events "
             "(target_session_id, source_session_id, event_name, event_type, event_data, deliver_at, claimed_at, claimed_by)"
             " VALUES (?, ?, ?, ?, ?, now(), now(), ?)")
        [(core/session-id->str session-id) (core/session-id->str session-id)
         (pr-str :e1-in-flight) (pr-str :external) (core/freeze {}) worker-a-id])
      (core/execute-sql! *pool*
        (str "INSERT INTO statechart_events "
             "(target_session_id, source_session_id, event_name, event_type, event_data, deliver_at)"
             " VALUES (?, ?, ?, ?, ?, now())")
        [(core/session-id->str session-id) (core/session-id->str session-id)
         (pr-str :e2-same-session) (pr-str :external) (core/freeze {})])
      (core/execute-sql! *pool*
        (str "INSERT INTO statechart_events "
             "(target_session_id, source_session_id, event_name, event_type, event_data, deliver_at)"
             " VALUES (?, ?, ?, ?, ?, now())")
        [(core/session-id->str other-sid) (core/session-id->str other-sid)
         (pr-str :other) (pr-str :external) (core/freeze {})])

      (let [q          (pg-eq/new-queue *pool* "worker-b")
            received   (atom [])
            handler    (fn [_env event] (swap! received conj (:name event)) :ok)]
        (sp/receive-events! q {} handler)
        (let [names (set @received)]
          (is (not (contains? names :e2-same-session))
              "worker-b must NOT claim e2 while worker-a has e1 in flight — cross-worker per-session FIFO is broken without this guard")
          (is (contains? names :other)
              "worker-b still makes progress on events for other sessions (no global lock)"))))))

