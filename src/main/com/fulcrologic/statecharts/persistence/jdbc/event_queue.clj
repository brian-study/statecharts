(ns com.fulcrologic.statecharts.persistence.jdbc.event-queue
  "A PostgreSQL-backed event queue with exactly-once delivery semantics.

   Events are stored in a database table with support for:
   - Delayed delivery (via deliver_at timestamp)
   - Exactly-once processing (via SELECT FOR UPDATE SKIP LOCKED)
   - Cancellation of pending delayed events
   - Claim timeout recovery for failed workers"
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.events :as evts]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.protocols :as sp]
   [taoensso.timbre :as log])
  (:import
   [java.time OffsetDateTime Duration]))

;; -----------------------------------------------------------------------------
;; Event Type Support
;; -----------------------------------------------------------------------------

(defn- supported-type?
  "Returns true if the given type looks like a statechart type."
  [type]
  (or
   (nil? type)
   (and (string? type) (str/starts-with? (str/lower-case type) "http://www.w3.org/tr/scxml"))
   (= type ::sc/chart)
   (= type :statechart)))

;; -----------------------------------------------------------------------------
;; Internal Helpers
;; -----------------------------------------------------------------------------

(defn- parse-event-type
  "Read an event-type from the DB back to a keyword.

   New rows store `(pr-str type)` — e.g. `\":external\"` or
   `\":com.fulcrologic.statecharts/chart\"`. Older rows (pre-2.0.2) stored
   `(name type)` — e.g. `\"external\"`, namespace stripped. The parser is
   tolerant: EDN-read anything that starts with `:`, otherwise fall back to
   `(keyword s)` for the legacy shape."
  [s]
  (when s
    (if (str/starts-with? s ":")
      (edn/read-string s)
      (keyword s))))

(defn- parse-invoke-id
  "Read an invoke-id from the DB back to its original type.

   New rows (2.0.4+) store `(pr-str invoke-id)` — keywords, UUIDs, numbers,
   and strings all round-trip through EDN. Legacy rows (pre-2.0.4) stored
   `(name invoke-id)` as a bare string — but the original was always a
   keyword (pre-2.0.4 write path used `(name x)` which only accepts
   keyword/string/symbol, and the dominant case was keyword), so we
   restore the keyword shape on readback.

   Matches `job_store/str->invokeid`'s legacy fallback so both decoders
   agree on the type of a legacy bare row; `handle-external-invocations!`
   matches by `=` against the original idlocation value and a string vs
   keyword mismatch would silently skip finalize/autoforward."
  [s]
  (when s
    (cond
      (str/starts-with? s ":")                ; keyword (incl. namespaced)
      (try (edn/read-string s) (catch Exception _ s))

      (str/starts-with? s "\"")               ; quoted string
      (try (edn/read-string s) (catch Exception _ s))

      (str/starts-with? s "#")                ; tagged literal, e.g. #uuid "..."
      (try (edn/read-string s) (catch Exception _ s))

      (re-matches #"-?\d+(?:\.\d+)?" s)       ; number
      (try (edn/read-string s) (catch Exception _ s))

      :else                                   ; legacy bare row — always keyword
      (keyword s))))

(defn- event->row
  "Convert a send-request to a database row."
  [{:keys [event data type target source-session-id send-id invoke-id delay]}]
  (let [now (OffsetDateTime/now)
        deliver-at (if delay
                     (.plus now (Duration/ofMillis delay))
                     now)]
    {:target-session-id (core/session-id->str (or target source-session-id))
     :source-session-id (when source-session-id (core/session-id->str source-session-id))
     :send-id send-id
     ;; invoke-id may be a keyword (including namespaced), UUID, number, string,
     ;; etc. `(name x)` only works for keywords/strings and loses namespaces.
     ;; Round-trip via EDN so row->event can restore the original type.
     :invoke-id (when invoke-id (pr-str invoke-id))
     :event-name (pr-str event)
     :event-type (pr-str (or type :external))
     :event-data (core/freeze (or data {}))
     :deliver-at deliver-at}))

(defn- row->event
  "Convert a database row back to an event."
  [row]
  (let [event-name (edn/read-string (:event-name row))
        data (core/thaw (:event-data row))]
    (evts/new-event
     (cond-> {:name event-name
              :type (parse-event-type (:event-type row))
              :target (core/str->session-id (:target-session-id row))
              :data (or data {})}
       (:source-session-id row)
       (assoc ::sc/source-session-id (core/str->session-id (:source-session-id row)))
       (:send-id row)
       (assoc :sendid (:send-id row) ::sc/send-id (:send-id row))
       (:invoke-id row)
       (assoc :invokeid (parse-invoke-id (:invoke-id row)))))))

(defn- insert-event!
  "Insert an event into the queue."
  [ds send-request]
  (core/execute! ds
                 {:insert-into :statechart-events
                  :values [(event->row send-request)]})
  true)

(defn- cancel-events!
  "Cancel pending delayed events matching session-id and send-id."
  [ds session-id send-id]
  (core/execute! ds
                 {:delete-from :statechart-events
                  :where [:and
                          [:= :source-session-id (core/session-id->str session-id)]
                          [:= :send-id send-id]
                          [:is :processed-at nil]
                          [:> :deliver-at [:now]]]})
  true)

(defn- claim-events!
  "Claim events ready for delivery using SELECT FOR UPDATE SKIP LOCKED.
   Returns the claimed event rows."
  [conn node-id {:keys [session-id batch-size]
                 :or {batch-size 10}}]
  (let [batch-size (long batch-size)] ;; ensure numeric — prevent SQL injection
    (if session-id
      (core/execute-sql! conn
        (str "UPDATE statechart_events "
             "SET claimed_at = now(), claimed_by = ? "
             "WHERE id IN ("
             "  SELECT id FROM statechart_events "
             "  WHERE processed_at IS NULL "
             "    AND claimed_at IS NULL "
             "    AND deliver_at <= now() "
             "    AND target_session_id = ? "
             "  ORDER BY deliver_at, id "
             "  LIMIT " batch-size " "
             "  FOR UPDATE SKIP LOCKED"
             ") "
             "RETURNING *")
        [node-id (core/session-id->str session-id)])
      (core/execute-sql! conn
        (str "UPDATE statechart_events "
             "SET claimed_at = now(), claimed_by = ? "
             "WHERE id IN ("
             "  SELECT id FROM statechart_events "
             "  WHERE processed_at IS NULL "
             "    AND claimed_at IS NULL "
             "    AND deliver_at <= now() "
             "  ORDER BY deliver_at, id "
             "  LIMIT " batch-size " "
             "  FOR UPDATE SKIP LOCKED"
             ") "
             "RETURNING *")
        [node-id]))))

(defn- mark-processed!
  "Mark an event as processed."
  [conn event-id]
  (core/execute! conn
                 {:update :statechart-events
                  :set {:processed-at [:now]}
                  :where [:= :id event-id]}))

(defn- release-claim!
  "Release a claim on an event (for retry after failure)."
  [conn event-id]
  (core/execute! conn
                 {:update :statechart-events
                  :set {:claimed-at nil
                        :claimed-by nil}
                  :where [:= :id event-id]}))

;; -----------------------------------------------------------------------------
;; EventQueue Implementation
;; -----------------------------------------------------------------------------

(defrecord JdbcEventQueue [datasource node-id]
  sp/EventQueue
  (send! [_ _env send-request]
    (let [{:keys [event type target source-session-id delay]} send-request
          target-id (or target source-session-id)]
      (if (and (supported-type? type) target-id)
        (do
          (insert-event! datasource send-request)
          (log/debug "Event queued"
                     {:event event
                      :target target-id
                      :delay-ms delay
                      :node-id node-id})
          true)
        (do
          (log/trace "Event not queued (unsupported type or no target)"
                     {:event event :type type :target target-id})
          false))))

  (cancel! [_ _env session-id send-id]
    (log/debug "Cancelling delayed event"
               {:session-id session-id
                :send-id send-id
                :node-id node-id})
    (cancel-events! datasource session-id send-id))

  (receive-events! [this env handler]
    (sp/receive-events! this env handler {}))

  (receive-events! [_ env handler options]
    ;; Claim events in their own transaction.
    ;; UPDATE … RETURNING does NOT preserve the subquery's ORDER BY, so
    ;; re-sort in memory before handing to the handler to keep FIFO delivery.
    (let [claimed-events (->> (core/with-tx [tx datasource]
                                (claim-events! tx node-id options))
                              (sort-by (juxt :deliver-at :id)))
          claimed-count (count claimed-events)]
      (when (pos? claimed-count)
        (log/debug "Claimed events for processing"
                   {:count claimed-count
                    :node-id node-id
                    :session-filter (:session-id options)}))
      ;; Process each event in its own transaction so that a failure in event N
      ;; doesn't roll back the mark-processed of events 1..N-1.
      ;;
      ;; Per-session FIFO invariant: if the handler throws on event N, any
      ;; subsequent events for the SAME session in this batch must wait for
      ;; N's retry to succeed first — their claims are released so the next
      ;; poll picks them up AFTER the retried event. Events for OTHER sessions
      ;; keep processing so a poison event in one session doesn't block
      ;; liveness in others.
      (loop [[row & more] claimed-events
             blocked-sessions #{}]
        (when row
          (let [event-id (:id row)
                event-name (:event-name row)
                target (:target-session-id row)
                start-time (System/nanoTime)
                blocked? (contains? blocked-sessions target)
                next-blocked
                (cond
                  blocked?
                  (do
                    (core/with-tx [tx datasource]
                      (release-claim! tx event-id))
                    blocked-sessions)

                  :else
                  (try
                    (let [event (row->event row)]
                      (handler env event)
                      (core/with-tx [tx datasource]
                        (mark-processed! tx event-id))
                      (let [duration-ms (/ (- (System/nanoTime) start-time) 1e6)]
                        (log/debug "Event processed"
                                   {:event-id event-id
                                    :event event-name
                                    :target target
                                    :duration-ms duration-ms
                                    :node-id node-id}))
                      blocked-sessions)
                    (catch Exception e
                      (log/error e "Event handler threw an exception"
                                 {:event-id event-id
                                  :event-name event-name
                                  :target target
                                  :node-id node-id})
                      (core/with-tx [tx datasource]
                        (release-claim! tx event-id))
                      (log/info "Event released for retry"
                                {:event-id event-id
                                 :event-name event-name
                                 :target target})
                      (conj blocked-sessions target))
                    (catch Error e
                      (log/error e "Event handler threw a fatal error"
                                 {:event-id event-id
                                  :event-name event-name
                                  :target target
                                  :node-id node-id})
                      (try
                        (core/with-tx [tx datasource]
                          (release-claim! tx event-id))
                        (log/info "Event released for retry after fatal error"
                                  {:event-id event-id
                                   :event-name event-name
                                   :target target})
                        (catch Throwable t
                          (log/error t "Failed to release claim after fatal error"
                                     {:event-id event-id})))
                      (throw e))))]
            (recur more next-blocked)))))))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn new-queue
  "Create a new JDBC-backed event queue.

   datasource - javax.sql.DataSource (HikariCP is the standard choice)
   node-id    - Unique identifier for this worker node (for claim tracking)"
  ([datasource] (new-queue datasource (str (random-uuid))))
  ([datasource node-id]
   (->JdbcEventQueue datasource node-id)))

;; -----------------------------------------------------------------------------
;; Maintenance Functions
;; -----------------------------------------------------------------------------

(defn recover-stale-claims!
  "Recover events that were claimed but never processed.
   This can happen if a worker crashes during processing.

   timeout-seconds - How long a claim can be held before recovery (default 30)

   Returns the number of recovered events."
  ([ds] (recover-stale-claims! ds 30))
  ([ds timeout-seconds]
   (let [timeout-seconds (long timeout-seconds) ;; ensure numeric — prevent SQL injection
         result (core/execute! ds
                               {:update :statechart-events
                                :set {:claimed-at nil
                                      :claimed-by nil}
                                :where [:and
                                        [:is-not :claimed-at nil]
                                        [:is :processed-at nil]
                                        [:< :claimed-at [:raw (str "now() - interval '" timeout-seconds " seconds'")]]]})
         recovered-count (core/affected-row-count result)]
     (when (pos? recovered-count)
       (log/info "Recovered stale event claims"
                 {:count recovered-count
                  :timeout-seconds timeout-seconds}))
     recovered-count)))

(defn purge-processed-events!
  "Delete processed events older than the specified retention period.

   retention-days - How many days of processed events to keep (default 7)

   Returns the number of purged events."
  ([ds] (purge-processed-events! ds 7))
  ([ds retention-days]
   (let [retention-days (long retention-days) ;; ensure numeric — prevent SQL injection
         result (core/execute! ds
                               {:delete-from :statechart-events
                                :where [:and
                                        [:is-not :processed-at nil]
                                        [:< :processed-at [:raw (str "now() - interval '" retention-days " days'")]]]})
         purged-count (core/affected-row-count result)]
     (when (pos? purged-count)
       (log/info "Purged old processed events"
                 {:count purged-count
                  :retention-days retention-days}))
     purged-count)))

(defn queue-depth
  "Get the current queue depth (unprocessed events).

   Options:
   - :session-id - Filter by session ID"
  ([ds] (queue-depth ds {}))
  ([ds {:keys [session-id]}]
   (let [;; `(update :where conj …)` on `[:is :processed-at nil]` produces
         ;; `[:is :processed-at nil […]]`, which HoneySQL formats as the
         ;; illegal `processed_at IS NULL IS (…)`. Build the predicate list
         ;; first, then wrap in :and if we ended up with more than one.
         predicates (cond-> [[:is :processed-at nil]]
                      session-id
                      (conj [:= :target-session-id (core/session-id->str session-id)]))
         where-clause (if (= 1 (count predicates))
                        (first predicates)
                        (into [:and] predicates))
         sql {:select [[[:count :*] :count]]
              :from [:statechart-events]
              :where where-clause}]
     (:count (core/execute-one! ds sql)))))
