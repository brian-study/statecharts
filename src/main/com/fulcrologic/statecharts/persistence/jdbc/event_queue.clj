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
     :invoke-id (when invoke-id (name invoke-id))
     :event-name (pr-str event)
     :event-type (name (or type :external))
     :event-data (core/freeze (or data {}))
     :deliver-at deliver-at}))

(defn- row->event
  "Convert a database row back to an event."
  [row]
  (let [event-name (edn/read-string (:event-name row))
        data (core/thaw (:event-data row))]
    (evts/new-event
     (cond-> {:name event-name
              :type (keyword (:event-type row))
              :target (core/str->session-id (:target-session-id row))
              :data (or data {})}
       (:source-session-id row)
       (assoc ::sc/source-session-id (core/str->session-id (:source-session-id row)))
       (:send-id row)
       (assoc :sendid (:send-id row) ::sc/send-id (:send-id row))
       (:invoke-id row)
       (assoc :invokeid (:invoke-id row))))))

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
    ;; Claim events in their own transaction
    (let [claimed-events (core/with-tx [tx datasource]
                           (claim-events! tx node-id options))
          claimed-count (count claimed-events)]
      (when (pos? claimed-count)
        (log/debug "Claimed events for processing"
                   {:count claimed-count
                    :node-id node-id
                    :session-filter (:session-id options)}))
      ;; Process each event in its own transaction so that a failure in event N
      ;; doesn't roll back the mark-processed of events 1..N-1.
      (doseq [row claimed-events]
        (let [event-id (:id row)
              event-name (:event-name row)
              target (:target-session-id row)
              start-time (System/nanoTime)]
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
                            :node-id node-id})))
            (catch Exception e
              (log/error e "Event handler threw an exception"
                         {:event-id event-id
                          :event-name event-name
                          :target target
                          :node-id node-id})
              ;; Release claim so event can be retried
              (core/with-tx [tx datasource]
                (release-claim! tx event-id))
              (log/info "Event released for retry"
                        {:event-id event-id
                         :event-name event-name
                         :target target}))
            (catch Error e
              (log/error e "Event handler threw a fatal error"
                         {:event-id event-id
                          :event-name event-name
                          :target target
                          :node-id node-id})
              ;; Release this event's claim so it can be retried. Remaining
              ;; claimed events in this batch will be recovered by stale claim
              ;; timeout.
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
              (throw e))))))))

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
   (let [sql (cond-> {:select [[[:count :*] :count]]
                      :from [:statechart-events]
                      :where [:is :processed-at nil]}
               session-id
               (update :where conj [:= :target-session-id (core/session-id->str session-id)]))]
     (:count (core/execute-one! ds sql)))))
