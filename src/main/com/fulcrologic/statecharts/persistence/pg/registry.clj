(ns com.fulcrologic.statecharts.persistence.pg.registry
  "A PostgreSQL-backed statechart registry with local caching.

   Chart definitions are stored in the database and cached in memory
   for fast lookups."
  (:require
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.protocols :as sp]
   [pg.core :as pg]
   [pg.pool :as pool]
   [taoensso.timbre :as log]))

;; -----------------------------------------------------------------------------
;; Internal Helpers
;; -----------------------------------------------------------------------------

(defn- src->str
  "Convert a statechart src to a string for database storage."
  [src]
  (pr-str src))

(defn- str->src
  "Convert a stored string back to a statechart src."
  [s]
  (when s
    (clojure.edn/read-string s)))

(defn- fetch-definition
  "Fetch a chart definition by src."
  [conn src]
  (when-let [row (core/execute-one! conn
                                    {:select [:definition]
                                     :from [:statechart-definitions]
                                     :where [:= :src (src->str src)]})]
    (core/thaw (:definition row))))

(defn- fetch-all-definitions
  "Fetch all chart definitions."
  [conn]
  (let [rows (core/execute! conn
                            {:select [:src :definition]
                             :from [:statechart-definitions]})]
    (into {}
          (map (fn [row]
                 [(str->src (:src row))
                  (core/thaw (:definition row))]))
          rows)))

(defn- upsert-definition!
  "Insert or update a chart definition."
  [conn src definition]
  (core/execute! conn
                 {:insert-into :statechart-definitions
                  :values [{:src (src->str src)
                            :definition (core/freeze definition)}]
                  :on-conflict [:src]
                  :do-update-set {:definition :excluded.definition
                                  :version [:+ :statechart-definitions.version 1]
                                  :updated-at [:now]}})
  true)

;; -----------------------------------------------------------------------------
;; StatechartRegistry Implementation
;; -----------------------------------------------------------------------------

(defrecord PostgresStatechartRegistry [pool cache]
  sp/StatechartRegistry
  (register-statechart! [_ src statechart-definition]
    ;; Update database
    (if (pool/pool? pool)
      (pg/with-connection [c pool]
        (upsert-definition! c src statechart-definition))
      (upsert-definition! pool src statechart-definition))
    ;; Update cache
    (swap! cache assoc src statechart-definition)
    (log/info "Statechart registered"
              {:src src
               :states (count (filter #(= :state (:node-type %))
                                      (tree-seq map? :children statechart-definition)))})
    nil)

  (get-statechart [_ src]
    ;; Check cache first
    (if-let [chart (get @cache src)]
      (do
        (log/trace "Statechart cache hit" {:src src})
        chart)
      ;; Not in cache, check database
      (let [chart (if (pool/pool? pool)
                    (pg/with-connection [c pool]
                      (fetch-definition c src))
                    (fetch-definition pool src))]
        (if chart
          (do
            ;; Populate cache
            (swap! cache assoc src chart)
            (log/debug "Statechart cache miss, loaded from DB" {:src src})
            chart)
          (do
            (log/trace "Statechart not found" {:src src})
            nil)))))

  (all-charts [_]
    ;; Always fetch from database for complete picture
    (let [charts (if (pool/pool? pool)
                   (pg/with-connection [c pool]
                     (fetch-all-definitions c))
                   (fetch-all-definitions pool))]
      ;; Update cache with all charts
      (reset! cache charts)
      (log/debug "Loaded all statecharts" {:count (count charts)})
      charts)))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn new-registry
  "Create a new PostgreSQL-backed statechart registry.

   pool - pg2 connection pool"
  [pool]
  (->PostgresStatechartRegistry pool (atom {})))

(defn clear-cache!
  "Clear the local cache. Useful after external database modifications."
  [registry]
  (let [count (count @(:cache registry))]
    (reset! (:cache registry) {})
    (log/debug "Registry cache cleared" {:previous-count count}))
  nil)

(defn preload-cache!
  "Preload all chart definitions into the cache.
   Returns the registry for chaining."
  [registry]
  (let [charts (sp/all-charts registry)]
    (log/info "Registry cache preloaded" {:count (count charts)}))
  registry)
