(ns com.fulcrologic.statecharts.persistence.jdbc.registry
  "A JDBC-backed statechart registry with local caching.

   Chart definitions are stored in the database and cached in memory
   for fast lookups."
  (:require
   [clojure.edn :as edn]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.protocols :as sp]
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
    (edn/read-string s)))

(defn- fetch-definition
  "Fetch a chart definition by src."
  [ds src]
  (when-let [row (core/execute-one! ds
                                    {:select [:definition]
                                     :from [:statechart-definitions]
                                     :where [:= :src (src->str src)]})]
    (core/thaw (:definition row))))

(defn- fetch-all-definitions
  "Fetch all chart definitions."
  [ds]
  (let [rows (core/execute! ds
                            {:select [:src :definition]
                             :from [:statechart-definitions]})]
    (into {}
          (map (fn [row]
                 [(str->src (:src row))
                  (core/thaw (:definition row))]))
          rows)))

(defn- upsert-definition!
  "Insert or update a chart definition."
  [ds src definition]
  (core/execute! ds
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

(defrecord JdbcStatechartRegistry [datasource cache]
  sp/StatechartRegistry
  (register-statechart! [_ src statechart-definition]
    ;; Update database
    (upsert-definition! datasource src statechart-definition)
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
      (let [chart (fetch-definition datasource src)]
        (if chart
          (do
            (swap! cache assoc src chart)
            (log/debug "Statechart cache miss, loaded from DB" {:src src})
            chart)
          (do
            (log/trace "Statechart not found" {:src src})
            nil)))))

  (all-charts [_]
    ;; Always fetch from database for complete picture
    (let [charts (fetch-all-definitions datasource)]
      (reset! cache charts)
      (log/debug "Loaded all statecharts" {:count (count charts)})
      charts)))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn new-registry
  "Create a new JDBC-backed statechart registry.

   datasource - a javax.sql.DataSource (HikariCP is the standard choice) or a
                java.sql.Connection."
  [datasource]
  (->JdbcStatechartRegistry datasource (atom {})))

(defn clear-cache!
  "Clear the local cache. Useful after external database modifications."
  [registry]
  (let [n (count @(:cache registry))]
    (reset! (:cache registry) {})
    (log/debug "Registry cache cleared" {:previous-count n}))
  nil)

(defn preload-cache!
  "Preload all chart definitions into the cache.
   Returns the registry for chaining."
  [registry]
  (let [charts (sp/all-charts registry)]
    (log/info "Registry cache preloaded" {:count (count charts)}))
  registry)
