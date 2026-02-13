(ns com.fulcrologic.statecharts.persistence.pg.core
  "Core utilities for PostgreSQL persistence layer.

   Provides:
   - Session ID serialization/deserialization
   - Nippy-based binary serialization for Clojure data
   - HoneySQL query execution wrappers"
  (:require
   [clojure.edn :as edn]
   [pg.core :as pg]
   [pg.honey :as pgh]
   [pg.pool :as pool]
   [taoensso.nippy :as nippy]
   [taoensso.timbre :as log]))

;; -----------------------------------------------------------------------------
;; Session ID Serialization
;; -----------------------------------------------------------------------------

(defn session-id->str
  "Convert a session ID to a string for database storage.
   Supports UUIDs, keywords, symbols, numbers, and strings."
  [session-id]
  (cond
    (string? session-id) session-id
    (keyword? session-id) (pr-str session-id)
    (symbol? session-id) (pr-str session-id)
    (uuid? session-id) (str session-id)
    :else (str session-id)))

(defn str->session-id
  "Convert a string from database back to original session ID type.
   Attempts to read EDN if it looks like a keyword/symbol."
  [s]
  (when s
    (if (or (.startsWith s ":")
            (.startsWith s "'"))
      (edn/read-string s)
      ;; Try UUID, fall back to string
      (or (parse-uuid s) s))))

;; -----------------------------------------------------------------------------
;; Binary Serialization (nippy)
;; -----------------------------------------------------------------------------
;;
;; We use nippy for binary serialization of Clojure data to PostgreSQL BYTEA.
;; This preserves all Clojure types (sets, keywords, symbols, etc.) without
;; any conversion logic. Much simpler than JSONB with type tagging.
;;
;; IMPORTANT: The pg2 connection pool MUST be configured with:
;;   :binary-encode? true
;;   :binary-decode? true
;; Without binary protocol, bytea columns return as Base64 strings which
;; is inefficient and indicates a misconfigured connection.

(defn freeze
  "Serialize Clojure data to bytes for PostgreSQL BYTEA storage.
   Preserves all Clojure types exactly."
  [data]
  (nippy/freeze data))

(defn thaw
  "Deserialize bytes from PostgreSQL BYTEA back to Clojure data.
   Returns nil for nil input.

   Requires pg2 pool configured with :binary-decode? true.
   Throws if bytea is returned as String (indicates misconfigured pool)."
  [input]
  (when input
    (when-not (bytes? input)
      (throw (ex-info "bytea column returned as String - pg2 pool must have :binary-decode? true"
                      {:type (type input)
                       :hint "Configure pool with (pg/pool (assoc config :binary-decode? true))"})))
    (nippy/thaw input)))

;; -----------------------------------------------------------------------------
;; Query Execution
;; -----------------------------------------------------------------------------

(defn execute!
  "Execute a HoneySQL query and return all result rows.
   Takes either a connection or a pool.

   conn-or-pool - connection or connection pool
   hsql - HoneySQL map"
  [conn-or-pool hsql]
  (let [[sql & params] (pgh/format hsql)]
    (log/debug "execute!" {:sql sql :params params})
    (try
      (if (pool/pool? conn-or-pool)
        (pg/with-connection [c conn-or-pool]
          (pg/execute c sql {:params (vec params)
                             :kebab-keys? true}))
        (pg/execute conn-or-pool sql {:params (vec params)
                                      :kebab-keys? true}))
      (catch NullPointerException e
        (log/error e "NPE in pg2 execute!"
                   {:sql sql
                    :param-count (count params)
                    :param-types (mapv type params)
                    :thread (.getName (Thread/currentThread))
                    :conn-type (type conn-or-pool)})
        (throw e)))))

(defn execute-one!
  "Execute a HoneySQL query and return the first result row (or nil).

   conn-or-pool - connection or connection pool
   hsql - HoneySQL map"
  [conn-or-pool hsql]
  (first (execute! conn-or-pool hsql)))

(defn affected-row-count
  "Return the number of affected rows from execute!/pg2 results.

   pg2 versions may return either:
   - a row sequence (e.g. INSERT ... RETURNING / UPDATE ... RETURNING)
   - a summary map (e.g. {:updated n}, {:deleted n}, {:inserted n})"
  [result]
  (cond
    (nil? result) 0
    (number? result) result
    (map? result) (long (or (:updated result)
                            (:deleted result)
                            (:inserted result)
                            (:next.jdbc/update-count result)
                            0))
    (sequential? result) (count result)
    :else 0))

;; -----------------------------------------------------------------------------
;; Optimistic Locking Support
;; -----------------------------------------------------------------------------

(def ^:private version-key ::version)

(defn attach-version
  "Attach version metadata to working memory for optimistic locking."
  [wmem version]
  (when wmem
    (with-meta wmem {version-key version})))

(defn get-version
  "Get the version from working memory metadata."
  [wmem]
  (get (meta wmem) version-key))
