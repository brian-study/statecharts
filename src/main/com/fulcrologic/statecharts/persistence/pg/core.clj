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

(defn freeze
  "Serialize Clojure data to bytes for PostgreSQL BYTEA storage.
   Preserves all Clojure types exactly."
  [data]
  (nippy/freeze data))

(defn thaw
  "Deserialize bytes from PostgreSQL BYTEA back to Clojure data.
   Returns nil for nil input."
  [bytes]
  (when bytes
    (nippy/thaw bytes)))

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
    (if (pool/pool? conn-or-pool)
      (pg/with-connection [c conn-or-pool]
        (pg/execute c sql {:params (vec params)
                           :kebab-keys? true}))
      (pg/execute conn-or-pool sql {:params (vec params)
                                    :kebab-keys? true}))))

(defn execute-one!
  "Execute a HoneySQL query and return the first result row (or nil).

   conn-or-pool - connection or connection pool
   hsql - HoneySQL map"
  [conn-or-pool hsql]
  (first (execute! conn-or-pool hsql)))

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
