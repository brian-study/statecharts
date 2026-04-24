(ns com.fulcrologic.statecharts.persistence.jdbc.core
  "Core utilities for the JDBC persistence layer.

   Provides:
   - Session ID serialization/deserialization
   - Nippy-based binary serialization for Clojure data
   - HoneySQL + next.jdbc query execution wrappers

   Consumers supply a javax.sql.DataSource (HikariCP is the standard choice)."
  (:require
   [clojure.edn :as edn]
   [honey.sql :as sql]
   [next.jdbc :as jdbc]
   [next.jdbc.date-time]
   [next.jdbc.result-set :as rs]
   [taoensso.nippy :as nippy]
   [taoensso.timbre :as log])
  (:import
   [java.sql ResultSet]
   [java.time ZoneOffset]))

;; Loading `next.jdbc.date-time` registers parameter-binding extensions for
;; java.time types (OffsetDateTime/Instant/etc.) so call sites can pass them
;; through without manual conversion.

;; -----------------------------------------------------------------------------
;; Session ID Serialization
;; -----------------------------------------------------------------------------

(defn session-id->str
  "Convert a session ID to a string for database storage.

   Keywords (`:kw` / `:ns/kw`) and symbols are stored via `pr-str` so
   they keep their leading `:` / namespace / symbol markers. UUIDs are
   stored in their bare string form (no tag) — back-compat with pre-
   2.0.11 deployments. Strings are stored via `pr-str` (quoted) so the
   decoder can distinguish a user-supplied string that happens to look
   like a UUID or keyword from a genuine UUID / keyword session id."
  [session-id]
  (cond
    (string? session-id)  (pr-str session-id)
    (keyword? session-id) (pr-str session-id)
    (symbol? session-id)  (pr-str session-id)
    (uuid? session-id)    (str session-id)
    :else                 (str session-id)))

(defn str->session-id
  "Convert a string from database back to original session ID type.

   Shape-inspecting inverse of `session-id->str`:
   - leading `\"` → EDN read (quoted string form)
   - leading `:` → EDN read (keyword)
   - parses as long/double → number (SCXML `::sc/id` allows numbers)
   - looks like a UUID → UUID
   - otherwise → bare string (legacy rows pre-2.0.11 that stored strings
     unquoted)."
  [s]
  (when s
    (cond
      (.startsWith ^String s "\"") (try (edn/read-string s) (catch Exception _ s))
      (.startsWith ^String s ":")  (try (edn/read-string s) (catch Exception _ s))
      :else                        (or (parse-long s) (parse-double s) (parse-uuid s) s))))

;; -----------------------------------------------------------------------------
;; Binary Serialization (nippy)
;; -----------------------------------------------------------------------------
;;
;; nippy is used for binary serialization of Clojure data to PostgreSQL BYTEA.
;; This preserves all Clojure types (sets, keywords, symbols, etc.) without any
;; conversion logic. pgjdbc returns BYTEA columns as `byte[]` natively, so no
;; special datasource configuration is required.

(defn freeze
  "Serialize Clojure data to bytes for PostgreSQL BYTEA storage.
   Preserves all Clojure types exactly."
  [data]
  (nippy/freeze data))

(defn thaw
  "Deserialize bytes from PostgreSQL BYTEA back to Clojure data.
   Returns nil for nil input."
  [input]
  (when input
    (when-not (bytes? input)
      (throw (ex-info "bytea column returned as non-byte[]; expected byte[] from pgjdbc"
                      {:type (type input)})))
    (nippy/thaw input)))

;; -----------------------------------------------------------------------------
;; Result Set Builder
;; -----------------------------------------------------------------------------
;;
;; Column values go through a scoped builder so `TIMESTAMPTZ` columns (returned
;; by pgjdbc as `java.sql.Timestamp`) are converted to `java.time.OffsetDateTime`
;; — matching the shape the prior pg2 backend returned. Scoping via
;; `builder-adapter` avoids globally extending `rs/ReadableColumn`, which would
;; otherwise conflict with consumers (e.g. Brian) that install their own
;; `Timestamp` extensions on the same JVM.

(defn- column-by-index
  [builder ^ResultSet rs ^Integer i]
  (let [v (.getObject rs i)]
    (if (instance? java.sql.Timestamp v)
      (.atOffset (.toInstant ^java.sql.Timestamp v) ZoneOffset/UTC)
      (rs/read-column-by-index v (:rsmeta builder) i))))

(def ^:private kebab-odt-builder
  (rs/builder-adapter rs/as-unqualified-kebab-maps column-by-index))

(def ^:private result-opts
  "Produce unqualified kebab-case result-set keys (e.g. :session-id, :event-name)
   with `TIMESTAMPTZ` columns read as `OffsetDateTime` (not `java.sql.Timestamp`)."
  {:builder-fn kebab-odt-builder})

;; -----------------------------------------------------------------------------
;; Query Execution
;; -----------------------------------------------------------------------------

(defn execute!
  "Execute a HoneySQL query and return all result rows as a vector.

   ds-or-conn - javax.sql.DataSource or java.sql.Connection
   hsql       - HoneySQL map

   For UPDATE/DELETE/INSERT without a RETURNING clause, next.jdbc returns
   `[{:next.jdbc/update-count N}]`. Use `affected-row-count` to get N."
  [ds-or-conn hsql]
  (let [[sql-str & params :as sql+params] (sql/format hsql)]
    (log/debug "execute!" {:sql sql-str :params params})
    (try
      (jdbc/execute! ds-or-conn sql+params result-opts)
      (catch Exception e
        (log/error e "execute! failed"
                   {:sql sql-str
                    :param-count (count params)
                    :param-types (mapv type params)
                    :thread (.getName (Thread/currentThread))
                    :ds-type (type ds-or-conn)})
        (throw e)))))

(defn execute-one!
  "Execute a HoneySQL query and return the first result row (or nil)."
  [ds-or-conn hsql]
  (let [[sql-str & params :as sql+params] (sql/format hsql)]
    (log/debug "execute-one!" {:sql sql-str :params params})
    (try
      (jdbc/execute-one! ds-or-conn sql+params result-opts)
      (catch Exception e
        (log/error e "execute-one! failed"
                   {:sql sql-str
                    :param-count (count params)
                    :param-types (mapv type params)
                    :thread (.getName (Thread/currentThread))
                    :ds-type (type ds-or-conn)})
        (throw e)))))

(defn execute-sql!
  "Execute raw SQL with positional `?` parameters.

   Use this for DDL, SQL that HoneySQL can't express cleanly, or places where
   the SQL is already built (e.g. SELECT FOR UPDATE SKIP LOCKED subqueries)."
  ([ds-or-conn sql-str]
   (execute-sql! ds-or-conn sql-str nil))
  ([ds-or-conn sql-str params]
   (let [sql+params (into [sql-str] params)]
     (log/debug "execute-sql!" {:sql sql-str :params params})
     (try
       (jdbc/execute! ds-or-conn sql+params result-opts)
       (catch Exception e
         (log/error e "execute-sql! failed"
                    {:sql sql-str
                     :param-count (count params)
                     :param-types (mapv type params)
                     :thread (.getName (Thread/currentThread))
                     :ds-type (type ds-or-conn)})
         (throw e))))))

(defn execute-sql-one!
  "Execute raw SQL with positional `?` parameters and return the first row (or nil)."
  ([ds-or-conn sql-str]
   (execute-sql-one! ds-or-conn sql-str nil))
  ([ds-or-conn sql-str params]
   (let [sql+params (into [sql-str] params)]
     (jdbc/execute-one! ds-or-conn sql+params result-opts))))

(defn affected-row-count
  "Return the number of affected rows from an `execute!`/`execute-one!` result.

   next.jdbc returns:
   - `[{:next.jdbc/update-count N}]` from execute! on mutations without RETURNING
   - `{:next.jdbc/update-count N}`   from execute-one! on the same
   - a vector of rows                from execute! with RETURNING (or SELECT)
   - a single row or nil             from execute-one!"
  [result]
  (cond
    (nil? result) 0
    (number? result) result
    (map? result) (long (or (:next.jdbc/update-count result)
                            (:updated result)
                            (:deleted result)
                            (:inserted result)
                            0))
    (sequential? result)
    (let [n (count result)]
      (if (and (= 1 n)
               (map? (first result))
               (contains? (first result) :next.jdbc/update-count))
        (long (:next.jdbc/update-count (first result)))
        n))
    :else 0))

;; -----------------------------------------------------------------------------
;; Transactions
;; -----------------------------------------------------------------------------

(defmacro with-tx
  "Open a transaction on `src` (DataSource or Connection) and bind `sym` to a
   tx-bound connection for the duration of body.

       (with-tx [tx ds]
         (do-stuff tx))

   Semantics match `next.jdbc/with-transaction`."
  [[sym src] & body]
  `(jdbc/with-transaction [~sym ~src]
     ~@body))

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
