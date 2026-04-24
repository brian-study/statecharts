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
;; ::sc/id Serialization (shared by session-id, invoke-id, and durable jobs)
;; -----------------------------------------------------------------------------
;;
;; `::sc/id` is `[:or uuid? number? keyword? string?]`. Each JDBC subsystem
;; (session-id, queued events, durable jobs) stores these identifiers as a
;; shape-inspecting text form. Before 2.0.17 each subsystem had its own
;; encoder and decoder with slightly different rules, and edge cases
;; (ratios, bigints, big decimals, tagged-numeric keywords) kept slipping
;; through whichever site had been patched least recently. `encode-id` and
;; `decode-id` below are the single source of truth; the public wrappers
;; `session-id->str` / `str->session-id` and the private helpers in
;; `event_queue.clj` / `job_store.clj` configure them via options for the
;; legacy on-disk shapes they must remain compatible with.

(def tagged-number-re
  "Matches Clojure literals produced by pr-str for numeric subtypes whose
   type would otherwise be lost by `parse-long` / `parse-double`:

       42N, -42N, 99999999999N        — BigInt
       3.14M, -0.001M, 0M             — plain BigDecimal
       1E+10M, 9.99E+50M, 1.5E-10M    — scientific BigDecimal (Clojure
                                         produces these outside a narrow
                                         magnitude window)
       1/2, -3/7                      — Ratio

   **Public API since 2.0.17.** Safe to reference from out-of-library
   callers; regex contents may grow (to cover more `number?` subtypes)
   but will not shrink."
  #"-?\d+(\.\d+)?([Ee][+-]?\d+)?[NM]|-?\d+/-?\d+")

(defn- numeric-bare-name?
  "Would the bare name parse as a Long or Double? If so, a keyword/symbol
   whose name is `bare` needs the leading `:` via pr-str so the decoder
   can tell it apart from a real number. Referenced by
   `:keyword-shape :bare-unless-numeric`."
  [^String bare]
  (boolean (or (parse-long bare) (parse-double bare))))

(defn encode-id
  "Serialize an `::sc/id` value to a string for DB storage.

   Options:
   - `:uuid-shape`    — `:bare` (just `(str uuid)`) or `:tagged`
                        (`\"#uuid \\\"…\\\"\"`). Sessions use `:bare` for
                        back-compat with pre-2.0.11 rows; invoke-ids
                        use `:tagged`.
   - `:keyword-shape` — `:marked` (always `pr-str`, yields `\":kw\"`) or
                        `:bare-unless-numeric` (bare name unless it would
                        parse as a number, in which case `pr-str`).
                        Sessions use `:marked`; durable jobs use
                        `:bare-unless-numeric` for back-compat with
                        pre-2.0.8 rows that stored keyword invoke-ids
                        via `(name x)`.
   - `:symbol-shape`  — same split as `:keyword-shape` (symbols aren't in
                        `::sc/id` but some callers pass them through).

   Strings, numbers, and anything else valid go through `pr-str`.
   BigInt/BigDecimal/Ratio round-trip because `pr-str` preserves their
   `N` / `M` / ratio markers; plain Long/Double pr-str to the same text
   as `(str n)` so on-disk shape is unchanged for them.

   `nil` returns `nil` (pre-2.0.17 `session-id->str` returned `\"\"` via
   `(str nil)`; the new behaviour surfaces the bug at the caller instead
   of silently writing an empty-string row — all session-id columns are
   `NOT NULL`).

   **Public API since 2.0.17.** The option set is stable; new options
   may be added (with defaults that preserve current behaviour) but
   existing ones will not be removed."
  [x {:keys [uuid-shape keyword-shape symbol-shape]
      :or {uuid-shape    :bare
           keyword-shape :marked
           symbol-shape  :marked}}]
  (when x
    (cond
      (string? x)  (pr-str x)
      (keyword? x) (case keyword-shape
                     :marked              (pr-str x)
                     :bare-unless-numeric (let [bare (subs (str x) 1)]
                                            (if (numeric-bare-name? bare)
                                              (pr-str x)
                                              bare)))
      (symbol? x)  (case symbol-shape
                     :marked              (pr-str x)
                     :bare-unless-numeric (let [bare (str x)]
                                            (if (numeric-bare-name? bare)
                                              (pr-str x)
                                              bare)))
      (uuid? x)    (case uuid-shape
                     :bare   (str x)
                     :tagged (pr-str x))
      (number? x)  (pr-str x)
      ;; `::sc/id` is closed over string/number/keyword/uuid, so :else is
      ;; only reached by invalid input. pr-str is the most recoverable
      ;; fallback — matches the pre-2.0.17 invoke-id encoder.
      :else        (pr-str x))))

(defn decode-id
  "Deserialize a DB string back to the original `::sc/id` value.

   Shape-inspecting inverse of `encode-id`. The one option,
   `:legacy-fallback`, covers two per-site asymmetries that exist
   because each subsystem's writer has a different history:

   - `:legacy-fallback :string`  — **session-id mode.**
     Enables bare-UUID recognition (`parse-uuid`) because `session-id->str`
     has always written UUIDs bare for pre-2.0.11 compatibility.
     Disables `#`-tagged-literal read because session-id writers have
     never produced `#` forms; a pre-2.0.11 bare string whose content
     starts with `#` (e.g. `\"#foo\"` or `\"#{1 2 3}\"`) stays a string.
     A bare row that isn't a number or UUID falls back to the string
     itself.

   - `:legacy-fallback :keyword` — **invoke-id mode.**
     Enables `#`-tagged-literal read (needed so `#uuid \"…\"` produced
     by `invokeid->str` / `event->row` decodes back to a UUID).
     Disables `parse-uuid` because pre-2.0.4 / pre-2.0.8 invoke-id
     writers used `(name x)` which only accepts keywords/strings, and a
     UUID-shaped *bare* row therefore represents a keyword whose name
     was UUID-shaped (e.g. `(keyword \"550e8400-…\")`). A bare
     non-numeric row falls back to `(keyword s)`.

   Migration note: a bare row whose content is digit-only (e.g. `\"42\"`)
   decodes as a Long regardless of fallback choice — that migration note
   is in CHANGELOG 2.0.13 for sessions and applies equally to
   invoke-ids.

   **Public API since 2.0.17.** See `encode-id` for the stability
   contract."
  [s {:keys [legacy-fallback]
      :or {legacy-fallback :string}}]
  (when s
    (cond
      (.startsWith ^String s "\"") (try (edn/read-string s) (catch Exception _ s))
      (.startsWith ^String s ":")  (try (edn/read-string s) (catch Exception _ s))
      ;; `#`-tagged literal — only enabled for invoke-id-style callers,
      ;; because session-id writers never produced `#` forms and a
      ;; pre-2.0.11 bare legacy row starting with `#` (e.g. `"#foo"`,
      ;; `"#{1 2 3}"`) must continue to decode as the raw string.
      (and (.startsWith ^String s "#") (= :keyword legacy-fallback))
      (try (edn/read-string s) (catch Exception _ s))
      :else
      ;; Parser order is load-bearing: `parse-long` first (cheapest and
      ;; most common); `parse-double` catches scientific/decimal; the
      ;; `:string`-mode `parse-uuid` must come after the number parsers
      ;; because bare UUIDs could not otherwise tell themselves apart
      ;; from "arbitrary alphanumeric string" cheaply; `tagged-number-re`
      ;; catches `42N` / `3.14M` / `1/2`; finally the fallback.
      (or (parse-long s)
          (parse-double s)
          ;; `:string` mode: session-id wrote bare UUIDs pre-2.0.11.
          ;; `:keyword` mode: invoke-id never wrote bare UUIDs (all
          ;; UUIDs go through `#uuid`); a UUID-shaped bare row here is
          ;; a legacy keyword whose name happened to be UUID-shaped.
          (when (= :string legacy-fallback) (parse-uuid s))
          (when (re-matches tagged-number-re s)
            (try (edn/read-string s) (catch Exception _ nil)))
          (case legacy-fallback
            :keyword (keyword s)
            :string  s)))))

(defn session-id->str
  "Serialize a session-id for DB storage. See `encode-id` for the scheme."
  [session-id]
  (encode-id session-id {:uuid-shape :bare :keyword-shape :marked :symbol-shape :marked}))

(defn str->session-id
  "Deserialize a DB session-id string. See `decode-id` for the scheme.

   Migration note (2.0.13): legacy pre-2.0.11 rows that stored a *string*
   session-id whose contents were digit-only (e.g. `\"12345\"`) will now
   decode as the number `12345`. Post-2.0.11 rows are unaffected because
   strings carry the pr-str quote marker."
  [s]
  (decode-id s {:legacy-fallback :string}))

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
    (map? result) (long (or (:next.jdbc/update-count result) 0))
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

   Semantics match `next.jdbc/with-transaction`.

   **Do not nest `with-tx` calls.** `next.jdbc/with-transaction`'s nested
   semantics are treacherous (default `:commit` writes the inner body
   even if the outer block throws; `:ignore` makes the inner block a
   no-op but still looks like a transaction to readers; `:savepoint`
   is untested in this codebase). If a helper needs to run inside a
   caller's existing transaction, expose it as an `in-tx` variant that
   takes the open `Connection` directly — see `job-store/cancel-by-session-in-tx!`
   for the pattern."
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
