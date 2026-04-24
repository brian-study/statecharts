(ns com.fulcrologic.statecharts.persistence.jdbc.fixtures
  "Shared test fixtures for the PostgreSQL persistence layer.

   Before 2.0.21 the `with-pool` / `with-clean-tables` pair + `test-config`
   map were copy-pasted verbatim across four files (chaos_test.clj,
   integration_test.clj, job_store_spec.clj, regression_test.clj). A
   PG config change required updating four places. This namespace is the
   single source of truth; test namespaces `use-fixtures` from here.

   Environment variables:
   - PG_TEST_HOST     (default: localhost)
   - PG_TEST_PORT     (default: 5432)
   - PG_TEST_DATABASE (default: statecharts_test)
   - PG_TEST_USER     (default: postgres)
   - PG_TEST_PASSWORD (default: postgres)"
  (:require
   [com.fulcrologic.statecharts.persistence.jdbc.schema :as schema]
   [next.jdbc.connection :as jdbc.connection])
  (:import
   [com.zaxxer.hikari HikariDataSource]))

(def test-config
  "HikariCP config for the test DB. Overridable via env vars."
  {:dbtype   "postgres"
   :dbname   (or (System/getenv "PG_TEST_DATABASE") "statecharts_test")
   :host     (or (System/getenv "PG_TEST_HOST") "localhost")
   :port     (parse-long (or (System/getenv "PG_TEST_PORT") "5432"))
   :username (or (System/getenv "PG_TEST_USER") "postgres")
   :password (or (System/getenv "PG_TEST_PASSWORD") "postgres")})

(def ^:dynamic *pool*
  "Bound to a HikariDataSource for the duration of the test suite via
   `with-pool`. Test namespaces either `use-fixtures :once` this
   namespace's `with-pool` or `alter-var-root` their own pool var."
  nil)

(defn with-pool
  "`:once` fixture: stand up a Hikari pool, bind `*pool*` for the
   duration of `f`, and close the pool on exit."
  [f]
  (let [ds (jdbc.connection/->pool HikariDataSource test-config)]
    (try
      (binding [*pool* ds]
        (f))
      (finally
        (.close ^HikariDataSource ds)))))

(defn with-clean-tables
  "`:each` fixture: ensure the schema exists, truncate before the test,
   and truncate again on the way out so a test that leaves rows behind
   doesn't pollute the next one."
  [f]
  (schema/create-tables! *pool*)
  (schema/truncate-tables! *pool*)
  (try
    (f)
    (finally
      (schema/truncate-tables! *pool*))))
