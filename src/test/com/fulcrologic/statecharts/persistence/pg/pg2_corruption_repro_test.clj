(ns com.fulcrologic.statecharts.persistence.pg.pg2-corruption-repro-test
  "Reproduction tests for pg2 0.1.41 connection corruption via readTypesProcess.

   ## Background

   pg2's Connection.readTypesProcess() (Connection.java:226-272) uses the COPY
   protocol to read type metadata from pg_type. Its message loop only handles:
   CopyData, CopyOutResponse, CopyDone, CommandComplete, ReadyForQuery, and
   ErrorResponse. Any other message (NoticeResponse, ParameterStatus) causes:

       throw new PGError(\"Unexpected message in readTypes: %s\", msg)

   This error does NOT consume ReadyForQuery, leaving bytes in the TCP input
   stream. Pool.returnConnection() (Pool.java:199-267) does not validate stream
   state -- it only checks isTxError(), isTransaction(), isClosed(). So the
   connection returns to the pool with residual bytes in its InputStream.

   The next borrower of that connection reads stale bytes, interpreting them as
   responses to its own query. This causes:
   - NPE at Connection.sendBind:735 when parameterDescription is null
   - \"Wrong parameters count\" when a stale ParameterDescription from a
     different query is misread

   ## Trigger mechanism

   VOID (OID 2278) is NOT pre-registered in pg2's Processors.oidMap (see
   Processors.java:64-142). `select pg_notify($1, $2)` returns VOID. When
   a connection first encounters VOID in RowDescription or ParameterDescription:

   1. unsupportedOids() identifies 2278 as unknown
   2. setTypesByOids() calls readTypesOids() with {2278}
   3. readTypesOids() builds a COPY query and calls readTypesProcess()
   4. If PostgreSQL sends NoticeResponse during this COPY (e.g., from a
      concurrent NOTIFY, GC warning, or config reload), the stream corrupts

   ## What these tests verify

   Test 1: VOID type resolution occurs on pg_notify and doesn't corrupt the
   connection on its own (no concurrent NoticeResponse). This is the baseline.

   Test 2: Under concurrent load with NOTIFY + mixed queries, pool connections
   may become corrupted. This reproduces the production failure pattern.

   Test 3: Direct verification that readTypesProcess throws on VOID resolution
   when a NoticeResponse arrives, and that the connection is poisoned afterward.

   ## Running

   Requires a running PostgreSQL instance. See integration_test.clj for config.
   Run with: clj -M:test -m kaocha.runner --focus :integration"
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [pg.core :as pg])
  (:import
   [java.util.concurrent CountDownLatch TimeUnit CyclicBarrier]
   [org.pg.enums OID]
   [org.pg.error PGError]
   [org.pg Pool Connection]))

;; ---------------------------------------------------------------------------
;; Test Configuration
;; ---------------------------------------------------------------------------

(def ^:private test-config
  "Database config matching production pool settings.
   :binary-encode? true :binary-decode? true matches our production config.
   :ps-cache? false disables prepared statement caching (also matches prod)."
  {:host (or (System/getenv "PG_TEST_HOST") "127.0.0.1")
   :port (parse-long (or (System/getenv "PG_TEST_PORT") "5432"))
   :database (or (System/getenv "PG_TEST_DATABASE") "brian")
   :user (or (System/getenv "PG_TEST_USER") "user")
   :password (or (System/getenv "PG_TEST_PASSWORD") "password")
   :binary-encode? true
   :binary-decode? true
   :ps-cache? false})

;; ---------------------------------------------------------------------------
;; Fixtures
;; ---------------------------------------------------------------------------

(def ^:dynamic *pool* nil)

(defn with-pool [f]
  (let [pool-config (assoc test-config
                           :pool-min-size 2
                           :pool-max-size 3)
        p (pg/pool pool-config)]
    (try
      (binding [*pool* p]
        (f))
      (finally
        (pg/close p)))))

(use-fixtures :each with-pool)

;; ---------------------------------------------------------------------------
;; Test 1: VOID type resolution via pg_notify
;; ---------------------------------------------------------------------------
;;
;; Verify that calling pg_notify (which returns VOID, OID 2278) triggers
;; type resolution via readTypesProcess. On a fresh connection, the VOID OID
;; is not pre-registered in pg2's Processors.oidMap, so the first execution
;; of `select pg_notify($1, $2)` will call:
;;
;;   prepareUnlocked -> unsupportedOids(oidsRD, oidsPD) -> setTypesByOids({2278})
;;     -> readTypesOids -> readTypesProcess (COPY protocol to pg_type)
;;
;; If readTypesProcess completes without receiving a NoticeResponse or
;; ParameterStatus, the connection should remain healthy. The subsequent
;; query should work fine.
;;
;; If the connection IS corrupted, the second query will throw one of:
;;   - NullPointerException at Connection.sendBind (null parameterDescription)
;;   - PGError "Wrong parameters count" (stale ParameterDescription)
;;

(deftest ^:integration void-type-resolution-via-notify-test
  (testing "VOID OID (2278) is not pre-registered in pg2"
    ;; Confirm the precondition: VOID is not in the static processor map.
    ;; If pg2 ever adds VOID to Processors, this test becomes a no-op
    ;; (the bug would be avoided because readTypesProcess wouldn't be called).
    (is (= 2278 OID/VOID)
        "VOID OID constant should be 2278")
    (is (nil? (org.pg.processor.Processors/getProcessor OID/VOID))
        "VOID should NOT be pre-registered in pg2's processor map"))

  (testing "pg_notify triggers VOID type resolution without corruption"
    ;; Use a direct connection (not pool) to control the lifecycle.
    (pg/with-connection [conn test-config]
      ;; First call: triggers readTypesProcess for VOID OID 2278.
      ;; pg_notify returns VOID which is not in the known OID set.
      ;; After prepareUnlocked, unsupportedOids will contain 2278,
      ;; causing setTypesByOids -> readTypesOids -> readTypesProcess.
      (let [notify-result (pg/execute conn
                                      "select pg_notify($1, $2)"
                                      {:params ["test_channel" "test_message"]})]
        (is (some? notify-result)
            "pg_notify should execute successfully"))

      ;; Second call on the SAME connection: if readTypesProcess corrupted
      ;; the stream, this query will fail with NPE or wrong param count.
      (let [result (pg/execute conn
                               "select $1::text as val"
                               {:params ["hello"]})]
        (is (= "hello" (:val (first result)))
            "Query after pg_notify should work (connection not corrupted)")))))

(deftest ^:integration void-type-resolution-pool-sequential-test
  (testing "sequential pg_notify + query on pooled connections"
    ;; With a pool, the same connection may serve pg_notify then a regular query.
    ;; This tests the production scenario: session save uses pg_notify,
    ;; then the connection returns to pool and is reused for a different query.
    (dotimes [i 10]
      ;; Each iteration borrows a connection, does pg_notify, returns it,
      ;; then borrows (possibly same) connection for a regular query.
      (pg/with-connection [c1 *pool*]
        (pg/execute c1
                    "select pg_notify($1, $2)"
                    {:params ["test_ch" (str "msg-" i)]}))

      ;; Now borrow again - may get the same connection
      (pg/with-connection [c2 *pool*]
        (let [result (pg/execute c2
                                 "select $1::int as num, $2::text as txt"
                                 {:params [42 "test"]})]
          (is (= 42 (:num (first result)))
              (str "Iteration " i ": query after notify should work"))
          (is (= "test" (:txt (first result)))
              (str "Iteration " i ": all params should bind correctly")))))))

;; ---------------------------------------------------------------------------
;; Test 2: Concurrent stress with NOTIFY + mixed queries
;; ---------------------------------------------------------------------------
;;
;; This simulates the production workload where:
;;   - Session saves use 4 parameters (INSERT/UPDATE with session_id, chart,
;;     wmem bytes, version)
;;   - Session fetches use 1 parameter (SELECT by session_id)
;;   - Notifications use 2 parameters (pg_notify(channel, payload))
;;
;; With a small pool (2-3 connections), threads contend for connections.
;; If readTypesProcess corrupts a connection during VOID type resolution,
;; a subsequent borrower may get a poisoned connection where:
;;   - The InputStream has unconsumed bytes from the previous query flow
;;   - readMessage() interprets those bytes as responses to the new query
;;   - ParameterDescription from old query mismatches new query's param count
;;
;; The concurrent NOTIFY calls are important because pg_notify causes PostgreSQL
;; to deliver NoticeResponse/NotificationResponse to OTHER connections that are
;; LISTENing (though in readTypesProcess, even normal concurrent activity on
;; the server can cause ParameterStatus messages for GUC changes).
;;

(deftest ^:integration concurrent-notify-and-mixed-queries-test
  (testing "concurrent pg_notify + queries with varying param counts"
    (let [thread-count 6
          iterations 50
          barrier (CyclicBarrier. thread-count)
          errors (atom [])
          done-latch (CountDownLatch. thread-count)
          ;; Capture pool value -- dynamic bindings don't propagate to threads
          pool *pool*]

      ;; Create a LISTEN on a dedicated connection to increase the chance
      ;; of NoticeResponse/NotificationResponse flowing during readTypesProcess.
      ;; This connection stays outside the pool.
      (pg/with-connection [listener-conn test-config]
        (pg/listen listener-conn "stress_test_channel")

        ;; Launch worker threads
        (doseq [thread-id (range thread-count)]
          (.start
           (Thread.
            (fn []
              (try
                (.await barrier) ;; synchronize start
                (dotimes [iter iterations]
                  (try
                    (case (mod (+ thread-id iter) 3)
                      ;; Pattern 1: pg_notify (2 params, returns VOID)
                      0 (pg/with-connection [c pool]
                          (pg/execute c
                                      "select pg_notify($1, $2)"
                                      {:params ["stress_test_channel"
                                                (str "t" thread-id "-i" iter)]}))

                      ;; Pattern 2: 1-param query (simulates session fetch)
                      1 (pg/with-connection [c pool]
                          (let [result (pg/execute c
                                                   "select $1::text as sid"
                                                   {:params [(str "session-" thread-id)]})]
                            (when-not (= (str "session-" thread-id)
                                         (:sid (first result)))
                              (swap! errors conj
                                     {:type :wrong-result
                                      :thread thread-id
                                      :iter iter
                                      :expected (str "session-" thread-id)
                                      :actual (:sid (first result))}))))

                      ;; Pattern 3: 4-param query (simulates session save)
                      2 (pg/with-connection [c pool]
                          (let [result (pg/execute c
                                                   "select $1::text as sid, $2::text as chart, $3::bytea as data, $4::int as ver"
                                                   {:params [(str "session-" thread-id)
                                                             "my-chart"
                                                             (.getBytes "fake-wmem-data")
                                                             (inc iter)]})]
                            (when-not (= (str "session-" thread-id)
                                         (:sid (first result)))
                              (swap! errors conj
                                     {:type :wrong-result
                                      :thread thread-id
                                      :iter iter})))))
                    (catch NullPointerException e
                      ;; NPE at Connection.sendBind when parameterDescription is null.
                      ;; This is the primary corruption symptom.
                      (swap! errors conj
                             {:type :npe
                              :thread thread-id
                              :iter iter
                              :message (.getMessage e)
                              :stacktrace (mapv str (take 5 (.getStackTrace e)))}))
                    (catch PGError e
                      ;; "Wrong parameters count" or "Unexpected message in readTypes"
                      (swap! errors conj
                             {:type :pg-error
                              :thread thread-id
                              :iter iter
                              :message (.getMessage e)}))
                    (catch Exception e
                      (swap! errors conj
                             {:type :other
                              :thread thread-id
                              :iter iter
                              :class (type e)
                              :message (.getMessage e)}))))
                (catch Exception e
                  (swap! errors conj
                         {:type :thread-failure
                          :thread thread-id
                          :class (type e)
                          :message (.getMessage e)}))
                (finally
                  (.countDown done-latch))))
            (str "pg2-stress-" thread-id))))

        ;; Wait for all threads to complete (generous timeout)
        (let [completed? (.await done-latch 60 TimeUnit/SECONDS)]
          (is completed? "All threads should complete within 60 seconds"))

        ;; Drain notifications from the listener
        (pg/poll-notifications listener-conn))

      ;; Report errors. In the buggy pg2 version, we expect NPE or PGError.
      ;; If pg2 is fixed or the race doesn't trigger, this test passes cleanly.
      ;;
      ;; NOTE: This test may pass intermittently even with the bug present,
      ;; because the corruption requires a NoticeResponse to arrive during
      ;; the readTypesProcess COPY flow, which is a race condition.
      (when (seq @errors)
        (let [by-type (group-by :type @errors)]
          (println "\n=== pg2 corruption repro results ===")
          (println "Total errors:" (count @errors))
          (doseq [[err-type errs] by-type]
            (println (str "  " err-type ": " (count errs) " occurrences"))
            (when-let [sample (first errs)]
              (println "    Sample:" (select-keys sample [:message :class :stacktrace]))))
          (println "====================================\n")

          ;; The test PASSES if no errors occurred (happy path).
          ;; If errors DID occur, this assertion will fail, confirming the bug.
          ;; Specifically, NPE and PGError("Wrong parameters count") are the
          ;; corruption signatures we're looking for.
          (is (empty? (filter #(#{:npe :pg-error} (:type %)) @errors))
              (str "Pool connections should not be corrupted. "
                   "Found " (count (filter #(#{:npe :pg-error} (:type %)) @errors))
                   " corruption errors (NPE or PGError). "
                   "This confirms the pg2 readTypesProcess bug.")))))))

;; ---------------------------------------------------------------------------
;; Test 3: Forced readTypesProcess failure via VOID OID check
;; ---------------------------------------------------------------------------
;;
;; This test verifies the THEORY by:
;; 1. Confirming VOID triggers readTypesProcess on first encounter
;; 2. Verifying connection health after VOID type resolution
;; 3. Checking that InputStream.available() == 0 after clean operations
;;    (residual bytes indicate stream corruption)
;;
;; We also attempt to provoke NoticeResponse during readTypesProcess by
;; using SET commands that generate ParameterStatus, though this is not
;; guaranteed to arrive during the narrow readTypesProcess window.
;;

(deftest ^:integration connection-stream-health-after-void-resolution-test
  (testing "connection stream has no residual bytes after VOID type resolution"
    (pg/with-connection [conn test-config]
      ;; Trigger VOID type resolution
      (pg/execute conn
                  "select pg_notify($1, $2)"
                  {:params ["health_check_ch" "test"]})

      ;; After a clean operation, the InputStream should have 0 available bytes.
      ;; If readTypesProcess left unconsumed messages, available() > 0.
      ;;
      ;; We access the private inStream field via reflection. This is fragile
      ;; but necessary to verify the corruption theory at the stream level.
      (let [in-stream-field (try
                              (let [f (.getDeclaredField Connection "inStream")]
                                (.setAccessible f true)
                                f)
                              (catch NoSuchFieldException _
                                ;; Field name may differ across pg2 versions
                                nil))]
        (when in-stream-field
          (let [^java.io.InputStream in-stream (.get in-stream-field conn)
                available (.available in-stream)]
            (is (zero? available)
                (str "InputStream should have 0 bytes available after clean "
                     "VOID resolution, but has " available " bytes. "
                     "Residual bytes indicate stream corruption.")))))))

  (testing "connection remains usable after multiple VOID-returning queries"
    (pg/with-connection [conn test-config]
      ;; Execute pg_notify multiple times. Each should work cleanly because
      ;; after the first call, VOID is cached in codecParams.oidMap and
      ;; readTypesProcess is NOT called again for that connection.
      (dotimes [i 5]
        (pg/execute conn
                    "select pg_notify($1, $2)"
                    {:params ["repeat_ch" (str "msg-" i)]}))

      ;; After VOID is cached, readTypesProcess should not be invoked again.
      ;; Verify the connection is still healthy with various param counts.
      (let [r1 (pg/execute conn "select 1 as n" {})
            r2 (pg/execute conn "select $1::text as v" {:params ["ok"]})
            r3 (pg/execute conn
                           "select $1::int as a, $2::text as b, $3::bool as c"
                           {:params [99 "test" true]})]
        (is (= 1 (:n (first r1))))
        (is (= "ok" (:v (first r2))))
        (is (= 99 (:a (first r3))))
        (is (= "test" (:b (first r3))))
        (is (= true (:c (first r3))))))))

(deftest ^:integration provoke-notice-during-type-resolution-test
  (testing "attempt to provoke NoticeResponse during readTypesProcess"
    ;; Strategy: create a scenario where PostgreSQL is likely to send
    ;; NoticeResponse or ParameterStatus messages asynchronously.
    ;;
    ;; We use two connections:
    ;; 1. One connection continuously generates notices by doing SET/RESET
    ;;    and RAISE NOTICE via DO blocks
    ;; 2. The other connection does pg_notify (triggering readTypesProcess
    ;;    for VOID on first use)
    ;;
    ;; The goal is to have the notice-generating activity cause NoticeResponse
    ;; messages on the wire while readTypesProcess is running its COPY loop.
    ;;
    ;; NOTE: In practice, NoticeResponse from OTHER connections don't arrive
    ;; on THIS connection's wire. PostgreSQL delivers notices only to the
    ;; connection that generated them. However, ParameterStatus CAN arrive
    ;; asynchronously (e.g., from pg_reload_conf() or GUC changes).
    ;;
    ;; This test is a best-effort attempt. The real corruption is most reliably
    ;; triggered when:
    ;; - A NOTIFY arrives on a LISTENing connection during readTypesProcess
    ;; - A server-side ParameterStatus change occurs (e.g., timezone change)
    ;; - The connection's own pg_notify generates a self-notification
    ;;
    ;; We try the self-notification approach: LISTEN on a channel, then
    ;; pg_notify on the same channel. The NotificationResponse (or
    ;; NoticeResponse) might arrive during readTypesProcess's COPY flow.
    (let [errors (atom [])
          iterations 100]

      ;; Each iteration uses a FRESH connection (no pool) so VOID is always
      ;; unknown, forcing readTypesProcess every time.
      (dotimes [i iterations]
        (try
          (pg/with-connection [conn test-config]
            ;; LISTEN on the channel we're about to notify.
            ;; This means the server will queue a NotificationResponse
            ;; for THIS connection when we call pg_notify.
            (pg/listen conn "self_notify_channel")

            ;; Now call pg_notify. This triggers:
            ;; 1. execute("select pg_notify($1, $2)", params)
            ;; 2. Inside execute -> prepareUnlocked -> unsupportedOids finds VOID
            ;; 3. setTypesByOids -> readTypesProcess (COPY flow)
            ;; 4. Meanwhile, the server may deliver NotificationResponse for
            ;;    our LISTEN, potentially during readTypesProcess
            (pg/execute conn
                        "select pg_notify($1, $2)"
                        {:params ["self_notify_channel" (str "self-msg-" i)]})

            ;; If we survived, verify connection health
            (let [result (pg/execute conn
                                     "select $1::int as v"
                                     {:params [i]})]
              (when-not (= i (:v (first result)))
                (swap! errors conj
                       {:type :wrong-result
                        :iter i
                        :expected i
                        :actual (:v (first result))}))))
          (catch NullPointerException e
            (swap! errors conj
                   {:type :npe
                    :iter i
                    :message (.getMessage e)
                    :stacktrace (mapv str (take 5 (.getStackTrace e)))}))
          (catch PGError e
            (swap! errors conj
                   {:type :pg-error
                    :iter i
                    :message (.getMessage e)}))
          (catch Exception e
            (swap! errors conj
                   {:type :other
                    :iter i
                    :class (type e)
                    :message (.getMessage e)}))))

      (when (seq @errors)
        (let [corruption-errors (filter #(#{:npe :pg-error} (:type %)) @errors)]
          (println "\n=== Self-notify corruption test results ===")
          (println "Total errors:" (count @errors))
          (println "Corruption errors (NPE/PGError):" (count corruption-errors))
          (when-let [sample (first corruption-errors)]
            (println "Sample:" (pr-str sample)))
          (println "============================================\n")

          (is (empty? corruption-errors)
              (str "Self-notify should not corrupt connections. "
                   "Found " (count corruption-errors) " corruption errors. "
                   "This would confirm readTypesProcess vulnerability to "
                   "NotificationResponse during COPY flow.")))))))

(deftest ^:integration pool-connection-reuse-after-void-test
  (testing "pool connections remain healthy across VOID type transitions"
    ;; This test specifically targets the Pool.returnConnection() gap:
    ;; returnConnection checks isTxError, isTransaction, isClosed but NOT
    ;; whether the InputStream has residual bytes.
    ;;
    ;; We use a pool of size 1 to guarantee the SAME connection is reused.
    (let [tiny-pool-config (assoc test-config
                                  :pool-min-size 1
                                  :pool-max-size 1)
          tiny-pool (pg/pool tiny-pool-config)]
      (try
        ;; Phase 1: pg_notify borrows and returns the connection
        (pg/with-connection [c1 tiny-pool]
          (pg/execute c1
                      "select pg_notify($1, $2)"
                      {:params ["reuse_ch" "phase1"]}))

        ;; Phase 2: Regular query borrows the SAME connection
        ;; If phase 1 corrupted the stream, this will fail.
        (pg/with-connection [c2 tiny-pool]
          (let [result (pg/execute c2
                                   "select $1::int as a, $2::text as b"
                                   {:params [42 "hello"]})]
            (is (= 42 (:a (first result)))
                "Reused connection should handle 2-param query correctly")
            (is (= "hello" (:b (first result)))
                "All parameters should bind to correct values")))

        ;; Phase 3: Query with different param count
        (pg/with-connection [c3 tiny-pool]
          (let [result (pg/execute c3
                                   "select $1::text as x, $2::int as y, $3::bool as z, $4::text as w"
                                   {:params ["test" 99 false "end"]})]
            (is (= "test" (:x (first result)))
                "4-param query should work after previous 2-param pg_notify")
            (is (= 99 (:y (first result))))
            (is (= false (:z (first result))))
            (is (= "end" (:w (first result))))))
        (finally
          (pg/close tiny-pool))))))
