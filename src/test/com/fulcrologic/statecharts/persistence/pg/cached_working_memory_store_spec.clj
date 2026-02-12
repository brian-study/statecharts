(ns com.fulcrologic.statecharts.persistence.pg.cached-working-memory-store-spec
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.persistence.pg.cached-working-memory-store :as cwms]
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.protocols :as sp]
   [com.fulcrologic.statecharts.working-memory-store.local-memory-store :as local-store]))

(defn- sid-key
  [session-id]
  (core/session-id->str session-id))

(defn- make-wmem
  [session-id rev]
  {::sc/session-id session-id
   ::sc/statechart-src :chart/test
   ::sc/configuration #{:state/active}
   ::sc/initialized-states #{:state/active}
   ::sc/history-value {}
   ::sc/running? true
   ::sc/data-model {:rev rev}})

(defn- versioned
  [wmem version]
  (core/attach-version wmem version))

(defn- new-counting-delegate
  []
  (let [delegate (local-store/new-store)
        get-count (atom 0)
        save-count (atom 0)
        delete-count (atom 0)
        fail-save? (atom false)
        store
        (reify sp/WorkingMemoryStore
          (get-working-memory [_ env session-id]
            (swap! get-count inc)
            (sp/get-working-memory delegate env session-id))
          (save-working-memory! [_ env session-id wmem]
            (swap! save-count inc)
            (if @fail-save?
              (throw (ex-info "save failure" {:session-id session-id}))
              (sp/save-working-memory! delegate env session-id wmem)))
          (delete-working-memory! [_ env session-id]
            (swap! delete-count inc)
            (sp/delete-working-memory! delegate env session-id)))]
    {:store store
     :delegate delegate
     :get-count get-count
     :save-count save-count
     :delete-count delete-count
     :fail-save? fail-save?}))

(deftest cache-miss-then-hit-test
  (let [{:keys [store delegate get-count]} (new-counting-delegate)
        sid :session-1
        wmem (make-wmem sid 0)
        cache-store (cwms/new-caching-store store 8)]
    (sp/save-working-memory! delegate {} sid wmem)
    (is (= wmem (sp/get-working-memory cache-store {} sid)))
    (is (= wmem (sp/get-working-memory cache-store {} sid)))
    (is (= 1 @get-count))
    (is (= {:hits 1 :misses 1 :hit-rate 0.5 :size 1 :max-size 8}
           (cwms/cache-stats cache-store)))))

(deftest write-through-versioned-save-test
  (let [{:keys [store get-count save-count]} (new-counting-delegate)
        sid :session-2
        cache-store (cwms/new-caching-store store 8)
        initial (versioned (make-wmem sid 0) 1)]
    (sp/save-working-memory! cache-store {} sid initial)
    (let [cached (sp/get-working-memory cache-store {} sid)]
      (is (= 2 (core/get-version cached)))
      (is (= 0 @get-count))
      (is (= 1 @save-count))
      (is (= sid (::sc/session-id cached))))))

(deftest evict-on-unversioned-save-test
  (let [{:keys [store delegate get-count]} (new-counting-delegate)
        sid :session-3
        cache-store (cwms/new-caching-store store 8)]
    (sp/save-working-memory! delegate {} sid (versioned (make-wmem sid 0) 3))
    (sp/get-working-memory cache-store {} sid) ; populate cache
    (reset! get-count 0)
    (sp/save-working-memory! cache-store {} sid (make-wmem sid 1))
    (is (nil? (get @(:cache cache-store) (sid-key sid))))
    (is (= 1 (:rev (::sc/data-model (sp/get-working-memory cache-store {} sid)))))
    (is (= 1 @get-count))))

(deftest monotonic-version-guard-test
  (let [{:keys [store]} (new-counting-delegate)
        sid :session-4
        cache-store (cwms/new-caching-store store 8)
        newer (versioned (make-wmem sid 0) 6)
        stale (versioned (make-wmem sid 0) 3)]
    (#'cwms/put-entry cache-store sid newer)
    (#'cwms/maybe-put-entry cache-store sid stale)
    (is (= 6 (some-> (get @(:cache cache-store) (sid-key sid)) :wmem core/get-version)))))

(deftest evict-on-save-failure-test
  (let [{:keys [store fail-save?]} (new-counting-delegate)
        sid :session-5
        cache-store (cwms/new-caching-store store 8)
        initial (versioned (make-wmem sid 0) 1)
        failing (versioned (make-wmem sid 1) 2)]
    (sp/save-working-memory! cache-store {} sid initial)
    (reset! fail-save? true)
    (is (thrown? clojure.lang.ExceptionInfo
                 (sp/save-working-memory! cache-store {} sid failing)))
    (is (nil? (get @(:cache cache-store) (sid-key sid))))))

(deftest lru-eviction-test
  (let [{:keys [store]} (new-counting-delegate)
        cache-store (cwms/new-caching-store store 3)]
    (doseq [sid [:s1 :s2 :s3 :s4]]
      (sp/save-working-memory! cache-store {} sid (versioned (make-wmem sid 0) 1)))
    (let [keys-in-cache (set (keys @(:cache cache-store)))]
      (is (= #{(sid-key :s2) (sid-key :s3) (sid-key :s4)} keys-in-cache))
      (is (not (contains? keys-in-cache (sid-key :s1)))))))

(deftest delete-evicts-test
  (let [{:keys [store delete-count]} (new-counting-delegate)
        sid :session-6
        cache-store (cwms/new-caching-store store 8)]
    (sp/save-working-memory! cache-store {} sid (versioned (make-wmem sid 0) 1))
    (is (some? (get @(:cache cache-store) (sid-key sid))))
    (sp/delete-working-memory! cache-store {} sid)
    (is (nil? (get @(:cache cache-store) (sid-key sid))))
    (is (= 1 @delete-count))))

(deftest stats-tracking-test
  (let [{:keys [store delegate]} (new-counting-delegate)
        sid :session-7
        cache-store (cwms/new-caching-store store 8)]
    (sp/save-working-memory! delegate {} sid (make-wmem sid 0))
    (sp/get-working-memory cache-store {} sid)          ; miss
    (sp/get-working-memory cache-store {} sid)          ; hit
    (sp/get-working-memory cache-store {} :missing-sid) ; miss
    (is (= {:hits 1 :misses 2 :hit-rate (/ 1.0 3.0) :size 1 :max-size 8}
           (cwms/cache-stats cache-store)))))

(deftest invalidate-unconditional-test
  (let [{:keys [store]} (new-counting-delegate)
        sid :session-8
        cache-store (cwms/new-caching-store store 8)]
    (sp/save-working-memory! cache-store {} sid (versioned (make-wmem sid 0) 1))
    (is (true? (cwms/invalidate! cache-store sid)))
    (is (false? (cwms/invalidate! cache-store sid)))
    (is (nil? (get @(:cache cache-store) (sid-key sid))))))

(deftest invalidate-if-remote-test
  (let [{:keys [store]} (new-counting-delegate)
        sid :session-9
        cache-store (cwms/new-caching-store store 8)]
    (sp/save-working-memory! cache-store {} sid (versioned (make-wmem sid 0) 1))
    (is (false? (cwms/invalidate-if-remote! cache-store sid)))
    (is (some? (get @(:cache cache-store) (sid-key sid))))
    (is (true? (cwms/invalidate-if-remote! cache-store sid)))
    (is (nil? (get @(:cache cache-store) (sid-key sid))))))

(deftest self-notify-simulation-test
  (let [{:keys [store]} (new-counting-delegate)
        sid :session-10
        cache-store (cwms/new-caching-store store 8)]
    (sp/save-working-memory! cache-store {} sid (versioned (make-wmem sid 0) 1))
    (is (= false (cwms/invalidate-if-remote! cache-store sid)))
    (is (= true (cwms/invalidate-if-remote! cache-store sid)))))

(deftest lock-free-read-no-atom-mutation-test
  (let [{:keys [store]} (new-counting-delegate)
        sid :session-11
        cache-store (cwms/new-caching-store store 8)]
    (sp/save-working-memory! cache-store {} sid (versioned (make-wmem sid 0) 1))
    (let [before @(:cache cache-store)]
      (sp/get-working-memory cache-store {} sid)
      (is (identical? before @(:cache cache-store)))
      (is (= 1 (:hits (cwms/cache-stats cache-store)))))))

(deftest clear-cache-and-reset-stats-test
  (let [{:keys [store delegate]} (new-counting-delegate)
        sid :session-12
        cache-store (cwms/new-caching-store store 8)]
    (sp/save-working-memory! delegate {} sid (make-wmem sid 0))
    (sp/get-working-memory cache-store {} sid)
    (sp/get-working-memory cache-store {} sid)
    (is (= true (cwms/clear-cache! cache-store)))
    (is (= 0 (:size (cwms/cache-stats cache-store))))
    (is (= true (cwms/reset-stats! cache-store)))
    (is (= 0 (:hits (cwms/cache-stats cache-store))))
    (is (= 0 (:misses (cwms/cache-stats cache-store))))))
