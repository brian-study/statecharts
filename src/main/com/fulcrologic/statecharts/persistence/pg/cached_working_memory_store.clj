(ns com.fulcrologic.statecharts.persistence.pg.cached-working-memory-store
  "Lock-free-read LRU cache decorator for WorkingMemoryStore.

   Cache semantics:
   - Read hit: lock-free map lookup + atomic hit counter increment.
   - Read miss: delegate fetch + populate cache (monotonic version guard).
   - Save (versioned): write-through with incremented version metadata.
   - Save (unversioned): evict entry (authoritative DB version is unknown).
   - Save failure/delete: evict entry.

   The cache also tracks recent local versioned saves so external NOTIFY handlers
   can skip self-echo invalidation via `invalidate-if-remote!`."
  (:require
   [com.fulcrologic.statecharts.persistence.pg.core :as core]
   [com.fulcrologic.statecharts.protocols :as sp])
  (:import
   (java.util.concurrent ConcurrentHashMap)
   (java.util.concurrent.atomic AtomicLong)))

(defn- session-key
  [session-id]
  (core/session-id->str session-id))

(defn- next-access!
  ^long [^AtomicLong op-counter]
  (.incrementAndGet op-counter))

(defn- evict-lru-until-size
  [entries max-size]
  (loop [m entries]
    (if (<= (count m) max-size)
      m
      (let [[victim-key _]
            (reduce-kv
             (fn [[best-k best-ts] k {:keys [accessed-at]}]
               (let [ts (long (or accessed-at 0))]
                 (if (or (nil? best-k) (< ts best-ts))
                   [k ts]
                   [best-k best-ts])))
             [nil Long/MAX_VALUE]
             m)]
        (recur (if victim-key
                 (dissoc m victim-key)
                 m))))))

(defn- monotonic-replacement?
  "True when replacing `existing` with `candidate` preserves monotonic versions."
  [existing candidate]
  (if-not existing
    true
    (let [existing-version (some-> existing :wmem core/get-version)
          candidate-version (core/get-version candidate)]
      (cond
        ;; If existing has a known version and candidate doesn't, keep existing.
        (and (some? existing-version) (nil? candidate-version)) false
        ;; If both known, only allow equal/newer.
        (and (some? existing-version) (some? candidate-version))
        (>= (long candidate-version) (long existing-version))
        :else true))))

(defn- put-entry
  [{:keys [cache max-size op-counter] :as _store} session-id wmem]
  (let [sid (session-key session-id)
        ts (next-access! ^AtomicLong op-counter)]
    (swap! cache
           (fn [entries]
             (evict-lru-until-size
              (assoc entries sid {:wmem wmem :accessed-at ts})
              max-size)))))

(defn- maybe-put-entry
  [{:keys [cache max-size op-counter] :as _store} session-id wmem]
  (let [sid (session-key session-id)
        ts (next-access! ^AtomicLong op-counter)]
    (swap! cache
           (fn [entries]
             (let [existing (get entries sid)]
               (if (monotonic-replacement? existing wmem)
                 (evict-lru-until-size
                  (assoc entries sid {:wmem wmem :accessed-at ts})
                  max-size)
                 entries))))))

(defn- evict-entry!
  [{:keys [cache]} session-id]
  (let [sid (session-key session-id)]
    (swap! cache dissoc sid)))

(defrecord CachingWorkingMemoryStore
  [delegate
   cache
   max-size
   op-counter
   hit-counter
   miss-counter
   recent-saves]
  sp/WorkingMemoryStore
  (get-working-memory [this env session-id]
    (let [sid (session-key session-id)
          cached-entry (get @cache sid)]
      (if cached-entry
        (do
          (.incrementAndGet ^AtomicLong hit-counter)
          (:wmem cached-entry))
        (do
          (.incrementAndGet ^AtomicLong miss-counter)
          (let [loaded (sp/get-working-memory delegate env session-id)]
            (when loaded
              (maybe-put-entry this sid loaded))
            loaded)))))

  (save-working-memory! [this env session-id wmem]
    (let [sid (session-key session-id)
          expected-version (core/get-version wmem)]
      (try
        (let [result (sp/save-working-memory! delegate env session-id wmem)]
          (if (some? expected-version)
            (let [next-version (inc (long expected-version))
                  cached-wmem (core/attach-version wmem next-version)]
              (put-entry this sid cached-wmem)
              (.put ^ConcurrentHashMap recent-saves sid Boolean/TRUE))
            (evict-entry! this sid))
          result)
        (catch Throwable t
          (evict-entry! this sid)
          (throw t)))))

  (delete-working-memory! [this env session-id]
    (let [sid (session-key session-id)]
      (try
        (sp/delete-working-memory! delegate env session-id)
        (finally
          (evict-entry! this sid)
          (.remove ^ConcurrentHashMap recent-saves sid))))))

(defn new-caching-store
  "Create a cache-decorated WorkingMemoryStore.

   max-size must be a positive integer."
  [delegate max-size]
  (let [max-size' (long max-size)]
    (when-not (pos? max-size')
      (throw (ex-info "max-size must be positive"
                      {:max-size max-size})))
    (->CachingWorkingMemoryStore delegate
                                 (atom {})
                                 max-size'
                                 (AtomicLong. 0)
                                 (AtomicLong. 0)
                                 (AtomicLong. 0)
                                 (ConcurrentHashMap.))))

(defn cache-stats
  "Return cache statistics."
  [{:keys [cache max-size hit-counter miss-counter]}]
  (let [hits (.get ^AtomicLong hit-counter)
        misses (.get ^AtomicLong miss-counter)
        total (+ hits misses)]
    {:hits hits
     :misses misses
     :hit-rate (if (pos? total) (/ (double hits) (double total)) 0.0)
     :size (count @cache)
     :max-size max-size}))

(defn invalidate!
  "Unconditionally evict a session from the cache.
   Returns true when an entry was removed."
  [{:keys [cache recent-saves]} session-id]
  (let [sid (session-key session-id)
        removed? (contains? @cache sid)]
    (swap! cache dissoc sid)
    (.remove ^ConcurrentHashMap recent-saves sid)
    removed?))

(defn invalidate-if-remote!
  "Evict session from cache unless a local recent-save token exists.

   Returns:
   - false when token existed (self-echo; no invalidation)
   - true when invalidation was applied for remote save"
  [{:keys [recent-saves] :as store} session-id]
  (let [sid (session-key session-id)
        local-token (.remove ^ConcurrentHashMap recent-saves sid)]
    (if local-token
      false
      (do
        (invalidate! store sid)
        true))))

(defn clear-cache!
  "Drop all cached entries."
  [{:keys [cache recent-saves]}]
  (reset! cache {})
  (.clear ^ConcurrentHashMap recent-saves)
  true)

(defn reset-stats!
  "Reset hit/miss counters to zero."
  [{:keys [hit-counter miss-counter]}]
  (.set ^AtomicLong hit-counter 0)
  (.set ^AtomicLong miss-counter 0)
  true)
