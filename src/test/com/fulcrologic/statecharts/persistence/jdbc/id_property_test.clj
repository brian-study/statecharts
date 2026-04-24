(ns com.fulcrologic.statecharts.persistence.jdbc.id-property-test
  "Property-based round-trip tests for `::sc/id` encode/decode.

   The JDBC persistence layer encodes three kinds of ID across three
   subsystems (session-id, event-queue invoke-id, durable-job invoke-id),
   each with its own options into the shared `core/encode-id` /
   `core/decode-id` implementation. Every release from 2.0.13 through
   2.0.16 patched a newly-discovered `::sc/id` subtype (BigInt, BigDecimal,
   Ratio, scientific BigDecimals, tagged-numeric keywords, digit-starting
   keywords, UUID-shaped strings, …) one case at a time; each fix was an
   enumerated-example test.

   This namespace closes the loop with a generator over the full
   `::sc/id` shape `[:or uuid? number? keyword? string?]` that exercises
   every encode/decode pair. A future encoder edge case fails here before
   it slips through the three subsystem wrappers."
  (:require
   [clojure.test :refer [deftest is]]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [com.fulcrologic.statecharts.persistence.jdbc.core :as core]
   [com.fulcrologic.statecharts.persistence.jdbc.event-queue :as pg-eq]
   [com.fulcrologic.statecharts.persistence.jdbc.job-store :as job-store])
  (:import
   [java.util UUID]))

;; -----------------------------------------------------------------------------
;; Generators for ::sc/id ≡ [:or uuid? number? keyword? string?]
;; -----------------------------------------------------------------------------

(def gen-number-subtype
  "Long, Double, BigInt, BigDecimal, and Ratio — every `number?` flavor
   the CHANGELOG had to patch one-by-one.

   Excludes `##NaN` / `##Inf` / `##-Inf`. NaN isn't equal to itself by
   `=`, so any round-trip 'fails' trivially; Inf encodings are
   pathological as IDs and out-of-scope for the encoders (no caller
   passes them through `::sc/id`). The `finite?` filter on `gen/double`
   removes them."
  (gen/one-of
    [gen/small-integer
     gen/large-integer
     (gen/such-that (fn [^double d] (Double/isFinite d)) gen/double)
     (gen/fmap bigint gen/small-integer)
     (gen/fmap #(java.math.BigDecimal. ^int %) gen/small-integer)
     ;; Scientific-notation BigDecimals (2.0.15 regex extension). Use
     ;; `movePointRight` so we stay in the BigDecimal-valid input space
     ;; instead of hand-formatting a string.
     (gen/fmap (fn [[unscaled exp]]
                 (.movePointRight (java.math.BigDecimal. ^int unscaled) ^int exp))
               (gen/tuple gen/small-integer (gen/choose 10 50)))
     ;; Ratios only exist when `n/d` doesn't simplify to an integer.
     ;; `(/ n d)` in Clojure may yield a Long (0, 3, -6/2=-3, etc.) — we
     ;; filter to keep only genuine `ratio?` values.
     (gen/such-that ratio?
                    (gen/fmap (fn [[n d]] (/ (biginteger n) (biginteger d)))
                              (gen/tuple gen/small-integer
                                         (gen/such-that (complement zero?) gen/small-integer))))]))

(def gen-keyword-id
  "Simple keywords, namespaced keywords, and digit-starting (numeric-
   looking) keywords that 2.0.9 had to round-trip with a pr-str marker."
  (gen/one-of
    [gen/keyword
     gen/keyword-ns
     ;; Digit-starting name: (keyword (str int)) — must keep its keyword identity.
     (gen/fmap (fn [n] (keyword (str n))) gen/small-integer)]))

(def gen-string-id
  "Arbitrary strings, including shapes that collide with other branches
   of `::sc/id` — digit-only, leading `:`, UUID-shaped, `#uuid \"…\"`."
  (gen/one-of
    [gen/string-alphanumeric
     (gen/fmap str gen/small-integer)            ;; digit-only
     (gen/fmap #(str ":" %) gen/string-alphanumeric) ;; leading colon
     (gen/return (str (UUID/randomUUID)))
     (gen/return (pr-str (UUID/randomUUID)))]))

(def gen-uuid (gen/fmap (fn [_] (UUID/randomUUID)) (gen/return nil)))

(def gen-id
  "`::sc/id` ≡ `[:or uuid? number? keyword? string?]`."
  (gen/one-of [gen-uuid gen-number-subtype gen-keyword-id gen-string-id]))

;; -----------------------------------------------------------------------------
;; Round-trip property per subsystem
;; -----------------------------------------------------------------------------

(def ^:private prop-runs
  "Number of examples per property. Kept modest so the unit suite still
   runs fast; bump locally when debugging a generator gap."
  200)

(deftest session-id-round-trip-property-test
  (let [result (tc/quick-check
                 prop-runs
                 (prop/for-all [id gen-id]
                   (let [encoded (core/session-id->str id)
                         decoded (core/str->session-id encoded)]
                     (= id decoded))))]
    (is (:result result)
        (str "session-id encode/decode must round-trip for every ::sc/id subtype, "
             "including every number? flavor and digit-starting keyword. "
             "Failure: " (:fail result) ", shrunk: " (:shrunk result)))))

(deftest event-queue-invoke-id-round-trip-property-test
  ;; `event_queue/parse-invoke-id` is a thin wrapper that decodes via
  ;; `core/decode-id` with `:legacy-fallback :keyword`. Inputs come from
  ;; `event->row`'s `(pr-str invoke-id)` writer — so we encode through
  ;; `pr-str` to match what the writer produces on disk.
  (let [result (tc/quick-check
                 prop-runs
                 (prop/for-all [id gen-id]
                   (let [encoded (pr-str id)
                         decoded (#'pg-eq/parse-invoke-id encoded)]
                     (= id decoded))))]
    (is (:result result)
        (str "event-queue invoke-id must round-trip via pr-str + parse-invoke-id. "
             "Failure: " (:fail result) ", shrunk: " (:shrunk result)))))

(deftest job-store-invoke-id-round-trip-property-test
  (let [result (tc/quick-check
                 prop-runs
                 (prop/for-all [id gen-id]
                   (let [encoded (job-store/invokeid->str id)
                         decoded (job-store/str->invokeid encoded)]
                     (= id decoded))))]
    (is (:result result)
        (str "durable-job invoke-id must round-trip via invokeid->str + str->invokeid. "
             "Failure: " (:fail result) ", shrunk: " (:shrunk result)))))

;; -----------------------------------------------------------------------------
;; Cross-encoder consistency — all three subsystems must agree on the
;; *type* of a legacy bare row. Historically session-id and event-queue
;; decoded bare rows to different shapes, breaking `=` matches during
;; invocation bookkeeping.
;; -----------------------------------------------------------------------------

(deftest bare-legacy-row-decoders-agree-on-keyword-for-invoke-ids-test
  ;; Both invoke-id sites use `:legacy-fallback :keyword`: a bare
  ;; non-numeric, non-UUID-shaped legacy row must decode as a keyword
  ;; in both event-queue and job-store.
  (let [result (tc/quick-check
                 prop-runs
                 (prop/for-all [n gen/keyword]
                   (let [bare-name (name n)]
                     (= (#'pg-eq/parse-invoke-id bare-name)
                        (job-store/str->invokeid bare-name)))))]
    (is (:result result)
        (str "event-queue and job-store legacy bare-row decoders must agree. "
             "Failure: " (:fail result) ", shrunk: " (:shrunk result)))))
