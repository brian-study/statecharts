(ns com.fulcrologic.statecharts.invocation.durable-job
  "Durable job invocation processor for statecharts.

   Jobs are persisted to PostgreSQL and survive server restarts. A background
   worker claims and executes jobs, dispatching done/error events back to
   the statechart session.

   The `src` attribute of the invoke element must be a keyword (e.g., :quiz-content-generation)
   that maps to a handler function in the worker's registry. This is unlike :future/:missionary
   where `src` is a resolved function."
  (:require
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.data-model.operations :as ops]
   [com.fulcrologic.statecharts.environment :as env]
   [com.fulcrologic.statecharts.persistence.pg.job-store :as job-store]
   [com.fulcrologic.statecharts.protocols :as sp]
   [taoensso.timbre :as log]))

(defn- invoke-data-keys
  "Returns the data model keys for tracking a durable job invoke.
   Keys are namespaced by invokeid to support multiple concurrent invokes."
  [invokeid]
  (let [ns-str (name invokeid)]
    {:job-id-key  (keyword ns-str "job-id")
     :job-kind-key (keyword ns-str "job-kind")}))

(defrecord DurableJobInvocationProcessor [pool]
  sp/InvocationProcessor
  (supports-invocation-type? [_ typ] (= :durable-job typ))

  (start-invocation! [_ env {:keys [invokeid src params]}]
    (let [session-id (env/session-id env)
          new-id     (random-uuid)
          job-type   (if (keyword? src) (name src) (str src))
          job-id     (job-store/create-job! pool
                       {:id           new-id
                        :session-id   session-id
                        :invokeid     invokeid
                        :job-type     job-type
                        :payload      params})
          {:keys [job-id-key job-kind-key]} (invoke-data-keys invokeid)]
      (log/info "Durable job created"
                {:job-id job-id :session-id session-id :invokeid invokeid :job-type job-type})
      (sp/update! (::sc/data-model env) env
        {:ops [(ops/assign job-id-key (str job-id)
                           job-kind-key job-type)]})))

  (stop-invocation! [_ env {:keys [invokeid]}]
    (let [session-id (env/session-id env)]
      (log/debug "Cancelling durable job" {:session-id session-id :invokeid invokeid})
      (job-store/cancel! pool session-id invokeid)))

  (forward-event! [_ _ _] nil))
