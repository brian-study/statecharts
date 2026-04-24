(ns com.fulcrologic.statecharts.invocation.future
  "Support for invoking futures (CLJ only) from statecharts. This support can be added by
   adding it to the env's `::sc/invocation-processors`. An invoke element can specify it wants to
   run a function in a future specifying the type as :future.

   The `src` attribute of the `invoke` element must be a lambda that takes one argument (the
   params of the invocation) and returns a map, which will be sent back to the invoking machine
   as the data of a `:done.invoke.invokeid` event.

   If the invocation is cancelled (the parent state is left), then future-cancel will be called on the future.
   "
  (:require
   [com.fulcrologic.statecharts :as sc]
   [com.fulcrologic.statecharts.environment :as env]
   [com.fulcrologic.statecharts.events :as evts]
   [com.fulcrologic.statecharts.protocols :as sp]
   [taoensso.timbre :as log]))

(defn- cancellation-exception?
  "True if the Throwable (or anything in its cause chain) is an
   Interrupted/Cancellation exception, or the current thread is
   interrupted.

   Walks `.getCause` because many libraries wrap InterruptedException in
   ExecutionException / CompletionException / domain wrappers before it
   reaches our catch. An `instance?` check on the top-level alone would
   misclassify those as real failures."
  [^Throwable t]
  (boolean
    (or (instance? InterruptedException t)
        (instance? java.util.concurrent.CancellationException t)
        (.isInterrupted (Thread/currentThread))
        (when-let [cause (.getCause t)]
          (cancellation-exception? cause)))))

(defrecord FutureInvocationProcessor [active-futures]
  sp/InvocationProcessor
  (supports-invocation-type? [_this typ] (= :future typ))
  (start-invocation! [_this {::sc/keys [event-queue]
                             :as env} {:keys [invokeid src params]}]
    (log/debug "Start future " invokeid src params)
    (let [source-session-id (env/session-id env)
          child-session-id (str source-session-id "." invokeid)
          done-event-name (evts/invoke-done-event invokeid)
          error-event-name (evts/invoke-error-event invokeid)]
      (if-not (fn? src)
        (do
          (sp/send! event-queue env {:target            source-session-id
                                     :sendid            child-session-id
                                     :source-session-id child-session-id
                                     :event             :error.platform
                                     :data              {:message "Could not invoke future. No function supplied."
                                                         :target  src}})
          false)
        ;; `started` promise: block the future body until we've registered it in active-futures,
        ;; so a very-fast completion can't run the finally dissoc before the assoc happens.
        (let [started (promise)
              f       (future
                        @started
                        (try
                          (let [result (src params)]
                            (sp/send! event-queue env {:target            source-session-id
                                                       :sendid            child-session-id
                                                       :source-session-id child-session-id
                                                       :invoke-id         invokeid
                                                       :event             done-event-name
                                                       :data              (if (map? result) result {})}))
                          (catch Throwable t
                            ;; `stop-invocation!` cancels the future, which
                            ;; interrupts its thread — blocking operations
                            ;; inside the body surface as Interrupted /
                            ;; Cancellation exceptions (sometimes wrapped in
                            ;; ExecutionException / CompletionException / a
                            ;; domain exception). That's a normal exit, not
                            ;; an invocation failure; emitting error.invoke.*
                            ;; would drive transitions that should never fire
                            ;; after the state was left.
                            (if (cancellation-exception? t)
                              (do
                                ;; Re-assert the interrupt flag per Java
                                ;; convention — we're swallowing the
                                ;; InterruptedException at the boundary.
                                (.interrupt (Thread/currentThread))
                                (log/debug "Future invocation cancelled, suppressing error.invoke.*"
                                           {:invokeid invokeid}))
                              (do
                                (log/error t "Future invocation failed" {:invokeid invokeid})
                                (sp/send! event-queue env {:target            source-session-id
                                                           :sendid            child-session-id
                                                           :source-session-id child-session-id
                                                           ;; Required for handle-external-invocations!
                                                           ;; to run finalize/autoforward against the
                                                           ;; correct <invoke> on the error path.
                                                           :invoke-id         invokeid
                                                           :event             error-event-name
                                                           :data              {:message (.getMessage t)
                                                                               :type    (str (type t))}}))))
                          (finally
                            (swap! active-futures dissoc child-session-id))))]
          (try
            (swap! active-futures assoc child-session-id f)
            (finally
              (deliver started true)))
          true))))
  (stop-invocation! [_ env {:keys [invokeid]}]
    (log/debug "Stop future" invokeid)
    (let [source-session-id (env/session-id env)
          child-session-id (str source-session-id "." invokeid)
          f (get @active-futures child-session-id)]
      (when f
        (log/debug "Sending cancel to future")
        (future-cancel f))
      true))
  (forward-event! [_this _env _event]
    (log/warn "Future event forwarding not supported")))

(defn new-future-processor
  "Create an invocation processor that can be used to run functions in futures."
  []
  (->FutureInvocationProcessor (atom {})))
