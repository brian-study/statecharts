(ns com.fulcrologic.statecharts.jobs.worker-missionary
  "Convenience entry point for starting a worker.
   The worker now handles concurrency internally via demand-driven claiming
   (see ADR-001). This namespace is a thin alias for backward compatibility."
  (:require
   [com.fulcrologic.statecharts.jobs.worker :as worker]))

(defn start-missionary-worker!
  "Start a worker. Delegates to worker/start-worker!."
  [opts]
  (worker/start-worker! opts))
