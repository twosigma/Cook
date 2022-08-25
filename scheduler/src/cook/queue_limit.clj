(ns cook.queue-limit
  (:require [clj-time.core :as time]
            [clojure.tools.logging :as log]
            [cook.cached-queries :as cached-queries]
            [cook.config :as config]
            [cook.datomic :as datomic]
            [cook.prometheus-metrics :as prom]
            [cook.queries :as queries]
            [cook.regexp-tools :as regexp-tools]
            [cook.util :as util]
            [datomic.api :as d]
            [plumbing.core :as pc]
            [metrics.timers :as timers]))

(defn- per-pool-config
  "Returns the :per-pool section of the queue-limits config"
  []
  (:per-pool (config/queue-limits)))

(defn- pool-global-threshold
  "Returns the pool-global-threshold for the given pool,
  the value at which the per-user queue limit switches
  from the 'normal' number to the 'constrained' number"
  [pool-name]
  (regexp-tools/match-based-on-pool-name
    (per-pool-config)
    pool-name
    :pool-global-threshold
    :default-value Integer/MAX_VALUE))

(defn- user-limit-normal
  "Returns the user-limit-normal for the given pool"
  [pool-name]
  (regexp-tools/match-based-on-pool-name
    (per-pool-config)
    pool-name
    :user-limit-normal
    :default-value Integer/MAX_VALUE))

(defn- user-limit-constrained
  "Returns the user-limit-constrained for the given pool"
  [pool-name]
  (regexp-tools/match-based-on-pool-name
    (per-pool-config)
    pool-name
    :user-limit-constrained
    :default-value Integer/MAX_VALUE))

(defn- update-interval-seconds
  "Returns the interval in seconds at which
  to refresh queue lengths from the database"
  []
  (:update-interval-seconds
    (config/queue-limits)))

(defn get-pending-jobs
  "Queries for and returns the set of
  currently pending jobs from the database"
  []
  (-> datomic/conn
      d/db
      queries/get-pending-job-ents))

(defn- jobs->queue-lengths
  "Given a collection of pending jobs, returns a map with two
  sub-maps of the following shape:

  {:pool->queue-length {pool-a 100 pool-b 200 ...}
   :pool->user->queue-length {pool-a {user-x 10 user-y 20 user-z 70}
                              pool-b {user-x 20 user-y 40 user-z 140}
                              ...}"
  [pending-jobs]
  (let [pool->pending-jobs
        (group-by
          cached-queries/job->pool-name
          pending-jobs)
        pool->user->queue-length
        (pc/map-vals
          #(pc/map-vals
             count
             (group-by
               cached-queries/job-ent->user
               %))
          pool->pending-jobs)]
    {:pool->queue-length
     (pc/map-vals
       #(->> % vals (reduce +))
       pool->user->queue-length)
     :pool->user->queue-length
     pool->user->queue-length}))

(defn query-queue-lengths
  "Queries for pending jobs from the database and
  returns a map with two sub-maps of the following shape:

  {:pool->queue-length {pool-a 100 pool-b 200 ...}
   :pool->user->queue-length {pool-a {user-x 10 user-y 20 user-z 70}
                              pool-b {user-x 20 user-y 40 user-z 140}
                              ...}"
  []
  (jobs->queue-lengths (get-pending-jobs)))

(let [pool->queue-length-atom (atom {})
      pool->user->queue-length-atom (atom {})]

  (defn user-queue-length
    "Returns the queue length for the given pool name and user"
    [pool-name user]
    (get-in
      @pool->user->queue-length-atom
      [pool-name user]
      0))

  (defn user-queue-limit
    "Returns the queue length limit for the given pool name -- if the
    pool-global queue length is <= than the pool global threshold, we
    use the 'normal' per-user limit, otherwise, we switch to using the
    'constrained' per-user limit"
    [pool-name]
    (let [pool-global-length (get @pool->queue-length-atom pool-name 0)
          pool-global-threshold (pool-global-threshold pool-name)]
      (if (<= pool-global-length pool-global-threshold)
        (user-limit-normal pool-name)
        (user-limit-constrained pool-name))))

  (defn inc-queue-length!
    "Increments the pool-global and per-user queue lengths for
    the given pool name and user by the given number of jobs"
    [pool-name user number-jobs]
    {:pre [(some? pool-name)]}
    (let [inc-number-jobs #(-> % (or 0) (+ number-jobs))]
      (swap! pool->queue-length-atom update pool-name inc-number-jobs)
      (swap! pool->user->queue-length-atom update-in [pool-name user] inc-number-jobs))
    {:pool->queue-length @pool->queue-length-atom
     :pool->user->queue-length @pool->user->queue-length-atom})

  (defn dec-queue-length!
    "Decrements the pool-global and per-user queue lengths for
    the given set of pending jobs that are being killed"
    [killed-pending-jobs]
    (let [{:keys [pool->queue-length
                  pool->user->queue-length]}
          (jobs->queue-lengths killed-pending-jobs)
          subtract-fn
          (fn [a b]
            (-> a (- b) (max 0)))]
      (swap! pool->queue-length-atom #(merge-with subtract-fn % pool->queue-length))
      (swap! pool->user->queue-length-atom #(util/deep-merge-with subtract-fn % pool->user->queue-length)))
    {:pool->queue-length @pool->queue-length-atom
     :pool->user->queue-length @pool->user->queue-length-atom})

  (timers/deftimer
    [cook-scheduler
     queue-limit
     update-queue-lengths!-duration])

  (defn update-queue-lengths!
    "Queries queue lengths from the database and updates the atoms"
    []
    (prom/with-duration
      prom/update-queue-lengths-duration
      (timers/time!
        update-queue-lengths!-duration
        (log/info "Starting queue length update")
        (let [{:keys [pool->queue-length
                      pool->user->queue-length]
               :as queue-lengths}
              (query-queue-lengths)]
          (log/info "Queried queue length" queue-lengths)
          (reset! pool->queue-length-atom
                  pool->queue-length)
          (reset! pool->user->queue-length-atom
                  pool->user->queue-length)
          (log/info "Done with queue length update"))))))

(defn start-updating-queue-lengths
  "Starts the chime to update queue lengths at the configured interval"
  []
  (let [interval-seconds (update-interval-seconds)]
    (log/info "Starting queue length updating at intervals of" interval-seconds "seconds")
    (chime/chime-at
      (util/time-seq
        (time/now)
        (time/seconds interval-seconds))
      (fn [_] (update-queue-lengths!))
      {:error-handler
       (fn [ex]
         (log/error ex "Failed to update queue length"))})))
