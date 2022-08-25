(ns cook.plugins.pool-mover
  (:require [clojure.tools.logging :as log]
            [cook.cached-queries :as cached-queries]
            [cook.config :as config]
            [cook.plugins.definitions :as chd]
            [cook.prometheus-metrics :as prom]
            [datomic.api :as d]
            [metrics.counters :as counters]))

(counters/defcounter [cook-mesos plugins pool-mover jobs-migrated])

(defrecord PoolMoverJobAdjuster [pool-mover-config]
  chd/JobAdjuster
  (adjust-job [_ {:keys [job/uuid job/pool] :as job-txn} db]
    (let [submission-pool (-> db (d/entity pool) :pool/name (or (config/default-pool)))]
      (if-let [{:keys [users destination-pool]} (get pool-mover-config submission-pool)]
        (let [user (cached-queries/job-ent->user job-txn)]
          (if-let [{:keys [portion]} (get users user)]
            (if (and (number? portion)
                     (> (* portion 100) (-> uuid hash (mod 100))))
              (try
                (log/info "Moving job" uuid "(" user ") from" submission-pool "pool to"
                          destination-pool "pool due to pool-mover configuration")
                (prom/inc prom/pool-mover-jobs-updated)
                (counters/inc! jobs-migrated)
                (assoc job-txn :job/pool (-> db (d/entity [:pool/name destination-pool]) :db/id))
                (catch Throwable t
                  (log/error t "Error when moving pool to" destination-pool)
                  job-txn))
              job-txn)
            job-txn))
        job-txn))))

(defn make-pool-mover-job-adjuster
  [config]
  (let [pool-mover-config (get-in config [:settings :plugins :pool-mover])]
    (log/info "Configuring PoolMoverJobAdjuster" pool-mover-config)
    (->PoolMoverJobAdjuster pool-mover-config)))
