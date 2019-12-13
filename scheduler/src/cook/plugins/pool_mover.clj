(ns cook.plugins.pool-mover
  (:require [clojure.tools.logging :as log]
            [cook.plugins.definitions :as chd]
            [cook.tools :as util]
            [datomic.api :as d]
            [metrics.counters :as counters]))

(counters/defcounter [cook-mesos plugins pool-mover jobs-migrated])

(defrecord PoolMoverJobAdjuster [pool-mover-config]
  chd/JobAdjuster
  (adjust-job [_ {:keys [job/uuid] :as job-map} db]
    (let [submission-pool (util/job->pool-name job-map)]
      (when-let [{:keys [users destination-pool]} (get pool-mover-config submission-pool)]
        (let [user (util/job-ent->user job-map)]
          (when-let [{:keys [portion]} (get users user)]
            (when (and (number? portion)
                       (> (* portion 100) (-> uuid hash (mod 100))))
              (try
                (log/info "Moving job" uuid "(" user ") from" submission-pool "pool to"
                          destination-pool "pool due to pool-mover configuration")
                (counters/inc! jobs-migrated)
                (assoc job-map :job/pool (d/entity db [:pool/name destination-pool]))
                (catch Throwable t
                  (log/error t "Error when transacting pool migration to" destination-pool)
                  job-map)))))))))

(defn make-pool-mover-job-adjuster
  [config]
  (let [pool-mover-config (get-in config [:settings :plugins :pool-mover])]
    (log/info "Configuring PoolMoverJobAdjuster" pool-mover-config)
    (->PoolMoverJobAdjuster pool-mover-config)))
