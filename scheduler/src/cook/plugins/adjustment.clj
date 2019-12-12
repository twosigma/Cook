(ns cook.plugins.adjustment
  (:require [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.datomic :as datomic]
            [cook.plugins.definitions :refer [JobAdjuster]]
            [cook.plugins.util]
            [cook.tools :as util]
            [datomic.api :as d]
            [metrics.counters :as counters]
            [mount.core :as mount]))

(def no-op
  (reify JobAdjuster
    (adjust-job [_ job-map] job-map)))

(defn create-default-plugin-object
  "Returns the configured JobAdjuster, or a no-op if none is defined."
  [config]
  (let [factory-fn (get-in config [:settings :plugins :job-adjuster :factory-fn])]
    (if factory-fn
      (do
        (log/info "Creating job adjuster plugin with" factory-fn)
        (if-let [resolved-fn (cook.plugins.util/resolve-symbol (symbol factory-fn))]
          (resolved-fn config)
          (throw (ex-info (str "Unable to resolve factory fn " factory-fn) {}))))
      no-op)))

(counters/defcounter [cook-mesos plugins pool-mover jobs-migrated])

(defrecord PoolMoverJobAdjuster [pool-mover-config]
  JobAdjuster
  (adjust-job [_ {:keys [db/id job/uuid] :as job-map}]
    (let [submission-pool (util/job->pool-name job-map)]
      (when-let [{:keys [users destination-pool]} (get pool-mover-config submission-pool)]
        (let [user (util/job-ent->user job-map)]
          (when-let [{:keys [portion]} (get users user)]
            (when (and (number? portion)
                       (> (* portion 100) (-> uuid hash (mod 100))))
              (try
                (log/info "Moving job" uuid "(" user ") from" submission-pool "pool to"
                          destination-pool "pool due to pool-mover configuration")
                @(d/transact datomic/conn
                             [[:db/add id :job/pool [:pool/name destination-pool]]])
                (.put util/job-ent->pool-cache id destination-pool)
                (counters/inc! jobs-migrated)
                (catch Throwable t
                  (log/error t "Error when transacting pool migration to" destination-pool))))))))))

(defn make-pool-mover-job-adjuster
  [config]
  (let [pool-mover-config (get-in config [:settings :plugins :pool-mover])]
    (log/info "Configuring PoolMoverJobAdjuster" pool-mover-config)
    (->PoolMoverJobAdjuster pool-mover-config)))

(mount/defstate plugin
                :start (create-default-plugin-object config/config))
