(ns cook.cached-queries
  (:require [cook.cache :as ccache]
            [cook.caches :as caches]
            [cook.config :as config]
            [cook.datomic :as datomic]
            [datomic.api :as d :refer [q]]))

(let [miss-fn
      (fn [{:keys [job/pool]}]
        (or (:pool/name pool)
            (config/default-pool)
            "no-pool"))]
  (defn job->pool-name
    "Return the pool name of the job. Guaranteed non nil."
    [job]
    (caches/lookup-cache-datomic-entity! caches/job-ent->pool-cache miss-fn job)))

(defn job-ent->user
  "Given a job entity, return the user the job runs as."
  [job-ent]
  (caches/lookup-cache-datomic-entity! caches/job-ent->user-cache :job/user job-ent))

(defn instance-uuid->job-uuid-datomic-query
  "Queries for the job uuid from an instance uuid.
   Returns nil if the instance uuid doesn't correspond
   to a job"
  [db instance-uuid]
  (->> (d/entity db [:instance/task-id (str instance-uuid)])
       :job/_instance
       :job/uuid))

(let [miss-fn
      (fn [instance-uuid]
        (str (instance-uuid->job-uuid-datomic-query (d/db datomic/conn) instance-uuid)))]
  (defn instance-uuid->job-uuid-cache-lookup
    "Get job-uuid from cache if it is present, else search datomic for it"
    [instance-uuid]
    (ccache/lookup-cache! caches/instance-uuid->job-uuid identity miss-fn instance-uuid)))

(let [miss-fn
      (fn [job-uuid]
        (d/entity (d/db datomic/conn) [:job/uuid job-uuid]))]
  (defn job-uuid->job-map-cache-lookup
    "Get job-map from cache if it is present, else search datomic for it"
    [job-uuid]
    (ccache/lookup-cache! caches/job-uuid->job-map identity miss-fn job-uuid)))
