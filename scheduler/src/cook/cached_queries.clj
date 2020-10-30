(ns cook.cached-queries
  (:require [cook.caches :as caches]
            [cook.config :as config]))

(let [miss-fn
      (fn [{:keys [job/pool]}]
        (or (:pool/name pool)
            (config/default-pool)
            "no-pool"))]
  (defn job->pool-name
    "Return the pool name of the job."
    [job]
    (caches/lookup-cache-datomic-entity! caches/job-ent->pool-cache miss-fn job)))

(defn job-ent->user
  "Given a job entity, return the user the job runs as."
  [job-ent]
  (caches/lookup-cache-datomic-entity! caches/job-ent->user-cache :job/user job-ent))
