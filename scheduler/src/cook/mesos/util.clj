;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns cook.mesos.util
  (:require [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.format :as tf]
            [cook.config :as config]
            [cook.mesos.pool :as pool]
            [cook.mesos.schema :as schema]
            [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.tools.logging :as log]
            [datomic.api :as d :refer (q)]
            [metatransaction.core :refer (db)]
            [metrics.timers :as timers]
            [plumbing.core :as pc :refer (map-vals map-keys)])
  (:import
    [com.google.common.cache Cache CacheBuilder]
    [java.util.concurrent TimeUnit]
    [java.util Date]
    [org.joda.time DateTime ReadablePeriod]))

(defn new-cache []
  "Build a new cache"
  (-> (CacheBuilder/newBuilder)
      (.maximumSize 1000000)
      ;; if its not been accessed in 2 hours, whatever is going on, its not being visted by the
      ;; scheduler loop anymore. E.g., its probably failed/done and won't be needed. So,
      ;; lets kick it out to keep cache small.
      (.expireAfterAccess 2 TimeUnit/HOURS)
      (.build)))

(defn lookup-cache!
  "Generic cache. Caches under a key (extracted from the item with extract-key-fn. Uses miss-fn to fill
  any misses. Caches only positive hits where both functions return non-nil"
  [^Cache cache extract-key-fn miss-fn item]
  (if-let [key (extract-key-fn item)]
    (if-let [result (.getIfPresent cache key)]
      result ; we got a hit.
      (let [new-result (miss-fn item)]
        ; Only cache non-nil
        (when new-result
          (.put cache key new-result))
        new-result))
    (miss-fn item)))

(defn lookup-cache-datomic-entity!
  "Specialized function for caching where datomic entities are the key.
  Extracts :db/id so that we don't keep the entity alive in the cache."
  [cache miss-fn entity]
  (lookup-cache! cache :db/id miss-fn entity))

(defonce ^Cache job-ent->resources-cache (new-cache))
(defonce ^Cache job-ent->pool-cache (new-cache))
(defonce ^Cache task-ent->user-cache (new-cache))
(defonce ^Cache job-ent->user-cache (new-cache))
(defonce ^Cache task->feature-vector-cache (new-cache))

(defn get-all-resource-types
  "Return a list of all supported resources types. Example, :cpus :mem :gpus ..."
  [db]
  (->> (q '[:find ?ident
            :where
            [?e :resource.type/mesos-name ?ident]]
          db)
       (map first)))

(let [default-pool (config/default-pool)
      miss-fn (fn [{:keys [job/pool]}]
                (or (:pool/name pool) default-pool "no-pool"))]
  (defn job->pool-name
    "Return the pool name of the job."
    [job]
    (lookup-cache-datomic-entity! job-ent->pool-cache miss-fn job)))

(defn without-ns
  [k]
  (if (keyword? k)
    (keyword (name k))
    k))

;; These two walk functions were copied from https://github.com/clojure/clojure/blob/master/src/clj/clojure/walk.clj
;; because the line about datomic.query.EntityMap needed to be added..
(defn walk
  "Traverses form, an arbitrary data structure.  inner and outer are
  functions.  Applies inner to each element of form, building up a
  data structure of the same type, then applies outer to the result.
  Recognizes all Clojure data structures. Consumes seqs as with doall."

  {:added "1.1"}
  [inner outer form]
  (cond
    (list? form) (outer (apply list (map inner form)))
    (instance? clojure.lang.IMapEntry form) (outer (vec (map inner form)))
    (seq? form) (outer (doall (map inner form)))
    (instance? clojure.lang.IRecord form)
    (outer (reduce (fn [r x] (conj r (inner x))) form form))
    ;; Added the line below to work with datomic entities..
    (instance? datomic.query.EntityMap form) (outer (into {} (map inner) form))
    (coll? form) (outer (into (empty form) (map inner) form))
    :else (outer form)))

(defn postwalk
  "Performs a depth-first, post-order traversal of form.  Calls f on
  each sub-form, uses f's return value in place of the original.
  Recognizes all Clojure data structures. Consumes seqs as with doall."
  {:added "1.1"}
  [f form]
  (walk (partial postwalk f) f form))

(defn deep-transduce-kv
  "Recursively applies the transducer xf over all kvs in the map or
  any nested maps"
  [xf m]
  (postwalk (fn [x]
              (if (map? x)
                (into {} xf x)
                x))
            m))

(defn entity->map
  "Takes a datomic entity and converts it along with any nested entities
   into clojure maps"
  ([entity db]
   (entity->map (d/entity db (:db/id entity))))
  ([entity]
   (deep-transduce-kv (map identity) entity)))

; During testing, we found that calling (keys) on a group entity with a large number of jobs was
; quite slow (12ms just to list keys vs 200 microseconds for job-ent->map.)
; As an optimization, load the group keys ahead of time from the schema.
(let [group-keys (->> schema/schema-attributes
                      (map :db/ident)
                      (filter #(= "group" (namespace %)))
                      ; Do not load the nested job entities for a group. These should be queried on-demand
                      ; as required.
                      (remove (partial = :group/job)))]
  (defn job-ent->map
    "Convert a job entity to a map.
     This also loads the associated group without the nested jobs in the group and converts it to a map.
     Returns nil if there was an error while converting the job entity to a map."
    ([job db]
     (job-ent->map (d/entity db (:db/id job))))
    ([job]
     (try
       (let [group-ent (first (:group/_job job))
             job (entity->map job)
             group (when group-ent
                     (->> group-keys
                          (pc/map-from-keys (fn [k] (entity->map (get group-ent k))))
                          ; The :group/job key normally returns a set, so let's do the same for compatibility
                          hash-set))]
         (cond-> job
                 group (assoc :group/_job group)))
       (catch Exception e
         ;; not logging the stacktrace as it adds noise and can cause the log files to blow up in size
         (log/error "Error while converting job entity to a map" {:job job :message (.getMessage e)}))))))

(defn remove-datomic-namespacing
  "Takes a map from datomic (pull) and removes the namespace
   as well as :db/id keys"
  [datomic-map]
  (->> datomic-map
       (deep-transduce-kv (comp
                            (filter (comp (partial not= :db/id) first))
                            (map (fn [[k v]]
                                   ;; This if is here in the case when a ident is used as
                                   ;; an enum and the data is gotten from the pull api.
                                   ;; It will be represented as:
                                   ;; {:thing/type {:ident :ident/thing}}
                                   (if (and (map? v) (:ident v))
                                     [k (without-ns (:ident v))]
                                     [k v])))
                            (map (juxt (comp without-ns first) second))))
       ;; Merge with {} in case datomic-map was nil so we get empty map back
       (merge {})))

(defn job-ent->container
  "Take a job entity and return its container"
  [db job-ent]
  (some-> job-ent :job/container remove-datomic-namespacing))

(defn job-ent->group-uuid
  "Take a job entity and return its group UUID"
  [job-ent]
  (some-> job-ent :group/_job first :group/uuid))

(defn job-ent->env
  "Take a job entity and return the environment variable map"
  [job-ent]
  (reduce (fn [m env-var]
            (assoc m
              (:environment/name env-var)
              (:environment/value env-var)))
          {}
          (:job/environment job-ent)))

(defn job-ent->label
  "Take a job entity and return the label map"
  [job-ent]
  (reduce (fn [m label-var]
            (assoc m
              (:label/key label-var)
              (:label/value label-var)))
          {}
          (:job/label job-ent)))

(defn job-ent->resources
  "Take a job entity and return a resource map. NOTE: the keys must be same as mesos resource keys"
  [job]
  (let [job-ent->resources-miss
        (fn [job-ent]
          (reduce (fn [m r]
                    (let [resource (keyword (name (:resource/type r)))]
                      (condp contains? resource
                        #{:cpus :mem :gpus} (assoc m resource (:resource/amount r))
                        #{:uri} (update-in m [:uris] (fnil conj [])
                                           {:cache (:resource.uri/cache? r false)
                                            :executable (:resource.uri/executable? r false)
                                            :value (:resource.uri/value r)
                                            :extract (:resource.uri/extract? r false)}))))
                  {:ports (:job/ports job-ent 0)}
                  (:job/resource job-ent)))]
    (lookup-cache-datomic-entity! job-ent->resources-cache job-ent->resources-miss job)))

(defn job-ent->attempts-consumed
  "Determines the amount of attempts consumed by a job-ent."
  [db job-ent]
  (d/invoke db :job/attempts-consumed db job-ent))

(defn sum-resources-of-jobs
  "Given a collections of job entities, returns the total resources they use
   {:cpus cpu :mem mem}"
  [job-ents]
  (loop [total-cpus 0.0
         total-mem 0.0
         [job-ent & job-ents] job-ents]
    (if job-ent
      (let [{:keys [cpus mem]} (job-ent->resources job-ent)]
        (recur (+ total-cpus (or cpus 0))
               (+ total-mem (or mem 0))
               job-ents))
      {:cpus total-cpus :mem total-mem})))

(defn total-resources-of-jobs
  "Given a collections of job entities, returns the total resources they use
   {:cpus cpu :mem mem :gpus gpu}"
  [job-ents]
  (reduce
    (fn [acc job-ent]
      (->
        job-ent
        job-ent->resources
        (select-keys [:cpus :mem :gpus])
        (->> (merge-with + acc))))
    {:cpus 0.0, :mem 0.0, :gpus 0.0, :jobs (count job-ents)}
    job-ents))

(defn- get-pending-job-ents*
  "Returns a seq of datomic entities corresponding to jobs

   Parameters:
   `unfiltered-db` a database generated by calling datomic.api/db directly
   `committed?` boolean whether the jobs are committed or not"
  [unfiltered-db committed?]
  ;; This function explicitly uses the unfiltered (not metatransaction filtered)
  ;; db to improve the performance of this query. We are working to remove
  ;; metatransaction throughout the code
  (->> (q '[:find [?j ...]
            :in $ ?state ?committed?
            :where
            [?j :job/state ?state]
            [?j :job/commit-latch ?cl]
            [?cl :commit-latch/committed? ?committed?]]
          unfiltered-db :job.state/waiting committed?)
       (map (partial d/entity unfiltered-db))))

(timers/deftimer [cook-mesos scheduler get-pending-jobs-duration])

(defn get-pending-job-ents
  "Returns a seq of datomic entities corresponding to jobs

   Parameters:
   `unfiltered-db` a database generated by calling datomic.api/db directly"
  ([unfiltered-db]
   (timers/time!
     get-pending-jobs-duration
     (get-pending-job-ents* unfiltered-db true))))

(timers/deftimer [cook-mesos scheduler get-completed-jobs-by-user-duration])

(defn job-ent->user
  "Given a job entity, return the user the job runs as."
  [job-ent]
  (lookup-cache-datomic-entity! job-ent->user-cache :job/user job-ent))

(defn job-ent->state
  "Given a job entity, returns the corresponding 'state', which means
  calling with a completed job will return either success or failed,
  depending on the state of the job's instances"
  [{:keys [:job/instance :job/state]}]
  (case state
    :job.state/completed
    (if (some #{:instance.status/success} (map :instance/status instance))
      "success"
      "failed")
    :job.state/running "running"
    :job.state/waiting "waiting"))

(def ^:const job-states #{"running" "waiting" "completed"})
(def ^:const instance-states #{"success" "failed"})


(defn job-submitted-in-range?
  "Returns true if the job-ent's :job/submit-time is between start-ms (inclusive) and
  end-ms (exclusive)"
  [{:keys [^Date job/submit-time]} ^long start-ms ^long end-ms]
  (let [submit-ms (.getTime submit-time)]
    (and (<= start-ms submit-ms)
         (< submit-ms end-ms))))

;; get-jobs-by-user-and-state-and-submit is a bit opaque
;; because it is reaching into datomic internals. Here is a quick explanation.
;; seek-datoms provides a pointer into the raw datomic indices
;; that we can then seek through. We set the pointer to look through the
;; vaet index, with value :job.state/{running,waiting}, seek to
;; state and then seek to the entity id that *would* have been
;; created at expanded start. This works because the submission
;; time (which we sort on) is correlated with the entity id.
;; We then seek through the list of all jobs in that state,
;; then filtering to the target user (which is cached).
;;
;; We could use the avet index here and return correct answers, but that
;; solution seemed to have less performance stability, and be slightly slower.
;; This may be related to how datomic does index refreshes.
;;
;; This function is O(# jobs in that state in the time range)
(defn get-jobs-by-user-and-state-and-submit
  "Returns all jobs for a particular user in the specified state
   and timeframe. Returns lazy output."
  [db ^String user ^Date start ^Date end state-keyword]
  (let [;; Expand the time range so that clock skew between cook
        ;; and datomic doesn't cause us to miss jobs
        ;; 1 hour was picked because a skew larger than that would be
        ;; suspicious
        expanded-start (Date. (- (.getTime start)
                                 (-> 1 t/hours t/in-millis)))
        expanded-end (Date. (+ (.getTime end)
                               (-> 1 t/hours t/in-millis)))
        entid-start (d/entid-at db :db.part/user expanded-start)
        entid-end (d/entid-at db :db.part/user expanded-end)
        job-state-entid (d/entid db :job/state)
        state-entid (d/entid db state-keyword)
        start-ms (.getTime start)
        end-ms (.getTime end)]
    (->> (d/seek-datoms db :vaet state-entid :job/state entid-start)
         (take-while #(and (< (:e %) entid-end)
                           (= (:a %) job-state-entid)
                           (= (:v %) state-entid)))
         (map :e)
         (map (partial d/entity db))
         (filter #(= (job-ent->user %) user))
         (filter #(job-submitted-in-range? % start-ms end-ms)))))

;; get-completed-jobs-by-user is a bit opaque because it is
;; reaching into datomic internals. Here is a quick explanation.
;; seek-datoms provides a pointer into the raw datomic indices
;; that we can then seek through. We set the pointer to look
;; through the avet index, with attribute :job/user, seek to
;; user and then seek to the entity id that *would* have been
;; created at expanded start.
;; This works because the submission time and job/user field
;; are set at the same time, in "real" time. This means that
;; jobs submitted after `start` will have been created after
;; expanded start
;; This function for scanning completed jobs differs from
;; get-active-jobs-by-user-and-state as it is also looking up
;; based on task state; users can ask for "success" or "failed" jobs.
;; This is about O(# jobs user submitted in the given time range)
(defn get-completed-jobs-by-user
  "Returns all completed job entities for a particular user
   in the specified timeframe. Supports looking up based
   on task state 'success' and 'failed' if passed into 'state'"
  [db ^String user ^Date start ^Date end limit state name-filter-fn include-custom-executor? pool-name]
  (timers/time!
    get-completed-jobs-by-user-duration
    (let [;; Expand the time range so that clock skew between cook
          ;; and datomic doesn't cause us to miss jobs
          ;; 1 hour was picked because a skew larger than that would be
          ;; suspicious
          expanded-start (Date. (- (.getTime start)
                                   (-> 1 t/hours t/in-millis)))
          expanded-end (Date. (+ (.getTime end)
                                 (-> 1 t/hours t/in-millis)))
          entid-start (d/entid-at db :db.part/user expanded-start)
          entid-end (d/entid-at db :db.part/user expanded-end)
          default-pool? (pool/default-pool? pool-name)
          pool-name' (or pool-name pool/nil-pool)
          job-user-entid (d/entid db :job/user)
          start-ms (.getTime start)
          end-ms (.getTime end)
          jobs
          (->> (d/seek-datoms db :avet :job/user user entid-start)
               (take-while #(and (< (:e %) entid-end)
                                 (= (:a %) job-user-entid)
                                 (= (:v %) user)))
               (map :e)
               (map (partial d/entity db))
               (filter #(job-submitted-in-range? % start-ms end-ms))
               (filter #(= :job.state/completed (:job/state %)))
               (filter #(pool/check-pool-for-listing % :job/pool pool-name' default-pool?)))]
      (->>
        (cond->> jobs
                 (not include-custom-executor?) (filter #(false? (:job/custom-executor %)))
                 (instance-states state) (filter #(= state (job-ent->state %)))
                 name-filter-fn (filter #(name-filter-fn (:job/name %))))
        ; No need to sort. We're traversing in entity_id order, so in time order.
        (take limit)
        doall))))

(defn uncommitted?
  "Returns true if the given job's commit latch is not committed"
  [job]
  (-> job :job/commit-latch :commit-latch/committed? not))

;; This is a separate query from completed jobs because most running/waiting jobs
;; will be at the end of the time range, so the query is somewhat inefficent.
;; The standard datomic query performs reasonably well on the smaller set of
;; running and waiting jobs, so it's implemented that way to keep things simple.
(defn get-active-jobs-by-user-and-state
  "Returns all jobs for a particular user in the specified state
   and timeframe. This query works for state waiting and running only."
  [db user start end limit state name-filter-fn include-custom-executor? pool-name]
  (let [state-keyword (case state
                        "running" :job.state/running
                        "waiting" :job.state/waiting)
        default-pool? (pool/default-pool? pool-name)
        pool-name' (or pool-name pool/nil-pool)]
    (timers/time!
      (timers/timer ["cook-mesos" "scheduler" (str "get-" (name state) "-jobs-by-user-duration")])
      (->>
        (cond->> (get-jobs-by-user-and-state-and-submit db user start end state-keyword)
                 (not include-custom-executor?) (filter #(false? (:job/custom-executor %)))
                 name-filter-fn (filter #(name-filter-fn (:job/name %)))
                 (and include-custom-executor? (= :job.state/waiting state-keyword)) (remove uncommitted?))
        (filter #(pool/check-pool-for-listing % :job/pool pool-name' default-pool?))
        ; No need to sort. We're traversing in entity_id order, so in time order.
        (take limit)
        doall))))


;; Users have many fewer running/waiting jobs than completed jobs, so they need different queries.
;; For searches for jobs in the running/waiting state, enumerating jobs based on state is more robustly
;; a 'small set' than enumerating by user. Enumerating all jobs by user is more likely to be a
;; small set than all jobs in the completed state. So, we special case the queries here.
(defn get-jobs-by-user-and-states
  "Returns all jobs for a particular user in the specified states."
  [db user states start end limit name-filter-fn include-custom-executor? pool-name]
  (let [get-jobs-by-state (fn get-jobs-by-state [state]
                            (if (#{"completed" "success" "failed"} state)
                              (get-completed-jobs-by-user db user start end limit state
                                                          name-filter-fn include-custom-executor? pool-name)
                              (get-active-jobs-by-user-and-state db user start end limit state
                                                                 name-filter-fn include-custom-executor? pool-name)))
        jobs-by-state (mapcat get-jobs-by-state states)]
    (timers/time!
      (timers/timer ["cook-mesos" "scheduler" "get-jobs-by-user-and-states-duration"])
      (->> jobs-by-state
           (sort-by :job/submit-time)
           (take limit)
           doall))))


(defn jobs-by-user-and-state
  "Returns all job entities for a particular user
   in a particular state.  Unlike get-jobs-by-user-and-state, doesn't
  impose any other conditions."
  [db user state pool-name]
  (let [requesting-default-pool? (pool/requesting-default-pool? pool-name)
        pool-name' (pool/pool-name-or-default pool-name)]
    (->> (q '[:find [?j ...]
              :in $ ?user ?state ?pool-name ?requesting-default-pool
              :where
              [?j :job/state ?state]
              [?j :job/user ?user]
              [(cook.mesos.pool/check-pool $ ?j :job/pool ?pool-name ?requesting-default-pool)]]
            db user state pool-name' requesting-default-pool?)
         (map (partial d/entity db))
         (map d/touch))))

(timers/deftimer [cook-mesos scheduler get-running-tasks-duration])

(defn get-running-task-ents
  "Returns all running task entities."
  [db]
  (timers/time!
    get-running-tasks-duration
    (->> (q '[:find [?i ...]
              :in $ [?status ...]
              :where
              [?i :instance/status ?status]]
            db [:instance.status/running :instance.status/unknown])
         (map (partial d/entity db)))))

(timers/deftimer [cook-mesos scheduler get-user-running-jobs-duration])

(defn get-user-running-job-ents
  "Returns all running job entities for a specific user."
  ([db user]
   (timers/time!
     get-user-running-jobs-duration
     (->> (q
            '[:find [?j ...]
              :in $ ?user
              :where
              ;; Note: We're assuming that many users will have significantly more
              ;; completed jobs than there are jobs currently running in the system.
              ;; If not, we might want to swap these two constraints.
              [?j :job/state :job.state/running]
              [?j :job/user ?user]]
            db user)
          (map (partial d/entity db))))))

(defn get-user-running-job-ents-in-pool
  "Returns all running job entities for a specific user and pool."
  [db user pool-name]
  (let [requesting-default-pool? (pool/requesting-default-pool? pool-name)
        pool-name' (pool/pool-name-or-default pool-name)]
    (timers/time!
      get-user-running-jobs-duration
      (->> (q
             '[:find [?j ...]
               :in $ ?user ?pool-name ?requesting-default-pool
               :where
               ;; Note: We're assuming that many users will have significantly more
               ;; completed jobs than there are jobs currently running in the system.
               ;; If not, we might want to swap these two constraints.
               [?j :job/state :job.state/running]
               [?j :job/user ?user]
               [(cook.mesos.pool/check-pool $ ?j :job/pool ?pool-name ?requesting-default-pool)]]
             db user pool-name' requesting-default-pool?)
           (map (partial d/entity db))))))

(timers/deftimer [cook-mesos scheduler get-running-jobs-duration])

(defn get-running-job-ents
  "Returns all running job entities."
  [db]
  (timers/time!
    get-running-jobs-duration
    (->> (q '[:find [?j ...]
              :in $
              :where
              [?j :job/state :job.state/running]]
            db)
         (map (partial d/entity db)))))

(defn job-allowed-to-start?
  "Converts the DB function :job/allowed-to-start? into a predicate"
  [db job]
  (try
    (d/invoke db :job/allowed-to-start? db (or (:db/id job)
                                               [:job/uuid (:job/uuid job)]))
    true
    (catch clojure.lang.ExceptionInfo e
      false)))

(defn create-task-ent
  "Takes a pending job entity and returns a synthetic running task entity for that job"
  [pending-job-ent & {:keys [hostname slave-id] :or {hostname nil slave-id nil}}]
  (merge {:job/_instance pending-job-ent
          :instance/status :instance.status/running}
         (when hostname {:instance/hostname hostname})
         (when slave-id {:instance/slave-id slave-id})))

(defn task-ent->user
  [task-ent]
  (let [task-ent->user-miss
        (fn [task-ent]
          (get-in task-ent [:job/_instance :job/user]))]
    (lookup-cache-datomic-entity! task-ent->user-cache task-ent->user-miss task-ent)))

(def ^:const default-job-priority 50)


(defn task->feature-vector
  "Vector of comparable features of a task.
   Last two elements are aribitary tie breakers.
   Use :db/id because they guarantee uniqueness for different entities
   (:db/id task) is not sufficient because synthetic task entities don't have :db/id
   This assumes there are at most one synthetic task for a job, otherwise uniqueness invariant will break"
  [task]
  (let [task->feature-vector-miss
        (fn [task]
          [(- (:job/priority (:job/_instance task) default-job-priority))
           (:instance/start-time task (java.util.Date. Long/MAX_VALUE))
           (:db/id task)
           (:db/id (:job/_instance task))])
        extract-key
        (fn [item]
          (or (:db/id item) (:db/id (:job/_instance item))))]
    (lookup-cache! task->feature-vector-cache extract-key task->feature-vector-miss task)))

(defn same-user-task-comparator
  "Comparator to order same user's tasks"
  ([]
   (same-user-task-comparator []))
  ([tasks]
   (fn [task1 task2]
     (compare (task->feature-vector task1)
              (task->feature-vector task2)))))

(defn retry-job!
  "Sets :job/max-retries to the given value for the given job UUID.
   Also resets the job state to 'waiting' if it had completed.
   Throws an exception if there is no job with that UUID."
  [conn uuid retries]
  (let [eid (-> (d/entity (d/db conn) [:job/uuid uuid])
                :db/id)]
    @(d/transact conn
                 [[:job/update-retry-count [:job/uuid uuid] retries]
                  [:job/update-state-on-retry [:job/uuid uuid] retries]])))

(defn filter-sequential
  "This function allows for filtering when the filter function needs to consider previous elements
   Lazily filters elements of coll.
   f is assumed to take two parameters, state and an element, i.e. (f state element)
   and return a pair [new-state should-keep?] where new-state will be passed to f when called on the next element.
   The new-state is passed regardless of whether should-keep? is truth-y or not."
  [f init-state coll]
  (letfn [(fr [{:keys [state]} x]
            (let [[state' should-keep?] (f state x)]
              {:state state'
               :x (when should-keep? x)}))]
    (->> coll
         (reductions fr {:state init-state :x nil})
         (filter :x)
         (map :x))))

(defn task-run-time
  "Returns the run time of the task as a joda interval"
  [task-ent]
  (let [start (tc/from-date (:instance/start-time task-ent))
        end (or (tc/from-date (:instance/end-time task-ent))
                (t/now))]
    (t/interval start end)))

(defn namespace-datomic
  "Namespaces keywords given the datomic conventions

   Examples:
   (namespace-datomic :straggler-handling :type)
   :straggler-handling/type
   (namespace-datomic :straggler-handling :type :quantile-deviation)
   :straggler-handling.type/quantile-deviation"
  ([name-space value]
   (keyword (name name-space) (name value)))
  ([name-space subspace value]
   (namespace-datomic (str (name name-space) "." (name subspace)) value)))

(defn make-guuid->juuids
  "Given a list of jobs, groups them by guuid. Returned value is a map that
   goes from guuid to a list of juuids."
  [jobs]
  (->> jobs
       (map (fn [job]
              (mapv #(vector (:group/uuid %) #{(:job/uuid job)}) (:group/_job job))))
       (map (partial into {}))
       (reduce (partial merge-with clojure.set/union))))

(defn make-guuid->considerable-cotask-ids
  "Takes a list of jobs and their corresponding task-ids. Returns a function that, given a group uuid, returns the
   the set of task-ids associated with that group. A set is returned for consistency with datomic queries."
  [job->considerable-task-id]
  (let [guuid->juuids (make-guuid->juuids (keys job->considerable-task-id))
        juuid->task-id (map-keys :job/uuid job->considerable-task-id)]
    (fn [guuid]
      (->> guuid
           (get guuid->juuids)
           (map juuid->task-id)
           set))))

(defn get-slave-attrs-from-cache
  "Looks up a slave property (properties are a union of the slave's attributes and its hostname) in the provided cache"
  [agent-attributes-cache-atom slave-id]
  (cache/lookup @agent-attributes-cache-atom slave-id))

(defn clear-uncommitted-jobs
  "Retracts entities that have not been committed as of now and were submitted before
   `submitted-before`

   Parameters:
   `conn` datomic database connection
   `submitted-before` clj-time datetime
   `dry-run` boolean, if true, will skip retracting the entities

   Returns:
   seq of uncommitted jobs deleted (or to delete in case of dry run)"
  [conn submitted-before dry-run?]
  (let [uncommitted-jobs (get-pending-job-ents* (d/db conn) false)
        committed-jobs (get-pending-job-ents* (d/db conn) true)
        committed-job-entids (set (map :db/id committed-jobs))
        uncommitted-before (filter #(t/before? (-> % :job/submit-time tc/from-date)
                                               submitted-before)
                                   uncommitted-jobs)]
    (when (some #(contains? committed-job-entids (:db/id %)) uncommitted-jobs)
      (throw (ex-info "There is overlap between committed and uncommitted jobs, there is something wrong!"
                      {:count-committed (count committed-jobs)
                       :count-uncommitted (count uncommitted-jobs)})))
    (when-not dry-run?
      (doseq [batch (partition-all 10 uncommitted-before)]
        @(d/transact conn (mapv #(vector :db.fn/retractEntity (:db/id %))
                                batch))))
    uncommitted-before))

(defn instance-running?
  [instance]
  (some #{(:instance/status instance)} #{:instance.status/running
                                         :instance.status/unknown}))

(defn close-when-ch!
  "When the value passed in is a channel, close it. Otherwise, do nothing"
  [maybe-ch]
  (try
    (async/close! maybe-ch)
    (catch Exception _)))

(defn chime-at-ch
  "Like chime-at (from chime[https://github.com/jarohen/chime])
   but pass in an arbitrary chan instead of times to make a chime chan

   Calls f with no arguments

   Will try to close the item pulled from ch once f has completed if the item is a channel"
  [ch f & [{:keys [error-handler on-finished]
            :or {error-handler identity
                 on-finished #()}}]]
  (async/go-loop []
    (if-let [x (async/<! ch)]
      (do (async/<! (async/thread
                      (try
                        (f)
                        (catch Exception e
                          (error-handler e)))))
          (close-when-ch! x)
          (recur))
      (on-finished)))
  (fn cancel! []
    (async/close! ch)))

(defn read-chan
  "Tries to read `ch-size` elements immediately from the channel and
   returns the values on the channel.

   This function does not block and may return an empty list if the channel
   is currently empty."
  [ch ch-size]
  (->> (repeatedly ch-size #(async/poll! ch))
       (remove nil?)))

(defn reducing-pipe
  "Reads elements from the `from` channel and supplies elements to the `to` channel.
   Maintains a state (initialized to `initial-state`) that is updated by applying the reducing
   function `reducer` to the current state and the incoming element `(reducer state element)`.
   When the `to` channel can receive an element, the current state is put on the `to` channel
   and the state is reset to `initial-state`.
   By default, the `to` channel will be closed when the `from` channel closes, but can be
   determined by the optional close? parameter.

   Note: This function does not perform error handling, exceptions must be explicitly handled in
   the provided functions (i.e. reducer)."
  [from reducer to & {:keys [close? initial-state] :or {close? true}}]
  (async/go-loop [state initial-state]
    (let [[data chan] (async/alts! [[to state] from] :priority true)]
      (condp = chan
        to (if data
             (recur initial-state)
             (recur state))
        from (if (nil? data)
               (when close?
                 (async/close! to))
               (recur (reducer state data)))))))

(defn cache-lookup!
  "Lookup a value by key in the cache store.
   If the cache has the key, return the value corresponding to the key in the cache.
   If the cache does not have the key, update the cache with key->not-found-value and return not-found-value."
  [cache-store key not-found-value]
  (-> (swap! cache-store
             #(if (cache/has? % key)
                (cache/hit % key)
                (cache/miss % key not-found-value)))
      (cache/lookup key)))

(defn cache-update!
  "Updates the key->value mapping in the cache store."
  [cache-store key value]
  (swap! cache-store #(-> %
                          (cache/evict key)
                          (cache/miss key value))))

(defn parse-time
  "Parses the provided string as a DateTime"
  [s]
  (or (tf/parse s)
      (tc/from-long (Long/parseLong s))))

(defn parse-int-default
  "Parses the provided string as an integer"
  [s d]
  (if (nil? s)
    d
    (Integer/parseInt s)))

(defn time-seq
  "Returns a sequence of date-time values growing over specific period.
   Takes as input the starting value and the growing value, returning a lazy infinite sequence."
  [start ^ReadablePeriod period]
  (iterate (fn [^DateTime t] (.plus t period)) start))

(defn deep-merge
  "Like merge, but merges maps recursively."
  [& maps]
  (if (every? map? maps)
    (apply merge-with deep-merge maps)
    (last maps)))
