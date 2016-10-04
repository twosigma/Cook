(ns cook.sim.schedule
  (:require [clojure.algo.generic.functor :refer [fmap]]
            [clojure.data.generators :as gen]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [datomic.api :as d]
            [incanter.stats :as stats]
            [simulant.sim :as sim]
            [simulant.util :as simu]
            [cook.sim.database :as db]
            [cook.sim.util :as u]
            [cook.sim.database :as data]))

(defn create-db-test
  "Returns test db entity"
  [conn test]
  (simu/require-keys test :db/id :test/duration)
  (-> @(d/transact conn [(assoc test :test/type :test.type/cook)])
      (simu/tx-ent (:db/id test))))

(defn create-db-user
  "Returns test db entity"
  [conn test]
  (let [user-tempid (d/tempid :test)]
    (-> @(d/transact conn [{:db/id user-tempid
                            :agent/type :agent.type/user
                            :test/_agents (simu/e test)}])
        (simu/tx-ent user-tempid))))

(defn create-db-job
  "Returns test db entity"
  [conn job-spec user-spec user-id]
  @(d/transact conn [{:db/id (d/tempid :test)
                      :agent/_actions user-id
                      :action/atTime (:at-ms job-spec)
                      :action/type :action.type/job
                      :job/username (:username user-spec)
                      :job/name (:name job-spec)
                      :job/priority (:priority job-spec)
                      :job/duration (:duration job-spec)
                      :job/memory (:memory job-spec)
                      :job/cpu (:cpu job-spec)
                      :job/exit-code (:exit-code job-spec)}]))

(defmethod sim/create-test :model.type/cook
  [conn model schedule]
  (let [test (create-db-test conn {:db/id (d/tempid :test)
                                   :test/duration (* 1000 (:duration-seconds schedule))
                                   :test/label (:label schedule)})]
    (doseq [r (:users schedule)]
      (let [user-id (create-db-user conn test)]
        (doseq [j (:jobs r)] (create-db-job conn j r (simu/e user-id)))))
    (d/entity (d/db conn) (simu/e test))))

(defn random-long
  [{:keys [mean std-dev floor ceiling]}]
  (max (min (long (stats/sample-normal 1 :mean mean :sd std-dev))
            ceiling)
       floor))

(defn random-name
  [length]
  (->> "abcdefghijklmnopqrstuvwxyz" gen/shuffle (take length) (apply str)))

(defn random-job
  [profile at-ms]
  {:at-ms at-ms
   :name (random-name 10)
   :priority (rand-int 101)
   :duration (random-long (:job-duration profile))
   :memory (random-long (:job-memory profile))
   :cpu (float (random-long (:job-cpu profile)))
   ;; TODO simulate jobs that will always fail, or fail sometimes?
   :exit-code 0})

(defn random-user
  [test-duration profile name]
  (let [duration-ms (* test-duration 1000)
        step #(random-long (fmap (partial * 1000) (:seconds-between-jobs profile)))]
    {:username name
     :profile (:description profile)
     :jobs (->> (reductions + (repeatedly step))
                (take-while (fn [t] (< t duration-ms)))
                (map (partial random-job profile))
                (into []))}))

(defn users-for-profile
  [test-duration profile]
  (into [] (map (partial random-user test-duration profile)
                (:usernames profile))))

(defn random-schedule
  [{:keys [label duration-seconds user-profiles] :as settings}]
  {:label label
   :duration-seconds duration-seconds
   :users (->> user-profiles
                     (map (partial users-for-profile duration-seconds))
                     (apply concat)
                     (into []))})

(defn import-schedule!
  [{conn :conn} filename]
  (println "Importing job schedule from" filename "...")
  (let [schedule (-> filename slurp edn/read-string)
        new-test (sim/create-test conn
                                  {:model/type :model.type/cook}
                                  schedule)
        schedule-id (:db/id new-test)]
    (println "New job schedule created: " schedule-id)
    (println "To run a simulation using this schedule, try arguments simulate -e " schedule-id)
    schedule-id))

(defn generate-job-schedule!
  [{settings :sim-model} filename]
  (println "Writing job schedule to" filename "...")
  (pp/pprint (random-schedule settings) (io/writer filename)))

(defn- format-schedule
  "Prepares a simulation id result from query for output via print-table"
  [db [id label duration num-users]]
  {"ID" id
   "Label" label
   "Duration" (str (/ duration 1000) " secs")
   "Num Users" num-users
   "Created" (u/created-at db id)})

(defn list-job-schedules
  [{conn :conn}]
  (println "Listing available job schedules...")
  (let [db (d/db conn)]
    (pp/print-table (map (partial format-schedule db)
                         (d/q '[:find ?e ?label ?dur (count ?agt)
                                :where [?e]
                                [?e :test/label ?label]
                                [?e :test/agents ?agt]
                                [?e :test/duration ?dur]
                                [?e :test/type :test.type/cook]]
                              db)))))
