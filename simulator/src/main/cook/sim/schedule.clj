(ns cook.sim.schedule
  (:require [clojure.data.generators :as gen]
            [clojure.pprint :as pprint]
            [datomic.api :as d]
            [simulant.sim :as sim]
            [simulant.util :as simu]
            [cook.sim.database :as db]
            [cook.sim.util :as u]
            [cook.sim.database :as data]))

(defn create-researchers
  "Returns new researcher ids sorted"
  [conn test]
  (let [model (-> test :model/_tests simu/solo)
        ids (repeatedly (:model/num-researchers model) #(d/tempid :test))
        txresult (->> ids
                      (map (fn [id] {:db/id id
                                     :agent/type :agent.type/researcher
                                     :test/_agents (simu/e test)}))
                      (d/transact conn))]
    (simu/tx-entids @txresult ids)))

(defn create-db-test
  "Returns test db entity"
  [conn model test]
  (simu/require-keys test :db/id :test/duration)
  (-> @(d/transact conn [(assoc test
                                :test/type :test.type/cook
                                :model/_tests (simu/e model))])
      (simu/tx-ent (:db/id test))))


(defn rand-geometric
  [model key]
  (-> model key / gen/geometric))

(defn generate-job
  "Generate a job for a researcher, based on the model"
  [test model researcher at-time]
  (let [model (-> test :model/_tests first)]
    [[{:db/id (d/tempid :test)
       :agent/_actions (simu/e researcher)
       :action/atTime at-time
       :action/type :action.type/job
       :job/duration (rand-geometric model :model/mean-job-duration)
       :job/memory (rand-geometric model :model/mean-job-memory)
       :job/cpu (float (rand-geometric model :model/mean-job-cpu))
       ;; TODO simulate jobs that will always fail, or fail sometimes?
       :job/exit-code 0
       }]]))

(defn generate-researcher-jobs
  "Generate all actions for a researcher, based on model"
  [test researcher]
  (let [model (-> test :model/_tests first)
        limit (:test/duration test)
        step #(gen/geometric (/ 1 (* 1000 (:model/mean-seconds-between-jobs model))))]
    (->> (reductions + (repeatedly step))
         (take-while (fn [t] (< t limit)))
         (mapcat #(generate-job test model researcher %)))))

(defn generate-all-jobs
  [test researchers]
  (mapcat
   (fn [r] (generate-researcher-jobs test r))
   researchers))

(defmethod sim/create-test :model.type/cook
  [conn model test]
  (let [test (create-db-test conn model test)
        researchers (create-researchers conn test)]
    (simu/transact-batch conn (generate-all-jobs test researchers) 5)
    (d/entity (d/db conn) (simu/e test))))

(defn make-model
  [settings conn]
  (let [{:keys [mean-seconds-between-jobs
                mean-job-duration
                mean-job-memory
                mean-job-cpu
                num-researchers]} settings
        model-id (d/tempid :model)
        cook-model-data [{:db/id model-id
                          :model/type :model.type/cook
                          :model/num-researchers num-researchers
                          :model/mean-seconds-between-jobs mean-seconds-between-jobs
                          :model/mean-job-duration mean-job-duration
                          :model/mean-job-memory mean-job-memory
                          :model/mean-job-cpu mean-job-cpu}]]
    (-> @(d/transact conn cook-model-data)
        (simu/tx-ent model-id))))

(defn generate-job-schedule!
  [settings {conn :conn}]
  (println "generating job schedule...")
  (let [duration (* (:sim-duration-seconds settings) 1000)
        sim-model (make-model settings conn)
        new-test (sim/create-test conn sim-model {:db/id (d/tempid :test)
                                                  :test/duration duration})]
    (println "New job schedule created: " (:db/id new-test))
    (println "To run a simulation using this schedule, try arguments simulate -e " (:db/id new-test))))


(defn- format-schedule
  "Prepares a simulation id result from query for output via print-table"
  [db [id duration num-researchers]]
  {"ID" id
   "Duration" (str (/ duration 1000) " secs")
   "Num Researchers" num-researchers
   "Created" (u/created-at db id)})

(defn list-job-schedules
  [{conn :conn}]
  (println "Listing available job schedules...")
  (let [db (d/db conn)]
    (pprint/print-table (map (partial format-schedule db)
                             (d/q '[:find ?e ?dur (count ?agt)
                                    :where [?e]
                                    [?e :test/agents ?agt]
                                    [?e :test/duration ?dur]
                                    [?e :test/type :test.type/cook]]
                                  db)))))
