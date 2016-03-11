(ns cook.sim.runner
  (:require [clojure.java.io :as io]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [datomic.api :as d]
            [simulant.sim :as sim]
            [simulant.util :as simu]
            [cook.sim.database :as db]
            [cook.sim.database :as data]))

(defmethod sim/create-sim :test.type/cook
  [sim-conn test sim]
  (-> @(d/transact sim-conn (sim/construct-basic-sim test sim))
      (simu/tx-ent (:db/id sim))))

(defn schedule-cook-job
  ;; TODO use the builtin HTTP client behavior in simulant repo instead?
  ;; it may be more robust in error handling etc - need to investigate
  [cook-uri job-id {duration :job/duration
                    mem :job/memory
                    cpu :job/cpu
                    exit-code :job/exit-code}]
  (let [cmd (str "sleep " duration "; exit " exit-code)
        ;; TODO: determine if max_retries and max_runtime need to be configured,
        ;; or even randomized per job?
        body (json/generate-string {:jobs [{:max_retries 3
                                            :max_runtime 86400000
                                            :mem mem
                                            :cpus cpu
                                            :uuid job-id
                                            :command cmd
                                            }]})]
    (println "scheduling cook job with payload: " body)
    (http/post (str cook-uri "/rawscheduler")
               {:body body :content-type :json})))

(defmethod sim/perform-action :action.type/job
  [action process]
  (let [sim (-> process :sim/_processes simu/only)
        action-log (simu/getx sim/*services* :simulant.sim/actionLog)
        before (System/nanoTime)
        job-uuid (d/squuid)]
    (schedule-cook-job (:sim/systemURI sim) job-uuid action)
    ;; TODO: record our own timestamp manually rather than relying on
    ;; datomic transactions?
    (action-log [{:actionLog/nsec (- (System/nanoTime) before)
                  :db/id (d/tempid :db.part/user)
                  :actionLog/sim (simu/e sim)
                  :actionLog/action (simu/e action)
                  :job/uuid job-uuid}])))

(defn simulate!
  [settings sim-db test-id]
  (println "Running a simulation for schedule " test-id "...")
  (let [{:keys [sim-db-uri cook-api-uri process-count]} settings
        conn (:conn sim-db)
        cook-sim (sim/create-sim conn
                                 (-> (d/db conn) (d/entity test-id))
                                 {:db/id  (d/tempid :sim)
                                  :sim/systemURI cook-api-uri
                                  :sim/processCount process-count
                                  :source/codebase (simu/gen-codebase)})]
    (println "Created simulation " (:db/id cook-sim) ".")
    (println "After jobs are finished, try running report -e " (:db/id cook-sim) ".")
    (sim/create-action-log conn cook-sim)
    ;; Since this test relies on real cook, we can never configure the clock
    ;; to run at any speed other than 1x
    (sim/create-fixed-clock conn cook-sim {:clock/multiplier 1})
    (time
     (mapv (fn [proc] @(:runner proc))
           (into [] (repeatedly (:sim/processCount cook-sim)
                                #(sim/run-sim-process sim-db-uri (:db/id cook-sim))))))))



