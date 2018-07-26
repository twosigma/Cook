(ns cook.mesos.data-locality
  (:require [cheshire.core :as cheshire]
            [clj-http.client :as http]
            [clj-time.core :as t]
            [clojure.set :as set]
            [cook.config :as config]
            [cook.mesos.util :as util]
            [plumbing.core :as pc]
            [clojure.tools.logging :as log]
            [datomic.api :as d])
  (:import com.netflix.fenzo.VMTaskFitnessCalculator
           com.netflix.fenzo.plugins.BinPackingFitnessCalculators
           java.util.UUID))

(let [data-local-costs (atom {})
      last-update-time (atom {})]

  (defn reset-data-local-costs! []
    (reset! data-local-costs {})
    (reset! last-update-time {}))

  (defn update-data-local-costs
    "Updates the current data local costs. Costs larger than the configured max-cost will be capped at max-cost
     and negative costs will be reset to 0."
    [job->host->cost uuids-to-remove]
    (let [{:keys [maximum-cost]} (config/data-local-fitness-config)
          clean-job->host->cost (pc/map-vals (fn [host->cost]
                                               (pc/map-vals (fn [cost] (-> cost (min maximum-cost) (max 0)))
                                                            host->cost))
                                             job->host->cost)]
      (swap! data-local-costs (fn update-data-local-costs-atom
                                [current]
                                (let [remove-old-values (apply dissoc current uuids-to-remove)]
                                  (merge remove-old-values clean-job->host->cost))))
      (swap! last-update-time (fn update-last-update-time
                                [current]
                                (let [remove-old-values (apply dissoc current uuids-to-remove)
                                      new-values (pc/map-from-keys (constantly (t/now)) (keys clean-job->host->cost))]
                                  (merge remove-old-values new-values))))))

  (defn get-data-local-costs
    "Returns the current cost for jobs to run on each host"
    []
    @data-local-costs)

  (defn job-ids-to-update
    "Return the list of job ids to update data locality costs for in the next iteration.
     The number of jobs is limited by the configured batch size."
    [db]
    (let [{:keys [batch-size]} (config/data-local-fitness-config)
          pending-jobs (->> db
                            util/get-pending-job-ents
                            (filter :job/data-local))
          last-update-time-val @last-update-time
          have-data?->job (group-by (fn [job] (contains? last-update-time-val (:job/uuid job)))
                                    pending-jobs)
          missing-data (->> (get have-data?->job false [])
                            (sort-by :job/submit-time)
                            (map :job/uuid))
          sorted-have-data (->> (get have-data?->job true [])
                                (map :job/uuid)
                                (sort-by last-update-time-val))
          pending-job-uuids (->> pending-jobs
                                 (map :job/uuid)
                                 (into (hash-set)))
          no-longer-waiting (set/difference (into (hash-set) (keys last-update-time-val))
                                            pending-job-uuids)]
      {:to-fetch (into [] (take batch-size (concat missing-data sorted-have-data)))
       :to-remove no-longer-waiting})))

(defn fetch-data-local-costs
  "Contacts the server to obtain the data local costs for the given job ids"
  [job-ids]
  (let [{:keys [cost-endpoint]} (config/data-local-fitness-config)
        request {:batch (UUID/randomUUID)
                 :tasks job-ids}
        _ (log/debug "Updating data local costs for: " job-ids)
        {:keys [body]} (http/post cost-endpoint {:body (cheshire/generate-string request)
                                                 :content-type :json
                                                 :accept :json
                                                 :as :json-string-keys
                                                 :spnego-auth true})
        costs (body "costs")]
    (->> costs
         (map (fn [{:strs [task_id node_costs]}]
                (let [costs (->> node_costs
                                 (map (fn [{:strs [node cost]}]
                                        [node cost]))
                                 (into {}))]
                  [task_id costs])))
         (into {}))))

(defn fetch-and-update-data-local-costs
  [db]
  (let [{:keys [to-fetch to-remove]} (job-ids-to-update db)]
    (when (not (empty? to-fetch))
      (log/debug "Updating data local costs for " to-fetch)
      (let [new-costs (fetch-data-local-costs to-fetch)]
        (log/debug "Got updated costs " new-costs)
        (update-data-local-costs new-costs to-remove)))))

(defn start-update-cycles!
  [datomic-conn trigger-chan]
  (log/info "Starting data local service update chan")
  (util/chime-at-ch trigger-chan
                    (fn [] (fetch-and-update-data-local-costs (d/db datomic-conn)))
                    {:error-handler (fn [e]
                                      (log/error e "Error updating data local costs"))}))

(defn get-normalized-fitness
  "Returns the fitness for the job to run on the given host, normalized by max-cost.
   If the host doesn't have a cost specified, assume max-cost."
  [uuid hostname max-cost]
  (let [data-local-costs (get-data-local-costs)
        cost (get-in data-local-costs [(str uuid) hostname] max-cost)]
    (- 1 (double (/ cost max-cost)))))

(deftype DataLocalFitnessCalculator [^VMTaskFitnessCalculator base-calculator data-locality-weight maximum-cost]
  VMTaskFitnessCalculator
  (getName [this] (-> this .getClass .getSimpleName))
  (calculateFitness [this task-request target-vm tracker-state]
    (let [base-fitness (.calculateFitness base-calculator task-request target-vm tracker-state)
          {:keys [job/uuid job/data-local]} (:job task-request)]
      (if data-local
        (let [normalized-fitness (get-normalized-fitness uuid (.getHostname target-vm) maximum-cost)
              data-local-fitness (* data-locality-weight normalized-fitness)]
          (+ data-local-fitness (* (- 1 data-locality-weight) base-fitness)))
        base-fitness))))

(def data-local-fitness-calculator "cook.mesos.data-locality/make-data-local-fitness-calculator")

(defn make-data-local-fitness-calculator
  "Loads settings from configuration to build a data local fitness calculator"
  []
  (let [{:keys [base-calculator data-locality-weight maximum-cost]} (config/data-local-fitness-config)]
    (->DataLocalFitnessCalculator base-calculator data-locality-weight maximum-cost)))
