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

(let [datasets->host-name->cost-atom (atom {})
      datasets->last-update-time-atom (atom {})]

  (defn reset-data-local-costs! []
    (reset! datasets->host-name->cost-atom {})
    (reset! datasets->last-update-time-atom {}))

  (defn update-data-local-costs
    "Updates the current data local costs. Costs larger than the configured max-cost will be capped at max-cost
     and negative costs will be reset to 0."
    [datasets->host->cost datasets-to-remove]
    (let [{:keys [maximum-cost]} (config/data-local-fitness-config)
          clean-datasets->host->cost (pc/map-vals (fn [host->cost]
                                                   (pc/map-vals (fn [cost] (-> cost (min maximum-cost) (max 0)))
                                                                host->cost))
                                                 datasets->host->cost)]
      (swap! datasets->host-name->cost-atom (fn update-datasets->host-name->cost-atom-atom
                                [current]
                                (let [remove-old-values (apply dissoc current datasets-to-remove)]
                                  (merge remove-old-values clean-datasets->host->cost))))
      (swap! datasets->last-update-time-atom (fn update-last-update-time
                                [current]
                                (let [remove-old-values (apply dissoc current datasets-to-remove)
                                      new-values (pc/map-from-keys (constantly (t/now)) (keys clean-datasets->host->cost))]
                                  (merge remove-old-values new-values))))))

  (defn get-data-local-costs
    "Returns the current cost for jobs to run on each host"
    []
    @datasets->host-name->cost-atom)


  (defn get-last-update-time
    "Returns the last update time information"
    []
    @datasets->last-update-time-atom)

  (defn jobs-to-update
    "Return the list of job ids to update data locality costs for in the next iteration.
     The number of jobs is limited by the configured batch size."
    [db]
    (let [{:keys [batch-size]} (config/data-local-fitness-config)
          pending-jobs (->> db
                            util/get-pending-job-ents
                            (filter (fn [j] (not (empty? (:job/datasets j)))))
                            (map (fn [{:keys [job/uuid job/submit-time job/datasets]}]
                                   {:job/uuid uuid
                                    :job/submit-time submit-time
                                    :job/datasets (into #{} (map util/make-dataset-map datasets))})))
          datasets->last-update-time @datasets->last-update-time-atom
          have-data?->job (group-by (fn [job] (contains? datasets->last-update-time (:job/datasets job)))
                                    pending-jobs)
          missing-data (->> (get have-data?->job false [])
                            (sort-by :job/submit-time))
          sorted-have-data (->> (get have-data?->job true [])
                                (sort-by (fn [j] (datasets->last-update-time (:job/datasets j)))))
          pending-job-datasets (->> pending-jobs
                                    (map :job/datasets)
                                    (into (hash-set)))
          no-longer-waiting (set/difference (-> datasets->last-update-time keys set)
                                            pending-job-datasets)]
      {:to-fetch (into [] (take batch-size (concat missing-data sorted-have-data)))
       :to-remove no-longer-waiting})))

(defn fetch-data-local-costs
  "Contacts the server to obtain the data local costs for the given job ids"
  [jobs]
  (let [{:keys [cost-endpoint]} (config/data-local-fitness-config)
        job-uuid->datasets (into {} (map (fn [job] [(str (:job/uuid job)) (:job/datasets job)])
                                         jobs))
        request {:batch (UUID/randomUUID)
                 :tasks (map (fn [job] {:task_id (str (:job/uuid job))
                                        :datasets (:job/datasets job)}))}
        _ (log/debug "Updating data local costs for: " job-uuid->datasets)
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
                  [(job-uuid->datasets task_id) costs])))
         (into {}))))

(defn fetch-and-update-data-local-costs
  [db]
  (let [{:keys [to-fetch to-remove]} (jobs-to-update db)]
    (when (not (empty? to-fetch))
      (log/debug "Updating data local costs for" (map :job/uuid to-fetch))
      (let [new-costs (fetch-data-local-costs to-fetch)]
        (log/info "Got updated costs for" (count new-costs) "jobs")
        (log/debug "Got updated costs" new-costs)
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
  [datasets hostname max-cost]
  (let [data-local-costs (get-data-local-costs)
        cost (get-in data-local-costs [datasets hostname] max-cost)]
    (- 1 (double (/ cost max-cost)))))

(deftype DataLocalFitnessCalculator [^VMTaskFitnessCalculator base-calculator data-locality-weight maximum-cost]
  VMTaskFitnessCalculator
  (getName [this] (-> this .getClass .getSimpleName))
  (calculateFitness [this task-request target-vm tracker-state]
    (let [base-fitness (.calculateFitness base-calculator task-request target-vm tracker-state)
          {:keys [job/uuid job/datasets]} (:job task-request)]
      (if datasets
        (let [normalized-fitness (get-normalized-fitness datasets (.getHostname target-vm) maximum-cost)
              data-local-fitness (* data-locality-weight normalized-fitness)]
          (+ data-local-fitness (* (- 1 data-locality-weight) base-fitness)))
        base-fitness))))

(def data-local-fitness-calculator "cook.mesos.data-locality/make-data-local-fitness-calculator")

(defn make-data-local-fitness-calculator
  "Loads settings from configuration to build a data local fitness calculator"
  []
  (let [{:keys [base-calculator data-locality-weight maximum-cost]} (config/data-local-fitness-config)]
    (->DataLocalFitnessCalculator base-calculator data-locality-weight maximum-cost)))
