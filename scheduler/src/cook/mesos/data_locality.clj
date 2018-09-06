(ns cook.mesos.data-locality
  (:require [cheshire.core :as cheshire]
            [clj-http.client :as http]
            [clj-time.coerce :as tc]
            [clj-time.core :as t]
            [clj-time.format :as tf]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [cook.config :as config]
            [cook.mesos.util :as util]
            [datomic.api :as d]
            [metrics.histograms :as histograms]
            [metrics.timers :as timers]
            [plumbing.core :as pc])
  (:import com.google.common.cache.Cache
           com.netflix.fenzo.VMTaskFitnessCalculator
           com.netflix.fenzo.plugins.BinPackingFitnessCalculators
           java.util.UUID))

(def partition-date-format (:basic-date tf/formatters))

(defonce ^Cache job-uuid->dataset-maps-cache (util/new-cache))

(defn- make-partition-map
  [partition-type partition]
  (let [format-date (fn format-date [date] (tf/unparse partition-date-format (tc/from-date date)))]
    (case partition-type
      "date" {"begin" (format-date (:dataset.partition/begin partition))
              "end" (format-date (:dataset.partition/end partition))})))

(defn- make-dataset-maps
  [job]
  (->> job
       :job/datasets
       (map (fn [{:keys [dataset/partitions dataset/partition-type dataset/parameters]}]
              (let [partitions (when (not (empty? partitions))
                                 (->> partitions
                                      (map (partial make-partition-map partition-type))
                                      (into #{})))]
                (cond-> {:dataset (into {} (map (fn [p] [(:dataset.parameter/key p) (:dataset.parameter/value p)])
                                                parameters))}
                  partitions (assoc :partitions partitions)))))
       (into #{})))

(defn get-dataset-maps
  "Returns the (possibly cached) datasets for the given job"
  [job]
  (util/lookup-cache! job-uuid->dataset-maps-cache
                      :job/uuid
                      make-dataset-maps
                      job))

(histograms/defhistogram [cook-mesos data-locality cost-staleness])

(let [datasets->host-name->cost-atom (atom {})
      datasets->last-update-time-atom (atom {})]

  (defn reset-data-local-costs! []
    (reset! datasets->host-name->cost-atom {})
    (reset! datasets->last-update-time-atom {}))

  (defn update-data-local-costs
    "Updates the current data local costs. Costs larger than the configured max-cost will be capped at max-cost
     and negative costs will be reset to 0."
    [datasets->host->cost stale-datasets]
    (let [{:keys [cache-ttl-ms]} (config/data-local-fitness-config)
          datasets->last-update-time @datasets->last-update-time-atom
          current-time (t/now)
          datasets-to-remove (filter (fn [datasets]
                                       (when-let [update-time (datasets->last-update-time datasets nil)]
                                         (< cache-ttl-ms
                                            (t/in-millis (t/interval update-time current-time)))))
                                     stale-datasets)
          clean-datasets->host->cost (pc/map-vals (fn [host->cost]
                                                    (pc/map-vals (fn [cost] (-> cost (min 1.0) (max 0)))
                                                                 host->cost))
                                                  datasets->host->cost)]
      (doseq [[_ update-time] (apply dissoc datasets->last-update-time stale-datasets)]
        (histograms/update! cost-staleness (t/in-millis (t/interval update-time current-time))))
      (swap! datasets->host-name->cost-atom (fn update-datasets->host-name->cost-atom-atom
                                              [current]
                                              (let [remove-old-values (apply dissoc current datasets-to-remove)]
                                                (merge remove-old-values clean-datasets->host->cost))))
      (swap! datasets->last-update-time-atom (fn update-last-update-time
                                               [current]
                                               (let [remove-old-values (apply dissoc current datasets-to-remove)
                                                     new-values (pc/map-from-keys (constantly current-time) (keys clean-datasets->host->cost))]
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
    (let [pending-jobs (->> db
                            util/get-pending-job-ents
                            (filter (fn [j] (not (empty? (:job/datasets j)))))
                            (map (fn [{:keys [job/uuid job/submit-time] :as job}]
                                   {:job/uuid uuid
                                    :job/submit-time submit-time
                                    :job/datasets (get-dataset-maps job)})))
          datasets->last-update-time @datasets->last-update-time-atom
          have-data?->job (group-by (fn [job] (contains? datasets->last-update-time (:job/datasets job)))
                                    pending-jobs)
          missing-data (->> (get have-data?->job false [])
                            (sort-by :job/submit-time))
          sorted-have-data (->> (get have-data?->job true [])
                                (sort-by (fn [j] (datasets->last-update-time (:job/datasets j)))))
          pending-job-datasets (->> pending-jobs
                                    (map :job/datasets)
                                    (into #{}))
          no-longer-waiting (set/difference (-> datasets->last-update-time keys set)
                                            pending-job-datasets)]
      {:to-fetch (concat missing-data sorted-have-data)
       :to-remove no-longer-waiting})))

(defn fetch-data-local-costs
  "Contacts the server to obtain the data local costs for the given job ids"
  [jobs]
  (let [{:keys [cost-endpoint]} (config/data-local-fitness-config)
        job-uuid->datasets (into {} (map (fn [job] [(str (:job/uuid job)) (:job/datasets job)])
                                         jobs))
        batch-id (UUID/randomUUID)
        request {:batch batch-id
                 :tasks (map (fn [job] {:task_id (str (:job/uuid job))
                                        :datasets (:job/datasets job)})
                             jobs)}
        _ (log/info "Updating data local costs for" (count jobs) "tasks with batch id" batch-id)
        _ (log/debug "Updating data local costs :" (cheshire/generate-string request))
        {:keys [body]} (http/post cost-endpoint {:body (cheshire/generate-string request)
                                                 :content-type :json
                                                 :accept :json
                                                 :as :json-string-keys
                                                 :spnego-auth true})
        _ (log/debug "Got response:" body)]
    (pc/for-map [{:strs [task_id node_costs]} (body "costs")]
       (job-uuid->datasets task_id)
       (pc/for-map [{:strs [node cost]} node_costs] node cost))))

(defn fetch-and-update-data-local-costs
  "Determine the datasets which need to be updated, fetch the costs, and update the cache with the
   latest costs."
  [db]
  (let [{:keys [batch-size]} (config/data-local-fitness-config)
        {:keys [to-fetch to-remove]} (jobs-to-update db)
        batch (into [] (take batch-size to-fetch))]
    (when (not (empty? batch))
      (log/debug "Updating data local costs for" (map :job/uuid batch))
      (let [new-costs (fetch-data-local-costs batch)]
        (log/info "Got updated costs for" (count new-costs) "jobs")
        (log/debug "Got updated costs" new-costs)
        (update-data-local-costs new-costs to-remove)))))

(timers/deftimer [cook-mesos data-locality cost-update-duration])

(defn start-update-cycles!
  "Starts a `chime-at-ch` on `trigger-chan` to update data local costs."
  [datomic-conn trigger-chan]
  (log/info "Starting data local service update chan")
  (util/chime-at-ch trigger-chan
                    (fn [] (timers/time!
                            cost-update-duration
                            (fetch-and-update-data-local-costs (d/db datomic-conn))))
                    {:error-handler (fn [e]
                                      (log/error e "Error updating data local costs"))}))

(deftype DataLocalFitnessCalculator [^VMTaskFitnessCalculator base-calculator data-locality-weight]
  VMTaskFitnessCalculator
  (getName [this] (-> this .getClass .getSimpleName))
  (calculateFitness [this task-request target-vm tracker-state]
    (let [base-fitness (.calculateFitness base-calculator task-request target-vm tracker-state)
          {:keys [job/uuid] :as job} (:job task-request)
          datasets (get-dataset-maps job)]
      (if-not (empty? (or datasets []))
        (let [normalized-fitness (- 1.0
                                    (get-in (get-data-local-costs) [datasets (.getHostname target-vm)] 1.0))
              data-local-fitness (* data-locality-weight normalized-fitness)]
          (log/debug "Computed data local fitness:" {:hostname (.getHostname target-vm)
                                                     :job uuid
                                                     :costs (get (get-data-local-costs) datasets)})
          (+ data-local-fitness (* (- 1 data-locality-weight) base-fitness)))
        base-fitness))))

(def data-local-fitness-calculator "cook.mesos.data-locality/make-data-local-fitness-calculator")

(defn make-data-local-fitness-calculator
  "Loads settings from configuration to build a data local fitness calculator"
  []
  (let [{:keys [base-calculator data-locality-weight]} (config/data-local-fitness-config)]
    (->DataLocalFitnessCalculator base-calculator data-locality-weight)))
