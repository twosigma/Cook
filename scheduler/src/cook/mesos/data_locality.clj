(ns cook.mesos.data-locality
  (:require [cook.config :as config]
            [plumbing.core :as pc])
  (:import com.netflix.fenzo.VMTaskFitnessCalculator
           com.netflix.fenzo.plugins.BinPackingFitnessCalculators))

(let [data-local-costs (atom {})]
  (defn update-data-local-costs
    "Updates the current data local costs. Costs larger than the configured max-cost will be capped at max-cost
     and negative costs will be reset to 0."
    [job->host->cost]
    (let [{:keys [maximum-cost]} (config/data-local-fitness-config)]
      (->> job->host->cost
           (pc/map-vals (fn [host->cost]
                          (pc/map-vals (fn [cost] (-> cost (min maximum-cost) (max 0)))
                                       host->cost)))
           (reset! data-local-costs))))
  (defn get-data-local-costs
    "Returns the current cost for jobs to run on each host"
    []
    @data-local-costs))

(defn get-normalized-fitness
  "Returns the fitness for the job to run on the given host, normalized by max-cost.
   If the host doesn't have a cost specified, assume max-cost."
  [uuid hostname max-cost]
  (let [data-local-costs (get-data-local-costs)
        cost (get-in data-local-costs [uuid hostname] max-cost)]
    (- 1 (double (/ cost max-cost)))))

(deftype DataLocalFitnessCalculator [^VMTaskFitnessCalculator base-calculator data-locality-weight maximum-cost]
  VMTaskFitnessCalculator
  (getName [this] (-> this .getClass .getSimpleName))
  (calculateFitness [this task-request target-vm tracker-state]
    (let [base-fitness (.calculateFitness base-calculator task-request target-vm tracker-state)
          {:keys [job/uuid job/supports-data-locality]} (:job task-request)]
      (if supports-data-locality
        (let [normalized-fitness (get-normalized-fitness uuid (.getHostname target-vm) maximum-cost)
              data-local-fitness (* data-locality-weight normalized-fitness)]
          (+ data-local-fitness (* (- 1 data-locality-weight) base-fitness)))
        base-fitness))))

(defn make-data-local-fitness-calculator
  "Loads settings from configuration to build a data local fitness calculator"
  []
  (let [{:keys [base-calculator data-locality-weight maximum-cost]} (config/data-local-fitness-config)]
    (->DataLocalFitnessCalculator base-calculator data-locality-weight maximum-cost)))
