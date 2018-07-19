(ns cook.mesos.data-locality
  (:require [cook.config :as config])
  (:import com.netflix.fenzo.VMTaskFitnessCalculator
           com.netflix.fenzo.plugins.BinPackingFitnessCalculators))

(def data-local-costs (atom {}))

(defn get-normalized-fitness
  "Returns the fitness for the job to run on the given host, normalized by max-cost"
  [uuid hostname max-cost]
  (let [cost (get-in @data-local-costs [uuid hostname] max-cost)]
    (- 1 (double (/ cost max-cost)))))

(deftype DataLocalFitnessCalculator [^VMTaskFitnessCalculator base-calculator data-locality-weight maximum-cost]
  VMTaskFitnessCalculator
  (getName [this] "DataLocalFitnessCalculator")
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
