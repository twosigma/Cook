(ns cook.scheduler.offer
  (:require [cook.compute-cluster :as cc])
  (:import (com.netflix.fenzo
             VirtualMachineLease VirtualMachineLease$Range)))

  (defn offer-resource-values
  [offer resource-name value-type]
  (->> offer :resources (filter #(= (:name %) resource-name)) (map value-type)))

(defn offer-resource-scalar
  [offer resource-name]
  (reduce + 0.0 (offer-resource-values offer resource-name :scalar)))

(defn offer-resource-ranges
  [offer resource-name]
  (reduce into [] (offer-resource-values offer resource-name :ranges)))

(defn offer-value-list-map
  [value-list]
  (->> value-list
       (map #(vector (:name %) (case (:type %)
                                 :value-scalar (:scalar %)
                                 :value-ranges (:ranges %)
                                 :value-set (:set %)
                                 :value-text (:text %)
                                 :value-text->scalar (:text->scalar %)
                                 ; Default
                                 (:value %))))
       (into {})))

(defn get-offer-attr-map
  "Gets all the attributes from an offer and puts them in a simple, less structured map of the form
   name->value"
  [{:keys [attributes compute-cluster hostname resources] :as offer}]
  (let [offer-attributes (offer-value-list-map attributes)
        offer-resources (offer-value-list-map resources)
        cook-attributes (cond->
                          {"HOSTNAME" hostname}
                          compute-cluster
                          (assoc "COOK_MAX_TASKS_PER_HOST" (cc/max-tasks-per-host compute-cluster)
                                 ; Note that this is dynamic and can change from moment to moment.
                                 ; This could be a problem in the agent-attributes-cache. However,
                                 ; the rebalancer ignores this constraint, so it's safe to cache this.
                                 "COOK_NUM_TASKS_ON_HOST" (cc/num-tasks-on-host compute-cluster hostname)
                                 "COOK_COMPUTE_CLUSTER_LOCATION" (cc/compute-cluster->location compute-cluster)))]
    (merge offer-resources offer-attributes cook-attributes)))

(defrecord VirtualMachineLeaseAdapter [offer time attr-map]
  VirtualMachineLease
  (cpuCores [_] (or (offer-resource-scalar offer "cpus") 0.0))
  ; We support disk but support different types of disk, so we set this metric to 0.0 and take care of binpacking disk in the disk-host-constraint
  (diskMB [_] 0.0)
  (getScalarValue [_ name] (or (double (offer-resource-scalar offer name)) 0.0))
  (getScalarValues [_]
    (reduce (fn [result resource]
              (if-let [value (:scalar resource)]
                ;; Do not remove the following fnil--either arg to + can be nil!
                (update-in result [(:name resource)] (fnil + 0.0 0.0) value)
                result))
            {}
            (:resources offer)))
  (getAttributeMap [_] attr-map)
  (getId [_] (-> offer :id :value))
  (getOffer [_] (throw (UnsupportedOperationException.)))
  (getOfferedTime [_] time)
  (getVMID [_] (-> offer :slave-id :value))
  (hostname [_] (:hostname offer))
  (memoryMB [_] (or (offer-resource-scalar offer "mem") 0.0))
  (networkMbps [_] 0.0)
  (portRanges [_] (mapv (fn [{:keys [begin end]}]
                          (VirtualMachineLease$Range. begin end))
                        (offer-resource-ranges offer "ports"))))

(defn offer->lease
  [offer time]
  (->VirtualMachineLeaseAdapter offer time (get-offer-attr-map offer)))