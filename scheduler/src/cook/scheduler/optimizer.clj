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
(ns cook.scheduler.optimizer
  (:require [chime :refer [chime-at chime-ch]]
            [clojure.tools.logging :as log]
            [cook.tools :as util]
            [cook.util :refer [lazy-load-var NonNegInt PosInt PosNum]]
            [datomic.api :refer [q]]
            [schema.core :as s]))

(def TimePeriodMs NonNegInt)

(def HostInfo
  {:count NonNegInt
   :instance-type s/Str
   :cpus PosNum
   :mem PosNum
   (s/optional-key :gpus) PosNum})

(defprotocol HostFeed
  "Protocol defining a service to get information on hosts that can be purchased."
  (get-available-host-info [this] "Returns a list of host info maps conforming
                                   to HostInfo schema above"))

(def Schedule
  "Schedule returned by optimizer. Each TimePeriodMs is some milliseconds into the future the optimizer
   recommendation map applies.
   The optimizer recommendation map currently only has one key, :suggested-matches which has the matches
   recommended as a value. The matches recommended are structured as a map from the host type in question
   to a list of job UUIDs the optimizer recommends scheduling

   Example schedule:
   {0 {:suggested-matches {{:count 10, :instance-type :mem-optimized, :cpus 10, :mem 200000}
                           [#uuid \"1db521d2-b1d5-41b4-9a93-a7e12abd940b\"]
                           {:count 100, :instance-type :cpu-optimized, :cpus 40, :mem 60000}
                           [#uuid \"5ac521d2-b1d5-41b4-9a93-a7e12abd4326\"]}}
    60000 {:suggested-matches {{:count 10, :instance-type :mem-optimized, :cpus 10, :mem 200000}
                               [#uuid \"724521d2-b1d5-41b4-9a93-a7e12abd940b\"]
                               {:count 100, :instance-type :cpu-optimized, :cpus 40, :mem 60000}
                               [#uuid \"adc521d2-b1d5-41b4-9a93-a7e12abd4326\"]}}}"
  {TimePeriodMs {:suggested-matches {HostInfo [s/Uuid]}}})


(defprotocol Optimizer
  "Protocol defining a tool to produce a schedule to execute"
  (produce-schedule [this queue running available host-infos]
                    "Returns a schedule of what jobs to place on what machines
                     and what hosts to purchase at different time steps.
                     Conforms to the Schedule schema above

                     Parameters:
                     queue -- Ordered list of jobs to run (see cook.schema job)
                     running -- Set of tasks running (see cook.schema instance)
                     available -- Set of offers outstanding
                     host-infos -- Host infos from HostFeed

                     Returns:
                     A schedule: a map where the keys are milliseconds in the future
                     and  the values are recommendations to take at that point in the future"))

(defn create-dummy-host-feed
  "Returns an instance of HostFeed which returns an empty list of hosts"
  [_]
  (log/info "Creating dummy host feed")
  (reify HostFeed
    (get-available-host-info [this]
      [])))

(defn create-dummy-optimizer
  "Returns an instance of Optimizer which returns an empty schedule"
  [_]
  (log/info "Creating dummy optimizer")
  (reify Optimizer
    (produce-schedule [this queue running available host-infos]
      {0 {:suggested-matches {}}})))

(defn optimizer-cycle!
  "Starts a cycle that:
   1. Gets queue, running, offer and purchasable host info
   2. Calls the `optimizer` to get a schedule

   Parameters:
   get-queue -- fn, no args fn that returns ordered list of jobs to run
   get-running -- fn, no args fn that returns a set of tasks running
   get-offers -- fn, 1 arg fn that takes a pool name and returns a set of offers in that pool
   host-feed -- instance of HostFeed
   optimizer -- instance of Optimizer
   interval -- joda-time period

   Raises an exception if there was a problem in the execution"
  [get-queue get-running get-offers host-feed optimizer]
  (let [queue (future (get-queue))
        running (future (get-running))
        ; TODO: Call get-offers. Integration of the optimizer with pools is not yet implemented.
        offers (future [])
        host-infos (get-available-host-info host-feed)
        _ (s/validate [HostInfo] host-infos)
        schedule (produce-schedule optimizer @queue @running @offers host-infos)]
    (s/validate Schedule schedule)
    schedule))

(defn start-optimizer-cycles!
  "Every interval, call `optimizer-cycle!`.
   Returns a function of no arguments to stop"
  [get-queue get-running get-offers optimizer-config trigger-chan]
  (log/info "Starting optimization cycler")
  (let [construct (fn construct [{:keys [create-fn config] :as c}]
                    ((lazy-load-var create-fn) config))
        host-feed (-> optimizer-config :host-feed construct)
        optimizer (-> optimizer-config :optimizer construct)]
    (log/info "Optimizer constructed")
    (util/chime-at-ch trigger-chan
                      (fn []
                        (log/info "Starting optimization cycle")
                        (optimizer-cycle! get-queue
                                          get-running
                                          get-offers
                                          host-feed
                                          optimizer))
                      {:error-handler (fn [e]
                                        (log/warn e "Error running optimizer"))})))
