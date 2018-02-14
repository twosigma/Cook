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
(ns cook.mesos.optimizer
  (:require [cheshire.core :as cheshire]
            [chime :refer [chime-at chime-ch]]
            [clj-http.client :as http]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clj-time.periodic :as periodic]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [cook.util :refer [lazy-load-var]]
            [cook.mesos.util :as util]
            [com.rpl.specter :as sp]
            [datomic.api :as d :refer (q)]
            [schema.core :as s]))
(def PosNum
  (s/both s/Num (s/pred pos? 'pos?)))

(def PosInt
  (s/both s/Int (s/pred pos? 'pos?)))

(def NonNegInt
  (s/both s/Int (s/pred #(<= 0 %) 'pos?)))

(def TimePeriodMs NonNegInt)

;; Elements missing:
;;   1. Attributes (chip set, az, ..) -- have a combinitorial problem here
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
  {TimePeriodMs {:suggested-matches {HostInfo [s/Uuid]}}})

(defprotocol Optimizer
  "Protocol defining a tool to produce a schedule to execute"
  (produce-schedule [this queue running available host-infos]
                    "Returns a schedule of what jobs to place on what machines
                     and what hosts to purchase at different time steps.
                     Conforms to the Schedule schema above

                     Parameters:
                     queue -- Ordered list of jobs to run (see cook.mesos.schema job)
                     running -- Set of tasks running (see cook.mesos.schema instance)
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
   get-offers -- fn, no args fn that returns a set of offers
   host-feed -- instance of HostFeed
   optimizer -- instance of Optimizer
   interval -- joda-time period

   Raises an exception if there was a problem in the execution"
  [get-queue get-running get-offers host-feed optimizer]
  (let [queue (future (get-queue))
        running (future (get-running))
        offers (future (get-offers))
        host-infos (get-available-host-info host-feed)
        _ (s/validate [HostInfo] host-infos)
        schedule (produce-schedule optimizer @queue @running @offers @host-infos)]
    (s/validate Schedule schedule)))

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
