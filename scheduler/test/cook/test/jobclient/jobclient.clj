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
(ns cook.test.jobclient.jobclient
  (:use clojure.test)
  (:require [clojure.core.async :as async]
            [cook.authorization :as auth]
            [cook.components :as components]
            [cook.test.testutil :refer (with-test-server restore-fresh-database! create-dummy-instance)]
            [datomic.api :as d])
  (:import com.twosigma.cook.jobclient.FetchableURI
           com.twosigma.cook.jobclient.FetchableURI$Builder
           com.twosigma.cook.jobclient.Group
           com.twosigma.cook.jobclient.Group$Builder
           com.twosigma.cook.jobclient.Group$Status
           com.twosigma.cook.jobclient.GroupListener
           com.twosigma.cook.jobclient.HostPlacement
           com.twosigma.cook.jobclient.HostPlacement$Builder
           com.twosigma.cook.jobclient.HostPlacement$Type
           com.twosigma.cook.jobclient.Job
           com.twosigma.cook.jobclient.Job$Builder
           com.twosigma.cook.jobclient.Job$Status
           com.twosigma.cook.jobclient.JobClient
           com.twosigma.cook.jobclient.JobClient$Builder
           com.twosigma.cook.jobclient.JobListener
           com.twosigma.cook.jobclient.StragglerHandling
           com.twosigma.cook.jobclient.StragglerHandling$Builder
           com.twosigma.cook.jobclient.StragglerHandling$Type))

(def port 3001)

(defn make-job
  [& {:keys [uuid name command mem cpus group retries]
      :or {uuid (java.util.UUID/randomUUID)
           name "cookjob"
           command "sleep 10"
           mem 100.0
           cpus 1.0
           retries (int 3)
           group nil}}]
  (-> (Job$Builder.)
      (.setUUID uuid)
      (.setName name)
      (.setCommand command)
      (.setMemory mem)
      (.setCpus cpus)
      (.setRetries (int retries))
      (cond->
        (not (nil? group)) (.setGroup group))
      (.build)))

(defn make-host-placement
  [& {:keys [type attribute]
      :or {type "ALL"
           attribute "lol"}}]
  (-> (HostPlacement$Builder.)
      (.setType (HostPlacement$Type/fromString type))
      (cond->
        (= type "ATTRIBUTE_EQUALS") (.setParameter "attribute" attribute))
      (.build)))

(defn make-straggler-handling
  [& {:keys [type quantile multiplier]
      :or {type "NONE"
           quantile 0.5
           multiplier 2.0}}]
  (-> (StragglerHandling$Builder.)
      (.setType (StragglerHandling$Type/fromString type))
      (cond->
        (= type "QUANTILE_DEVIATION") ((fn [b]
                                         (.setParameter b "quantile" quantile)
                                         (.setParameter b "multiplier" multiplier))))
      (.build)))

(defn make-group
  [& {:keys [uuid name hp sh]
      :or {uuid (java.util.UUID/randomUUID)
           name "cookgroup"
           hp (make-host-placement)
           sh (make-straggler-handling)}}]
  (-> (Group$Builder.)
      (.setUUID uuid)
      (.setName name)
      (.setHostPlacement hp)
      (.setStragglerHandling sh)
      (.build)))

(defn make-job-listener
  [fun]
  (reify JobListener (onStatusUpdate [this job] (fun job))))

(defn make-group-listener
  [fun]
  (reify GroupListener (onStatusUpdate [this group] (fun group))))

(defn change-job-state
  [conn juuid target]
  (case target
    :job.state/waiting ; Make a failed instance. Assuming that job has retries left, it will go into waiting
      (let [inst (create-dummy-instance conn [:job/uuid juuid]
                                        :instance-status :instance.status/unknown)]
        @(d/transact conn [[:instance/update-state inst :instance.status/failed [:reason/name :unknown]]]))
    :job.state/completed
      (let [inst (create-dummy-instance conn [:job/uuid juuid]
                                        :instance-status :instance.status/running)]
        @(d/transact conn [[:instance/update-state inst :instance.status/success [:reason/name :unknown]]]))))

(deftest jobclient-tester
  ; Start a mock server for testing
  (let [conn (restore-fresh-database! "datomic:mem://jobclient")
        db (d/db conn)
        jc (-> (JobClient$Builder.)
             (.setHost "localhost")
             (.setPort port)
             (.setJobEndpoint "rawscheduler")
             (.setGroupEndpoint "group")
             (.setStatusUpdateInterval 0.1)
             (.build))]
    (with-test-server [conn port]
      (testing "Submit loose job"
        (let [juuid (java.util.UUID/randomUUID)
              command "job-command"
              job (make-job :uuid juuid :command command)]
          (-> jc (.submit (java.util.ArrayList. [job])))
          (is (= command (-> jc (.query [juuid]) (.get juuid) (.getCommand))))))

      (testing "Test job with listener"
        (let [juuid (java.util.UUID/randomUUID)
              command "job-command"
              job (make-job :uuid juuid :command command)
              jchan (async/chan 1)
              jlistener (make-job-listener #(async/>!! jchan %))]
          (-> jc (.submit (java.util.ArrayList. [job]) jlistener))
          (change-job-state conn juuid :job.state/completed)
          (is (= (-> (async/<!! jchan) (.getStatus)) (Job$Status/fromString "COMPLETED")))))

      (testing "1 group, 1 job"
        (let [guuid (java.util.UUID/randomUUID)
              juuid (java.util.UUID/randomUUID)
              gname "group-name"
              group (make-group :uuid guuid :name gname)
              job (make-job :uuid juuid :group group)]
          (-> jc (.submitWithGroups (java.util.ArrayList. [job]) (java.util.ArrayList. [group])))
          (is (= gname (-> jc (.queryGroups [guuid]) (.get guuid) (.getName))))))
      (testing "1 group, 1 job, group settings"
        (let [guuid (java.util.UUID/randomUUID)
              juuid (java.util.UUID/randomUUID)
              gname "group-name"
              sh (make-straggler-handling :type "QUANTILE_DEVIATION")
              hp (make-host-placement :type "ATTRIBUTE_EQUALS")
              group (make-group :uuid guuid :name gname :straggler-handling sh :host-placement hp)
              job (make-job :uuid juuid :group group)]
          (-> jc (.submitWithGroups (java.util.ArrayList. [job]) (java.util.ArrayList. [group])))
          (is (= gname (-> jc (.queryGroups [guuid]) (.get guuid) (.getName))))))

      (testing "1 group, multiple jobs"
        (let [guuid (java.util.UUID/randomUUID)
              juuids (repeatedly 5 #(java.util.UUID/randomUUID))
              gname "group-name"
              group (make-group :uuid guuid :name gname)
              jobs (map #(make-job :uuid % :group group) juuids)
              _ (-> jc (.submitWithGroups (java.util.ArrayList. jobs) (java.util.ArrayList. [group])))
              retrieved-group (-> jc (.queryGroups [guuid]) (.get guuid))
              retrieved-jobs (set (.getJobs retrieved-group))]
          (is (= gname (.getName retrieved-group)))
          (is (every? true? (map #(contains? retrieved-jobs %) juuids)))))

      (testing "Test group listening for WAITING status"
        (let [guuid (java.util.UUID/randomUUID)
              juuids (repeatedly 5 #(java.util.UUID/randomUUID))
              group (make-group :uuid guuid)
              jobs (map #(make-job :uuid % :group group :retries 10) juuids)
              gchan (async/chan 1)
              glistener (make-group-listener #(async/>!! gchan %))]
          (-> jc (.submitWithGroups (java.util.ArrayList. jobs) (java.util.ArrayList. [group]) glistener))
          (doall (map #(change-job-state conn % :job.state/waiting) juuids))
          (is (= (-> (async/<!! gchan) (.getStatus)) (Group$Status/fromString "WAITING")))))

      (testing "Test group listening for COMPLETED status"
        (let [guuid (java.util.UUID/randomUUID)
              juuids (repeatedly 5 #(java.util.UUID/randomUUID))
              group (make-group :uuid guuid)
              jobs (map #(make-job :uuid % :group group) juuids)
              gchan (async/chan 1)
              glistener (make-group-listener #(async/>!! gchan %))]
          (-> jc (.submitWithGroups (java.util.ArrayList. jobs) (java.util.ArrayList. [group]) glistener))
          (doall (map #(change-job-state conn % :job.state/completed) juuids))

          (is (= (-> (async/<!! gchan) (.getStatus)) (Group$Status/fromString "COMPLETED")))))

      (testing "Query group manually"
        (let [guuid (java.util.UUID/randomUUID)
              gname "this-test-is-sweet"
              group (make-group :uuid guuid :name gname)]
          (-> jc (.submitWithGroups (java.util.ArrayList. []) (java.util.ArrayList. [group])))

          (is (= (-> jc (.queryGroup guuid) (.getName)) gname))))

      ; Looping infinitely after finishing the tests keeps the server running on a separate thread
      ; This allows interaction with the server from other sources (such as curl), which is useful when
      ; debugging broken tests.
      ; (while true))
    )))
