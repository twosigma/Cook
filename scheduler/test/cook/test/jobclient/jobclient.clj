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
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [cook.test.postgres]
            [cook.test.testutil :refer [create-dummy-instance restore-fresh-database! setup with-test-server]]
            [datomic.api :as d])
  (:import (com.twosigma.cook.jobclient FetchableURI FetchableURI$Builder Group Group$Builder Group$Status GroupListener HostPlacement
                                        HostPlacement$Builder HostPlacement$Type Job Job$Builder Job$Status JobClient JobClient$Builder
                                        JobListener StragglerHandling StragglerHandling$Builder StragglerHandling$Type)
           (java.util ArrayList UUID)))

(use-fixtures :once cook.test.postgres/with-pg-db)

(def port 3001)

(defn make-job
  [& {:keys [uuid name command mem cpus group retries]
      :or {uuid (UUID/randomUUID)
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
        (= type "ATTRIBUTE-EQUALS") (.setParameter "attribute" attribute))
      (.build)))

(defn make-straggler-handling
  [& {:keys [type quantile multiplier]
      :or {type "NONE"
           quantile 0.5
           multiplier 2.0}}]
  (-> (StragglerHandling$Builder.)
      (.setType (StragglerHandling$Type/fromString type))
      (cond->
        (= type "QUANTILE-DEVIATION") ((fn [b]
                                         (.setParameter b "quantile" quantile)
                                         (.setParameter b "multiplier" multiplier))))
      (.build)))

(defn make-group
  [& {:keys [uuid name host-placement straggler-handling]
      :or {uuid (UUID/randomUUID)
           name "cookgroup"
           host-placement (make-host-placement)
           straggler-handling (make-straggler-handling)}}]
  (-> (Group$Builder.)
      (.setUUID uuid)
      (.setName name)
      (.setHostPlacement host-placement)
      (.setStragglerHandling straggler-handling)
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
  (setup)
  ; Start a mock server for testing
  (let [conn (restore-fresh-database! "datomic:mem://jobclient")
        db (d/db conn)
        jc (-> (JobClient$Builder.)
               (.setHost "localhost")
               (.setPort port)
               (.setJobEndpoint "jobs")
               (.setGroupEndpoint "group")
               (.setStatusUpdateInterval 1)
               (.build))]
    (with-test-server [conn port]
                      (testing "Submit loose job"
                        (let [juuid (UUID/randomUUID)
                              command "job-command"
                              job (make-job :uuid juuid :command command)]
                          (-> jc (.submit (ArrayList. [job])))
                          (is (= command (-> jc (.query [juuid]) (.get juuid) (.getCommand))))))

                      (testing "Test job with listener"
                        (let [juuid (UUID/randomUUID)
                              command "job-command"
                              job (make-job :uuid juuid :command command)
                              jchan (async/chan 1)
                              jlistener (make-job-listener #(async/>!! jchan %))]
                          (-> jc (.submit (ArrayList. [job]) jlistener))
                          (change-job-state conn juuid :job.state/completed)
                          (is (= (-> (async/<!! jchan) (.getStatus)) (Job$Status/fromString "COMPLETED")))))

                      (testing "1 group, 1 job"
                        (let [guuid (UUID/randomUUID)
                              juuid (UUID/randomUUID)
                              gname "group-name"
                              group (make-group :uuid guuid :name gname)
                              job (make-job :uuid juuid :group group)]
                          (-> jc (.submitWithGroups (ArrayList. [job]) (ArrayList. [group])))
                          (is (= gname (-> jc (.queryGroups [guuid]) (.get guuid) (.getName))))))

                      (testing "1 group, 1 job, group settings"
                        (let [guuid (UUID/randomUUID)
                              juuid (UUID/randomUUID)
                              gname "group-name"
                              sh (make-straggler-handling :type "QUANTILE-DEVIATION" :quantile 0.25 :multiplier 2.1)
                              hp (make-host-placement :type "ATTRIBUTE-EQUALS" :attribute "region")
                              group (make-group :uuid guuid :name gname :straggler-handling sh :host-placement hp)
                              job (make-job :uuid juuid :group group)]
                          (-> jc (.submitWithGroups (ArrayList. [job]) (ArrayList. [group])))
                          (let [group (-> jc (.queryGroups [guuid]) (.get guuid))]
                            (is (= gname (.getName group)))
                            (is (= (HostPlacement$Type/ATTRIBUTE_EQUALS) (-> group .getHostPlacement .getType)))
                            (is (= {"attribute" "region"} (-> group .getHostPlacement .getParameters)))
                            (is (= (StragglerHandling$Type/QUANTILE_DEVIATION) (-> group .getStragglerHandling .getType)))
                            (is (= {"quantile" 0.25, "multiplier" 2.1} (-> group .getStragglerHandling .getParameters))))))

                      (testing "1 group, multiple jobs"
                        (let [guuid (UUID/randomUUID)
                              juuids (repeatedly 5 #(UUID/randomUUID))
                              gname "group-name"
                              group (make-group :uuid guuid :name gname)
                              jobs (map #(make-job :uuid % :group group) juuids)
                              _ (-> jc (.submitWithGroups (ArrayList. jobs) (ArrayList. [group])))
                              retrieved-group (-> jc (.queryGroups [guuid]) (.get guuid))
                              retrieved-jobs (set (.getJobs retrieved-group))]
                          (is (= gname (.getName retrieved-group)))
                          (is (every? true? (map #(contains? retrieved-jobs %) juuids)))))

                      (testing "Test group listening for WAITING status"
                        (let [guuid (UUID/randomUUID)
                              juuids (repeatedly 5 #(UUID/randomUUID))
                              group (make-group :uuid guuid)
                              jobs (map #(make-job :uuid % :group group :retries 10) juuids)
                              gchan (async/chan 1)
                              glistener (make-group-listener #(async/>!! gchan %))]
                          (-> jc (.submitWithGroups (ArrayList. jobs) (ArrayList. [group]) glistener))
                          (doall (map #(change-job-state conn % :job.state/waiting) juuids))
                          (is (= (-> (async/<!! gchan) (.getStatus)) (Group$Status/fromString "WAITING")))))

                      (testing "Test group listening for COMPLETED status"
                        (let [guuid (UUID/randomUUID)
                              juuids (repeatedly 5 #(UUID/randomUUID))
                              group (make-group :uuid guuid)
                              jobs (map #(make-job :uuid % :group group) juuids)
                              gchan (async/chan 1)
                              glistener (make-group-listener #(async/>!! gchan %))]
                          (-> jc (.submitWithGroups (ArrayList. jobs) (ArrayList. [group]) glistener))
                          (doall (map #(change-job-state conn % :job.state/completed) juuids))

                          (is (= (-> (async/<!! gchan) (.getStatus)) (Group$Status/fromString "COMPLETED")))))

                      (testing "Query group manually"
                        (let [guuid (UUID/randomUUID)
                              gname "this-test-is-sweet"
                              group (make-group :uuid guuid :name gname)]
                          (-> jc (.submitWithGroups (ArrayList. []) (ArrayList. [group])))

                          (is (= (-> jc (.queryGroup guuid) (.getName)) gname))))

                      (testing "Test impersonated job submit and abort"
                        (let [juuid (UUID/randomUUID)
                              command "job-command"
                              user "impersonated-user"
                              job (make-job :uuid juuid :command command)
                              jchan (async/chan 1)
                              jlistener (make-job-listener #(async/>!! jchan %))]
                          (-> jc (.impersonating user) (.submit (ArrayList. [job]) jlistener))
                          (change-job-state conn juuid :job.state/completed)
                          (is (= (-> (async/<!! jchan) (.getUser)) user))
                          (-> jc (.impersonating user) (.abort (ArrayList. [juuid])))))

                      (testing "Test impersonated submission of 1 group, multiple jobs"
                        (let [guuid (UUID/randomUUID)
                              juuids (repeatedly 5 #(UUID/randomUUID))
                              user "impersonated-user"
                              gname "group-name"
                              group (make-group :uuid guuid :name gname)
                              jobs (map #(make-job :uuid % :group group) juuids)
                              _ (-> jc (.impersonating user) (.submitWithGroups (ArrayList. jobs) (ArrayList. [group])))
                              retrieved-jobs (.queryJobs jc juuids)]
                          (is (= (count juuids) (count retrieved-jobs)))
                          (doseq [job (.values retrieved-jobs)]
                            (is (= (.getUser job) user)))))

                      (testing "mea culpa reason"
                        (let [juuid (UUID/randomUUID)
                              job (make-job :uuid juuid)
                              successful-instance (UUID/randomUUID)
                              mea-culpa-instance (UUID/randomUUID)
                              not-mea-culpa-instance (UUID/randomUUID)]
                          (.submit jc (ArrayList. [job]))
                          (let [instance (create-dummy-instance conn [:job/uuid juuid]
                                                                :instance-status :instance.status/unknown
                                                                :task-id (str mea-culpa-instance))]
                            @(d/transact conn [[:instance/update-state instance :instance.status/failed [:reason/name :mesos-slave-removed]]
                                               [:db/add instance :instance/reason [:reason/name :mesos-slave-removed]]]))
                          (let [instance (create-dummy-instance conn [:job/uuid juuid]
                                                                :instance-status :instance.status/unknown
                                                                :task-id (str not-mea-culpa-instance))]
                            @(d/transact conn [[:instance/update-state instance :instance.status/failed [:reason/name :mesos-command-executor-failed]]
                                               [:db/add instance :instance/reason [:reason/name :mesos-command-executor-failed]]]))
                          (let [instance (create-dummy-instance conn [:job/uuid juuid]
                                                                :instance-status :instance.status/running
                                                                :task-id (str successful-instance))]
                            @(d/transact conn [[:instance/update-state instance :instance.status/success [:reason/name :unknown]]]))
                          (let [job (-> jc (.query (ArrayList. [juuid])) (.get juuid))
                                instances (.getInstances job)
                                get-mea-culpa-for-id (fn [uuid] (.getReasonMeaCulpa (first (filter #(= uuid (.getTaskID %)) instances))))]
                            (is (= 3 (count instances)))
                            (is (get-mea-culpa-for-id mea-culpa-instance))
                            (is (= false (get-mea-culpa-for-id not-mea-culpa-instance)))
                            (is (nil? (get-mea-culpa-for-id successful-instance)))))))))
