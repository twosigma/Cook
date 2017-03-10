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
(ns cook.test.mesos.api
 (:use clojure.test)
 (:require [cook.mesos.api :as api :refer (main-handler)]
           [cook.mesos.util :as util]
           [cook.test.testutil :refer (restore-fresh-database! create-dummy-job create-dummy-instance)]
           [clojure.walk :refer (keywordize-keys)]
           [cook.authorization :as auth]
           [schema.core :as s]
           [clojure.data.json :as json]
           [datomic.api :as d :refer (q db)]))

(defn kw-keys
  [m]
  (reduce-kv (fn [m k v]
               (assoc m (keyword k) v))
             {}
             m))

(defn compare-uris
  "Takes 2 lists of URIs and makes sure they're the same.

   The first list is the correct list, which may be missing keys.
   The second list can be fully filled in by Cook with defaults."
  [gold-standard new-data]
  ; URI is order independent
  (let [gold-standard (sort-by :value (keywordize-keys gold-standard))
        new-data (sort-by :value (keywordize-keys new-data))
        resp-uris (map (fn [gold copy]
                         (select-keys copy (keys gold)))
                       gold-standard
                       new-data)]
    (= gold-standard resp-uris)))

(def authorized-fn auth/open-auth)

(defn basic-job
  []
  {"uuid" (str (java.util.UUID/randomUUID))
   "command" "hello world"
   "name" "my-cool-job"
   "priority" 66
   "max_retries" 100
   "cpus" 2.0
   "mem" 2048.0})

(defn basic-handler
  [conn & {:keys [cpus memory-gb gpus-enabled retry-limit] :or {cpus 12 memory-gb 100 gpus-enabled false retry-limit 200}}]
  (main-handler conn "my-framework-id" {:cpus cpus :memory-gb memory-gb :retry-limit retry-limit} gpus-enabled
                (fn [] []) authorized-fn))

(deftest handler-db-roundtrip
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        uuid #uuid "386b374c-4c4a-444f-aca0-0c25384c6fa0"
        env {"MY_VAR" "one"
             "MY_OTHER_VAR" "two"}
        labels {"key1" "value1"
                "key2" "value2"}
        uris [{"value" "http://cool.com/data.txt"
               "extract" false}
              {"value" "ftp://bar.com/data.zip"
               "extract" true
               "cache" true
               "executable" false}]
        job (merge (basic-job)
                   {"uuid" (str uuid)
                    "ports" 2
                    "uris" uris
                    "env" env
                    "labels" labels})
        h (basic-handler conn)]
    (is (<= 200
            (:status (h {:request-method :post
                         :scheme :http
                         :uri "/rawscheduler"
                         :headers {"Content-Type" "application/json"}
                         :authorization/user "dgrnbrg"
                         :body-params {"jobs" [job]}}))
            299))
    (let [ent (d/entity (db conn) [:job/uuid uuid])
          resources (util/job-ent->resources ent)]
      (is (= "hello world" (:job/command ent)))
      (is (= (util/job-ent->env ent) env))
      (is (= (util/job-ent->label ent) labels))
      (is (= (:cpus resources) 2.0))
      (is (= (:mem resources) 2048.0))
      (is (compare-uris (map kw-keys uris) (:uris resources))))
    (let [resp (h {:request-method :get
                   :scheme :http
                   :uri "/rawscheduler"
                   :authorization/user "dgrnbrg"
                   :query-params {"job" (str uuid)}})
          _ (is (<= 200 (:status resp) 299))
          [body] (-> resp :body slurp json/read-str)
          trimmed-body (select-keys body (keys job))
          resp-uris (map (fn [gold copy]
                           (select-keys copy (keys gold)))
                         uris
                         (get trimmed-body "uris"))]
      (is (zero? (get body "gpus")))
      (is (= (dissoc job "uris") (dissoc trimmed-body "uris")))
      (is (compare-uris uris (get trimmed-body "uris"))))
    (is (<= 400
            (:status (h {:request-method :delete
                         :scheme :http
                         :uri "/rawscheduler"
                         :authorization/user "dgrnbrg"
                         :query-params {"job" (str (java.util.UUID/randomUUID))}}))
            499))
    (is (<= 200
            (:status (h {:request-method :delete
                         :scheme :http
                         :uri "/rawscheduler"
                         :authorization/user "dgrnbrg"
                         :query-params {"job" (str uuid)}}))
            299))))

(deftest descriptive-state
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        job-waiting (create-dummy-job conn :user "mforsyth"
                                      :job-state :job.state/waiting)
        job-running (create-dummy-job conn :user "mforsyth"
                                      :job-state :job.state/running)
        job-success (create-dummy-job conn :user "mforsyth"
                                      :job-state :job.state/completed)
        instance-success (create-dummy-instance conn job-success
                                                :instance-status :instance.status/success)
        job-fail (create-dummy-job conn :user "mforsyth"
                                   :job-state :job.state/completed)
        instance-fail (create-dummy-instance conn job-success
                                             :instance-status :instance.status/success)
        db (d/db conn)
        waiting-uuid (:job/uuid (d/entity db job-waiting))
        running-uuid (:job/uuid (d/entity db job-running))
        success-uuid (:job/uuid (d/entity db job-success))
        fail-uuid (:job/uuid (d/entity db job-fail))
        h (basic-handler conn)
        req-attrs {:scheme :http
                   :uri "/rawscheduler"
                   :authorization/user "mforsyth"
                   :request-method :get}
        waiting-resp (h (merge req-attrs {:query-params {"job" (str waiting-uuid)}}))
        waiting-body (-> waiting-resp :body slurp json/read-str)
        running-resp (h (merge req-attrs {:query-params {"job" (str running-uuid)}}))
        running-body (-> running-resp :body slurp json/read-str)
        success-resp (h (merge req-attrs {:query-params {"job" (str success-uuid)}}))
        success-body (-> success-resp :body slurp json/read-str)
        fail-resp (h (merge req-attrs {:query-params {"job" (str fail-uuid)}}))
        fail-body (-> fail-resp :body slurp json/read-str)]
    (is (= ((first waiting-body) "state") "waiting"))
    (is (= ((first running-body) "state") "running"))
    (is (= ((first success-body) "state") "success"))
    (is (= ((first fail-body) "state") "failed"))))

(deftest job-validator
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        job (fn [cpus mem max-retries] {"uuid" (str (java.util.UUID/randomUUID))
                            "command" "hello world"
                            "name" "my-cool-job"
                            "priority" 66
                            "max_retries" max-retries
                            "max_runtime" 1000000
                            "cpus" cpus
                            "mem" mem})
        h (basic-handler conn :gpus-enabled true :cpus 3 :memory-gb 2 :retry-limit 200)]
    (testing "All within limits"
      (is (<= 200
              (:status (h {:request-method :post
                           :scheme :http
                           :uri "/rawscheduler"
                           :headers {"Content-Type" "application/json"}
                           :authorization/user "dgrnbrg"
                           :body-params {"jobs" [(job 1 500 100)]}}))
              299)))
    (testing "All at limits"
      (is (<= 200
              (:status (h {:request-method :post
                           :scheme :http
                           :uri "/rawscheduler"
                           :headers {"Content-Type" "application/json"}
                           :authorization/user "dgrnbrg"
                           :body-params {"jobs" [(job 3 2048 200)]}}))
              299)))
    (testing "Beyond limits cpus"
      (is (<= 400
              (:status (h {:request-method :post
                           :scheme :http
                           :uri "/rawscheduler"
                           :headers {"Content-Type" "application/json"}
                           :authorization/user "dgrnbrg"
                           :body-params {"jobs" [(job 3.1 100 100)]}}))
              499)))
    (testing "Beyond limits mem"
      (is (<= 400
              (:status (h {:request-method :post
                           :scheme :http
                           :uri "/rawscheduler"
                           :headers {"Content-Type" "application/json"}
                           :authorization/user "dgrnbrg"
                           :body-params {"jobs" [(job 1 2050 100)]}}))
              499)))
    (testing "Beyond limits retries"
      (is (<= 400
              (:status (h {:request-method :post
                           :scheme :http
                           :uri "/rawscheduler"
                           :headers {"Content-Type" "application/json"}
                           :authorization/user "dgrnbrg"
                           :body-params {"jobs" [(job 1 100 202)]}}))
              499)))))

(deftest gpus-api
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        job (fn [gpus ] (merge (basic-job) {"gpus" gpus}))
        h (basic-handler conn :gpus-enabled true)]
    (testing "negative gpus invalid"
      (is (<= 400
              (:status (h {:request-method :post
                           :scheme :http
                           :uri "/rawscheduler"
                           :headers {"Content-Type" "application/json"}
                           :authorization/user "dgrnbrg"
                           :body-params {"jobs" [(job -3)]}}))
              499)))
    (testing "Zero gpus invalid"
      (is (<= 400
              (:status (h {:request-method :post
                           :scheme :http
                           :uri "/rawscheduler"
                           :headers {"Content-Type" "application/json"}
                           :authorization/user "dgrnbrg"
                           :body-params {"jobs" [(job 0)]}}))
              499)))
    (let [successful-job (job 2)]
      (testing "Positive GPUs ok"
        (is (<= 200
                (:status (h {:request-method :post
                             :scheme :http
                             :uri "/rawscheduler"
                             :headers {"Content-Type" "application/json"}
                             :authorization/user "dgrnbrg"
                             :body-params {"jobs" [successful-job]}}))
                299)))
      (let [resp (h {:request-method :get
                     :scheme :http
                     :uri "/rawscheduler"
                     :authorization/user "dgrnbrg"
                     :query-params {"job" (str (get successful-job "uuid"))}})
            _ (is (<= 200 (:status resp) 299))
            [body] (-> resp :body slurp json/read-str)
            trimmed-body (select-keys body (keys successful-job))]
        (is (= (dissoc successful-job "uris") (dissoc trimmed-body "uris")))))))

(deftest retries-api
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        h (basic-handler conn)
        uuid1 (str (java.util.UUID/randomUUID))
        uuid2 (str (java.util.UUID/randomUUID))
        create-response (h {:request-method :post
                            :scheme :http
                            :uri "/rawscheduler"
                            :headers {"Content-Type" "application/json"}
                            :authorization/user "mforsyth"
                            :body-params
                            {"jobs" [(merge (basic-job) {"uuid" uuid1
                                                         "max_retries" 42})
                                     (merge (basic-job) {"uuid" uuid2
                                                         "max_retries" 30})]}})
        retry-req-attrs {:scheme :http
                         :uri "/retry"
                         :authorization/user "mforsyth"}]

    (testing "retry a single job with static retries"
      (let [update-resp (h (merge retry-req-attrs
                                  {:request-method :put
                                   :query-params {"job" uuid1}
                                   :body-params {"retries" 45}}))
            _ (is (<= 200 (:status update-resp) 299))
            read-resp (h (merge retry-req-attrs  {:request-method :get
                                                  :query-params {"job" uuid1}}))
            read-body (-> read-resp :body slurp json/read-str)]
        (is (= read-body 45))))

    (testing "retry multiple jobs incrementing retries"
      (let [update-resp (h (merge retry-req-attrs
                                  {:request-method :put
                                   :query-params {"job" [uuid1 uuid2]}
                                   :body-params {"increment" 3}}))
            read-resp1 (h (merge retry-req-attrs  {:request-method :get
                                                   :query-params {"job" uuid1}}))
            read-body1 (-> read-resp1 :body slurp json/read-str)
            read-resp2 (h (merge retry-req-attrs  {:request-method :get
                                                   :query-params {"job" uuid2}}))
            read-body2 (-> read-resp2 :body slurp json/read-str)]
        (is (= read-body1 48))
        (is (= read-body2 33))))))

(deftest instance-cancelling
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        job (create-dummy-job conn :user "mforsyth")
        instance (create-dummy-instance conn job
                                        :instance-status :instance.status/running)
        task-id (-> conn d/db (d/entity instance) :instance/task-id)
        h (basic-handler conn)
        req-attrs {:scheme :http
                   :uri "/rawscheduler"
                   :authorization/user "mforsyth"
                   :query-params {"instance" task-id}}
        initial-read-resp (h (merge req-attrs {:request-method :get}))
        initial-read-body (-> initial-read-resp :body slurp json/read-str)
        initial-instance-cancelled? (-> initial-read-body keywordize-keys
                                        first :instances first :cancelled boolean)
        cancel-resp (h (merge req-attrs {:request-method :delete}))
        followup-read-resp (h (merge req-attrs {:request-method :get}))
        followup-read-body (-> followup-read-resp :body slurp json/read-str)
        followup-instance-cancelled? (-> followup-read-body keywordize-keys
                                         first :instances first :cancelled boolean)]
    (is (not initial-instance-cancelled?))
    (is (<= 200 (:status cancel-resp) 299))
    (is followup-instance-cancelled?)))


(deftest quota-api
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        h (basic-handler conn)
        quota-req-attrs {:scheme :http
                         :uri "/quota"
                         :authorization/user "mforsyth"
                         :headers {"Content-Type" "application/json"}}
        initial-get-resp (h (merge quota-req-attrs
                                   {:request-method :get
                                    :query-params {:user "foo"}}))
        initial-get-body (-> initial-get-resp :body slurp json/read-str)]

    (testing "initial get successful"
      (is (<= 200 (:status initial-get-resp) 299)))

    (testing "update changes quota"
      (let [new-quota {:cpus 9.0 :mem 4323.0 :count 43 :gpus 3.0}
            update-resp (h (merge quota-req-attrs
                                  {:request-method :post
                                   :body-params {:user "foo" :quota new-quota}}))
            update-body (-> update-resp :body slurp json/read-str)
            _ (is (<= 200 (:status update-resp) 299))
            _ (is (= (kw-keys update-body) new-quota))
            get-resp (h (merge quota-req-attrs
                               {:request-method :get
                                :query-params {:user "foo"}}))
            get-body (-> get-resp :body slurp json/read-str)]
        (is (= get-body update-body))))

    (testing "delete resets quota"
      (let [delete-resp (h (merge quota-req-attrs
                                  {:request-method :delete
                                   :query-params {:user "foo"}}))
            _ (is (<= 200 (:status delete-resp) 299))
            get-resp (h (merge quota-req-attrs
                               {:request-method :get
                                :query-params {:user "foo"}}))
            get-body (-> get-resp :body slurp json/read-str)]
        (is (= get-body initial-get-body))))))

(deftest share-api
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        h (basic-handler conn)
        share-req-attrs {:scheme :http
                         :uri "/share"
                         :authorization/user "mforsyth"
                         :headers {"Content-Type" "application/json"}}
        initial-get-resp (h (merge share-req-attrs
                                   {:request-method :get
                                    :query-params {:user "foo"}}))
        initial-get-body (-> initial-get-resp :body slurp json/read-str)]

    (testing "initial get successful"
      (is (<= 200 (:status initial-get-resp) 299)))

    (testing "update changes share"
      (let [new-share {:cpus 9.0 :mem 4323.0 :gpus 3.0}
            update-resp (h (merge share-req-attrs
                                  {:request-method :post
                                   :body-params {:user "foo" :share new-share}}))
            update-body (-> update-resp :body slurp json/read-str)
            _ (is (<= 200 (:status update-resp) 299))
            _ (is (= (kw-keys update-body) new-share))
            get-resp (h (merge share-req-attrs
                               {:request-method :get
                                :query-params {:user "foo"}}))
            get-body (-> get-resp :body slurp json/read-str)]
        (is (= get-body update-body))))

    (testing "delete resets share"
      (let [delete-resp (h (merge share-req-attrs
                                  {:request-method :delete
                                   :query-params {:user "foo"}}))
            _ (is (<= 200 (:status delete-resp) 299))
            get-resp (h (merge share-req-attrs
                               {:request-method :get
                                :query-params {:user "foo"}}))
            get-body (-> get-resp :body slurp json/read-str)]
        (is (= get-body initial-get-body))))))

(deftest group-validator
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        make-job (fn [& {:keys [uuid name group]
                         :or {uuid (str (java.util.UUID/randomUUID))
                              name "cook-job"
                              group nil}}]
                   (merge {"uuid" uuid
                           "command" "hello world"
                           "name" name
                           "priority" 66
                           "max_retries" 3
                           "max_runtime" 1000000
                           "cpus" 1
                           "mem" 10}
                          (when group
                            {"group" group})))
        make-group (fn [& {:keys [uuid placement-type attribute straggler-handling-type quantile multiplier]
                           :or {uuid (str (java.util.UUID/randomUUID))
                                placement-type "all"
                                straggler-handling-type "none"
                                quantile 0.5
                                multiplier 2.0
                                attribute "test"}}]
                     {"uuid" uuid
                      "host_placement" (merge {"type" placement-type}
                                              (when (= placement-type "attribute-equals")
                                                {"parameters" {"attribute" attribute}}))
                      "straggler_handling" (merge {"type" straggler-handling-type }
                                                  (when (= straggler-handling-type "quantile-deviation")
                                                    {"parameters" {"quantile" quantile "multiplier" multiplier}}))})

        h (basic-handler conn)
        post (fn [params]
               (h {:request-method :post
                   :scheme :http
                   :uri "/rawscheduler"
                   :headers {"Content-Type" "application/json"}
                   :authorization/user "diego"
                   :body-params params}))
        get-group (fn [uuid]
                    (-> (h {:request-method :get
                            :scheme :http
                            :uri "/group"
                            :headers {"Content-Type" "application/json"}
                            :authorization/usr "diego"
                            :query-params {"uuid" uuid}})
                        :body
                        slurp
                        json/read-str))
        get-job (fn [uuid]
                  (-> (h {:request-method :get
                          :scheme :http
                          :uri "/rawscheduler"
                          :headers {"Content-Type" "application/json"}
                          :authorization/usr "diego"
                          :query-params {"job" uuid}})
                      :body
                      slurp
                      json/read-str))]
    (testing "One job one group"
      (let [guuid (str (java.util.UUID/randomUUID))
            juuid (str (java.util.UUID/randomUUID))
            post-resp (post {"groups" [(make-group :uuid guuid)]
                             "jobs" [(make-job :uuid juuid :group guuid)]})
            group-resp (get-group guuid)
            job-resp (get-job juuid)]

        (is (<= 200 (:status post-resp) 299)
            ; for debugging
            (try (slurp (:body post-resp)) (catch Exception e (:body post-resp))))
        (is (= guuid (-> group-resp first (get "uuid"))) group-resp)
        (is (= juuid (-> group-resp first (get "jobs") first)))

        (is (= juuid (-> job-resp first (get "uuid"))))
        (is (= guuid (-> job-resp first (get "groups") first)))))

    (testing "Two jobs one group"
      (let [guuid (str (java.util.UUID/randomUUID))
            juuid1 (str (java.util.UUID/randomUUID))
            juuid2 (str (java.util.UUID/randomUUID))
            post-resp (post {"groups" [(make-group :uuid guuid :placement-type "unique")]
                             "jobs" [(make-job :uuid juuid1 :group guuid)
                                     (make-job :uuid juuid2 :group guuid)]})
            group-resp (get-group guuid)
            job-resp1 (get-job juuid1)
            job-resp2 (get-job juuid2)]

        (is (<= 200 (:status post-resp) 299))
        (is (= guuid (-> group-resp first (get "uuid"))))
        (is (contains? (-> group-resp first (get "jobs") set) juuid1))
        (is (contains? (-> group-resp first (get "jobs") set) juuid2))

        (is (= juuid1 (-> job-resp1 first (get "uuid"))))
        (is (= guuid (-> job-resp1 first (get "groups") first)))
        (is (= guuid (-> job-resp2 first (get "groups") first)))))

    (testing "Implicitly created groups"
      (let [guuid1 (str (java.util.UUID/randomUUID))
            guuid2 (str (java.util.UUID/randomUUID))
            juuid1 (str (java.util.UUID/randomUUID))
            juuid2 (str (java.util.UUID/randomUUID))
            post-resp (post {"jobs" [(make-job :uuid juuid1 :group guuid1)
                                     (make-job :uuid juuid2 :group guuid2)]})
            group-resp1 (get-group guuid1)
            group-resp2 (get-group guuid2)
            job-resp1 (get-job juuid1)
            job-resp2 (get-job juuid2)]

        (is (<= 200 (:status post-resp) 299))
        (is (= guuid1 (-> group-resp1 first (get "uuid"))))
        (is (= guuid2 (-> group-resp2 first (get "uuid"))))

        (is (contains? (-> group-resp1 first (get "jobs") set) juuid1))
        (is (contains? (-> group-resp2 first (get "jobs") set) juuid2))

        (is (= (-> group-resp1 first) {"uuid" guuid1 "name" "defaultgroup" "host_placement" {"type" "all"} "jobs" [juuid1] "straggler_handling" {"type" "none"}}))
        (is (= (-> group-resp2 first) {"uuid" guuid2 "name" "defaultgroup" "host_placement" {"type" "all"} "jobs" [juuid2] "straggler_handling"  {"type" "none"}}))
        ))

    (testing "Multiple groups in one request"
      (let [guuid1 (str (java.util.UUID/randomUUID))
            guuid2 (str (java.util.UUID/randomUUID))
            juuids (vec (repeatedly 5 #(str (java.util.UUID/randomUUID))))
            post (fn [params]
               (h {:request-method :post
                   :scheme :http
                   :uri "/rawscheduler"
                   :headers {"Content-Type" "application/json"}
                   :authorization/user "dgrnbrg"
                   :body-params params}))
            post-resp (post {"groups" [(make-group :uuid guuid1 :placement-type "unique")
                                       (make-group :uuid guuid2 :placement-type "all")]
                             "jobs" [(make-job :uuid (get juuids 0) :group guuid1)
                                     (make-job :uuid (get juuids 1) :group guuid1)
                                     (make-job :uuid (get juuids 2) :group guuid2)
                                     (make-job :uuid (get juuids 3) :group guuid2)
                                     (make-job :uuid (get juuids 4) :group guuid2)]})
            group-resp1 (get-group guuid1)
            group-resp2 (get-group guuid2)]
        (is (<= 200 (:status post-resp) 299))

        (is (contains? (-> group-resp1 first (get "jobs") set) (get juuids 0)))
        (is (contains? (-> group-resp1 first (get "jobs") set) (get juuids 1)))

        (is (contains? (-> group-resp2 first (get "jobs") set) (get juuids 2)))
        (is (contains? (-> group-resp2 first (get "jobs") set) (get juuids 3)))
        (is (contains? (-> group-resp2 first (get "jobs") set) (get juuids 4)))))

    (testing "Group with defaulted host placement"
      (let [guuid (str (java.util.UUID/randomUUID))
            post-resp (post {"groups" [{"uuid" guuid}]
                             "jobs" [(make-job :group guuid) (make-job :group guuid)]})
            group-resp (get-group guuid)]
        (is (<= 201 (:status post-resp) 299))
        (is (= "all" (-> group-resp first (get "host_placement") (get "type"))))))

    (testing "Use attribute equals parameter"
      (let [guuid (str (java.util.UUID/randomUUID))
            post-resp (post {"groups" [(make-group :uuid guuid :placement-type "attribute-equals"
                                                   :attribute "test")]
                             "jobs" [(make-job :group guuid) (make-job :group guuid)]})
            group-resp (get-group guuid)]
        (is (<= 201 (:status post-resp) 299)
            ; for debugging
            (try (slurp (:body post-resp)) (catch Exception e (:body post-resp))))
        (is (= "test" (-> group-resp first (get "host_placement") (get "parameters")
                        (get "attribute"))))))

    (testing "Invalid placement type"
      (let [guuid (str (java.util.UUID/randomUUID))
            post-resp (post {"groups" [(make-group :uuid guuid :placement-type "not-a-type")]
                             "jobs" [(make-job :group guuid) (make-job :group guuid)]})]
        (is (<= 400 (:status post-resp) 499))))

    (testing "Missing host placement attribute equals parameter"
      (let [guuid (str (java.util.UUID/randomUUID))
            post-resp (post {"groups" [{"uuid" guuid
                                        "host_placement" {"type" "attribute-equals"}}]
                             "jobs" [(make-job :group guuid) (make-job :group guuid)]})]
        (is (<= 400 (:status post-resp) 499))))

    (testing "Straggler handling quantile deviation"
      (let [guuid1 (str (java.util.UUID/randomUUID))
            juuid1 (str (java.util.UUID/randomUUID))
            juuid2 (str (java.util.UUID/randomUUID))
            post-resp (post {"jobs" [(make-job :uuid juuid1 :group guuid1)
                                     (make-job :uuid juuid2 :group guuid1)]
                             "groups" [(make-group :uuid guuid1 :straggler-handling-type "quantile-deviation"
                                                   :quantile 0.5 :multiplier 2.0)]})
            group-resp1 (get-group guuid1)
            job-resp1 (get-job juuid1)
            job-resp2 (get-job juuid2)]

        (is (<= 200 (:status post-resp) 299)
            ; for debugging
            (try (slurp (:body post-resp)) (catch Exception e (:body post-resp))))
        (is (= guuid1 (-> group-resp1 first (get "uuid"))))

        (is (contains? (-> group-resp1 first (get "jobs") set) juuid1))
        (is (contains? (-> group-resp1 first (get "jobs") set) juuid2))

        (is (= (-> group-resp1 first (update "jobs" set)) {"uuid" guuid1 "name" "cookgroup" "host_placement" {"type" "all"} "jobs" #{juuid1 juuid2} "straggler_handling" {"type" "quantile-deviation" "parameters" {"quantile" 0.5 "multiplier" 2.0}}}))))
    (testing "Error trying to create previously existing group"
      (let [guuid (str (java.util.UUID/randomUUID))
            post-resp1 (post {"jobs" [(make-job :group guuid) (make-job :group guuid)]})
            post-resp2 (post {"jobs" [(make-job :group guuid)]})]
        (is (<= 201 (:status post-resp1) 299))
        (is (<= 400 (:status post-resp2) 499))))
    ))

(deftest retry-validator
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        uuid (java.util.UUID/randomUUID)
        job (merge (basic-job) {"uuid" (str uuid)})
        h (basic-handler conn :retry-limit 200)]
        (testing "Initial job creation"
          (is (= 201
                 (:status (h {:request-method :post
                  :scheme :http
                  :uri "/rawscheduler"
                  :headers {"Content-Type" "application/json"}
                  :authorization/user "dgrnbrg"
                  :body-params {"jobs" [job]}})))))

        (testing "Within limits"
          (is (<= 200
                  (:status (h {:request-method :post
                               :scheme :http
                               :uri "/retry"
                               :headers {"Content-Type" "application/json"}
                               :authorization/user "dgrnbrg"
                               :query-params {:job uuid}
                               :body-params {"retries" 198}}))
                  299)))
        (testing "Incrementing within limit"
          (is (<= 200
                  (:status (h {:request-method :post
                               :scheme :http
                               :uri "/retry"
                               :headers {"Content-Type" "application/json"}
                               :authorization/user "dgrnbrg"
                               :body-params {"jobs" [uuid]
                                             "increment" 1}}))
                  299)))
        (testing "At limits"
            (is (<= 200
                    (:status (h {:request-method :post
                                 :scheme :http
                                 :uri "/retry"
                                 :headers {"Content-Type" "application/json"}
                                 :authorization/user "dgrnbrg"
                                 :body-params {"jobs" [uuid]
                                               "retries" 200}}))
                    299)))
        (testing "Over limit"
          (is (<= 400
                  (:status (h {:request-method :post
                               :scheme :http
                               :uri "/retry"
                               :headers {"Content-Type" "application/json"}
                               :authorization/user "dgrnbrg"
                               :body-params {"job" [uuid]
                                        "retries" 201}}))
                  499)))

        (testing "Incrementing over limit"
            (is (<= 400
                    (:status (h {:request-method :post
                                 :scheme :http
                                 :uri "/retry"
                                 :headers {"Content-Type" "application/json"}
                                 :authorization/user "dgrnbrg"
                                 :body-params {"job" [uuid]
                                               "increment" 5}}))
                    499)))))

(deftest list-validator
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        uuid #uuid "386b374c-4c4a-444f-aca0-0c25384c6fa0"
        env {"MY_VAR" "one"
             "MY_OTHER_VAR" "two"}
        labels {"key1" "value1"
                "key2" "value2"}
        uris [{"value" "http://cool.com/data.txt"
               "extract" false}
              {"value" "ftp://bar.com/data.zip"
               "extract" true
               "cache" true
               "executable" false}]
        job {"uuid" (str uuid)
             "command" "hello world"
             "name" "my-cool-job"
             "priority" 66
             "max_retries" 100
             "max_runtime" 1000000
             "ports" 2
             "uris" uris
             "env" env
             "labels" labels
             "cpus" 2.0
             "mem" 2048.0}
        h (basic-handler conn :cpus 12 :memory-gb 100 :retry-limit 500)]
    (is (= "[]"
          (:body (h {:request-method :get
            :scheme :http
            :uri "/list"
            :authorization/user "wyegelwe"
            :body-params {"start-ms" (str (System/currentTimeMillis))
                          "end-ms" (str (+ (System/currentTimeMillis) 10))
                          "state" "running"
                          "user" "wyegelwe"}}))))

    ; Fail because missing user
    (is (<= 400
           (:status (h {:request-method :get
                      :scheme :http
                      :uri "/list"
                      :authorization/user "wyegelwe"
                      :body-params {"start-ms" (str (System/currentTimeMillis))
                               "end-ms" (str (+ (System/currentTimeMillis) 10))
                               "state" "running"
                               }}))
            499))

    ; Fail because missing state
    (is (<= 400
            (:status (h {:request-method :get
                       :scheme :http
                       :uri "/list"
                       :authorization/user "wyegelwe"
                       :body-params {"start-ms" (str (System/currentTimeMillis))
                                "end-ms" (str (+ (System/currentTimeMillis) 10))
                                "user" "wyegelwe"}}))
            499))

    ; Fail because start is after end
    (is (<= 400
            (:status (h {:request-method :get
                       :scheme :http
                       :uri "/list"
                       :authorization/user "wyegelwe"
                       :body-params {"start-ms" (str (+ (System/currentTimeMillis) 1000))
                                "end-ms" (str (+ (System/currentTimeMillis) 10))
                                "state" "running"
                                "user" "wyegelwe"
                                }}))
            499))

    (is (<= 200
        (:status (h {:request-method :post
                     :scheme :http
                     :uri "/rawscheduler"
                     :headers {"Content-Type" "application/json"}
                     :authorization/user "dgrnbrg"
                     :body-params {"jobs" [job]}}))
        299))

    (let [list-str (:body (h {:request-method :get
                              :scheme :http
                              :uri "/list"
                              :authorization/user "wyegelwe"
                              :body-params {"start-ms" (str (- (System/currentTimeMillis) 1000))
                                       "end-ms" (str (+ (System/currentTimeMillis) 1000))
                                       "state" "running+waiting+completed"
                                       "user" "dgrnbrg"
                                       }
                              }))
          jobs (json/read-str list-str)]
      (is (= 1 (count jobs)))
      (is (= (str uuid) (-> jobs first (get "uuid")))))

    (let [list-str (:body (h {:request-method :get
                              :scheme :http
                              :uri "/list"
                              :authorization/user "wyegelwe"
                              :body-params {"start-ms" (str (+ (System/currentTimeMillis) 1000))
                                       "end-ms" (str (+ (System/currentTimeMillis) 1000))
                                       "state" "running+waiting+completed"
                                       "user" "dgrnbrg"}}))
          jobs (json/read-str list-str)]
      (is (= 0 (count jobs))))))

(deftest test-make-type-parameter-txn
  (testing "Example test"
    (is (= (api/make-type-parameter-txn {:type :quantile-deviation
                                         :parameters {:quantile 0.5 :multiplier 2.5}}
                                        :straggler-handling)
           {:straggler-handling/type :straggler-handling.type/quantile-deviation
            :straggler-handling/parameters {:straggler-handling.quantile-deviation/quantile 0.5
                                            :straggler-handling.quantile-deviation/multiplier 2.5}}))))
