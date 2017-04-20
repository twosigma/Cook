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
  (:require [cheshire.core :as cheshire]
            [clj-time.core :as t]
            [clojure.data.json :as json]
            [clojure.walk :refer (keywordize-keys)]
            [cook.authorization :as auth]
            [cook.components :as components]
            [cook.mesos.api :as api :refer (main-handler)]
            [cook.mesos.reason :as reason]
            [cook.mesos.util :as util]
            [cook.test.testutil :refer (restore-fresh-database! create-dummy-job create-dummy-instance)]
            [datomic.api :as d :refer (q db)]
            [schema.core :as s])
  (:import com.fasterxml.jackson.core.JsonGenerationException
           java.net.ServerSocket
           java.util.UUID
           org.apache.curator.test.TestingServer
           org.joda.time.Minutes))

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
  {"uuid" (str (UUID/randomUUID))
   "command" "hello world"
   "name" "my-cool-job"
   "priority" 66
   "max_retries" 100
   "cpus" 2.0
   "mem" 2048.0})

(defn basic-handler
  [conn & {:keys [cpus memory-gb gpus-enabled retry-limit] :or {cpus 12 memory-gb 100 gpus-enabled false retry-limit 200}}]
  (main-handler conn "my-framework-id" {:cpus cpus :memory-gb memory-gb :retry-limit retry-limit} gpus-enabled
                (fn [] []) authorized-fn nil))

(defn response->body-data [response]
  (-> response :body slurp json/read-str))

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

    (testing "409 (conflict) on duplicate creation attempt"
      (let [response (h {:request-method :post
                         :scheme :http
                         :uri "/rawscheduler"
                         :headers {"Content-Type" "application/json"}
                         :authorization/user "dgrnbrg"
                         :body-params {"jobs" [job]}})]
        (is (= 409 (:status response)))
        (is (= (response->body-data response)
               {"error" (str "The following job UUIDs were already used: " uuid)}))))

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
          [body] (response->body-data resp)
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
                         :query-params {"job" (str (UUID/randomUUID))}}))
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
        waiting-body (response->body-data waiting-resp)
        running-resp (h (merge req-attrs {:query-params {"job" (str running-uuid)}}))
        running-body (response->body-data running-resp)
        success-resp (h (merge req-attrs {:query-params {"job" (str success-uuid)}}))
        success-body (response->body-data success-resp)
        fail-resp (h (merge req-attrs {:query-params {"job" (str fail-uuid)}}))
        fail-body (response->body-data fail-resp)]
    (is (= ((first waiting-body) "state") "waiting"))
    (is (= ((first running-body) "state") "running"))
    (is (= ((first success-body) "state") "success"))
    (is (= ((first fail-body) "state") "failed"))))

(deftest job-validator
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        job (fn [cpus mem max-retries] {"uuid" (str (UUID/randomUUID))
                                        "command" "hello world"
                                        "name" "my-cool-job"
                                        "priority" 66
                                        "max_retries" max-retries
                                        "max_runtime" 1000000
                                        "cpus" cpus
                                        "mem" mem})
        h (basic-handler conn :gpus-enabled true :cpus 3 :memory-gb 2 :retry-limit 200)]
    (testing "default attributes"
      (let [job-with-defaults (dissoc (job 1 500 100) "name" "priority")
            create-response (h {:request-method :post
                                :scheme :http
                                :uri "/rawscheduler"
                                :headers {"Content-Type" "application/json"}
                                :authorization/user "mforsyth"
                                :body-params {"jobs" [job-with-defaults]}})
            get-response (h {:scheme :http
                             :uri "/rawscheduler"
                             :authorization/user "mforsyth"
                             :request-method :get
                             :query-params {"job" (job-with-defaults "uuid")}})
            new-job (-> get-response response->body-data first)]
        (is (= (:status create-response) 201))
        (is (= (:status get-response) 200))
        (is (= (new-job "name") "cookjob"))
        (is (= (new-job "priority") util/default-job-priority))))
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
            [body] (response->body-data resp)
            trimmed-body (select-keys body (keys successful-job))]
        (is (= (dissoc successful-job "uris") (dissoc trimmed-body "uris")))))))

(deftest retries-api
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        h (basic-handler conn)
        uuid1 (str (UUID/randomUUID))
        uuid2 (str (UUID/randomUUID))
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

    (testing "Specifying both \"job\" and \"jobs\" is malformed"
      (let [update-resp (h (merge retry-req-attrs
                                  {:request-method :put
                                   :body-params {:job uuid1
                                                 :jobs [uuid2]
                                                 "retries" 4}}))]
        (is (= (:status update-resp) 400))))

    (testing "Specifying \"job\" in query and body is malformed"
      (let [update-resp (h (merge retry-req-attrs
                                  {:request-method :put
                                   :query-params {:job uuid2}
                                   :body-params {:job uuid1
                                                 "retries" 4}}))]
        (is (= (:status update-resp) 400))))

    (testing "Specifying \"job\" in query and \"jobs\" in body is malformed"
      (let [update-resp (h (merge retry-req-attrs
                                  {:request-method :put
                                   :query-params {:job uuid2}
                                   :body-params {:jobs [uuid1]
                                                 "retries" 4}}))]
        (is (= (:status update-resp) 400))))

    (testing "retry a single job with static retries"
      (let [update-resp (h (merge retry-req-attrs
                                  {:request-method :put
                                   :query-params {"job" uuid1}
                                   :body-params {"retries" 45}}))
            _ (is (<= 200 (:status update-resp) 299))
            read-resp (h (merge retry-req-attrs  {:request-method :get
                                                  :query-params {:job uuid1}}))
            read-body (response->body-data read-resp)]
        (is (= read-body 45))))

    (testing "retry a single job incrementing retries"
      (let [update-resp (h (merge retry-req-attrs
                                  {:request-method :put
                                   :body-params {"job" uuid1
                                                 "increment" 2}}))
            _ (is (<= 200 (:status update-resp) 299))
            read-resp (h (merge retry-req-attrs  {:request-method :get
                                                  :query-params {:job uuid1}}))
            read-body (response->body-data read-resp)]
        (is (= read-body 47))))

    (testing "retry multiple jobs incrementing retries"
      (let [update-resp (h (merge retry-req-attrs
                                  {:request-method :put
                                   :query-params {"job" [uuid1 uuid2]}
                                   :body-params {"increment" 3}}))
            read-resp1 (h (merge retry-req-attrs  {:request-method :get
                                                   :query-params {"job" uuid1}}))
            read-body1 (response->body-data read-resp1)
            read-resp2 (h (merge retry-req-attrs  {:request-method :get
                                                   :query-params {"job" uuid2}}))
            read-body2 (response->body-data read-resp2)]
        (is (= read-body1 50))
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
        initial-read-body (response->body-data initial-read-resp)
        initial-instance-cancelled? (-> initial-read-body keywordize-keys
                                        first :instances first :cancelled boolean)
        cancel-resp (h (merge req-attrs {:request-method :delete}))
        followup-read-resp (h (merge req-attrs {:request-method :get}))
        followup-read-body (response->body-data followup-read-resp)
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
        initial-get-body (response->body-data initial-get-resp)]

    (testing "initial get successful"
      (is (<= 200 (:status initial-get-resp) 299)))

    (testing "update changes quota"
      (let [new-quota {:cpus 9.0 :mem 4323.0 :count 43 :gpus 3.0}
            update-resp (h (merge quota-req-attrs
                                  {:request-method :post
                                   :body-params {:user "foo" :quota new-quota}}))
            update-body (response->body-data update-resp)
            _ (is (<= 200 (:status update-resp) 299))
            _ (is (= (kw-keys update-body) new-quota))
            get-resp (h (merge quota-req-attrs
                               {:request-method :get
                                :query-params {:user "foo"}}))
            get-body (response->body-data get-resp)]
        (is (= get-body update-body))))

    (testing "delete resets quota"
      (let [delete-resp (h (merge quota-req-attrs
                                  {:request-method :delete
                                   :query-params {:user "foo"}}))
            _ (is (<= 200 (:status delete-resp) 299))
            get-resp (h (merge quota-req-attrs
                               {:request-method :get
                                :query-params {:user "foo"}}))
            get-body (response->body-data get-resp)]
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
        initial-get-body (response->body-data initial-get-resp)]

    (testing "initial get successful"
      (is (<= 200 (:status initial-get-resp) 299)))

    (testing "update changes share"
      (let [new-share {:cpus 9.0 :mem 4323.0 :gpus 3.0}
            update-resp (h (merge share-req-attrs
                                  {:request-method :post
                                   :body-params {:user "foo" :share new-share}}))
            update-body (response->body-data update-resp)
            _ (is (<= 200 (:status update-resp) 299))
            _ (is (= (kw-keys update-body) new-share))
            get-resp (h (merge share-req-attrs
                               {:request-method :get
                                :query-params {:user "foo"}}))
            get-body (response->body-data get-resp)]
        (is (= get-body update-body))))

    (testing "delete resets share"
      (let [delete-resp (h (merge share-req-attrs
                                  {:request-method :delete
                                   :query-params {:user "foo"}}))
            _ (is (<= 200 (:status delete-resp) 299))
            get-resp (h (merge share-req-attrs
                               {:request-method :get
                                :query-params {:user "foo"}}))
            get-body (response->body-data get-resp)]
        (is (= get-body initial-get-body))))))

(deftest group-validator
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        make-job (fn [& {:keys [uuid name group]
                         :or {uuid (str (UUID/randomUUID))
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
                           :or {uuid (str (UUID/randomUUID))
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
                    (response->body-data (h {:request-method :get
                                   :scheme :http
                                   :uri "/group"
                                   :headers {"Content-Type" "application/json"}
                                   :authorization/usr "diego"
                                   :query-params {"uuid" uuid}})))
        get-job (fn [uuid]
                  (response->body-data (h {:request-method :get
                                 :scheme :http
                                 :uri "/rawscheduler"
                                 :headers {"Content-Type" "application/json"}
                                 :authorization/usr "diego"
                                 :query-params {"job" uuid}})))]
    (testing "One job one group"
      (let [guuid (str (UUID/randomUUID))
            juuid (str (UUID/randomUUID))
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
      (let [guuid (str (UUID/randomUUID))
            juuid1 (str (UUID/randomUUID))
            juuid2 (str (UUID/randomUUID))
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
      (let [guuid1 (str (UUID/randomUUID))
            guuid2 (str (UUID/randomUUID))
            juuid1 (str (UUID/randomUUID))
            juuid2 (str (UUID/randomUUID))
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
      (let [guuid1 (str (UUID/randomUUID))
            guuid2 (str (UUID/randomUUID))
            juuids (vec (repeatedly 5 #(str (UUID/randomUUID))))
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

    (testing "Overriding group immutability"
      (let [guuid (str (java.util.UUID/randomUUID))
            juuids (vec (repeatedly 2 #(str (java.util.UUID/randomUUID))))
            post (fn [params]
               (h {:request-method :post
                   :scheme :http
                   :uri "/rawscheduler"
                   :headers {"Content-Type" "application/json"}
                   :authorization/user "dgrnbrg"
                   :body-params params}))
            initial-resp (post {"jobs" [(make-job :uuid (get juuids 0) :group guuid)]})
            no-override-resp (post {"jobs" [(make-job :uuid (get juuids 1) :group guuid)]})
            override-resp (post {"override-group-immutability" true
                                 "jobs" [(make-job :uuid (get juuids 1) :group guuid)]})]
        (is (<= 200 (:status initial-resp) 299))
        (is (<= 400 (:status no-override-resp) 499))
        (is (<= 200 (:status override-resp) 299))))

    (testing "Group with defaulted host placement"
      (let [guuid (str (UUID/randomUUID))
            post-resp (post {"groups" [{"uuid" guuid}]
                             "jobs" [(make-job :group guuid) (make-job :group guuid)]})
            group-resp (get-group guuid)]
        (is (<= 201 (:status post-resp) 299))
        (is (= "all" (-> group-resp first (get "host_placement") (get "type"))))))

    (testing "Use attribute equals parameter"
      (let [guuid (str (UUID/randomUUID))
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
      (let [guuid (str (UUID/randomUUID))
            post-resp (post {"groups" [(make-group :uuid guuid :placement-type "not-a-type")]
                             "jobs" [(make-job :group guuid) (make-job :group guuid)]})]
        (is (<= 400 (:status post-resp) 499))))

    (testing "Missing host placement attribute equals parameter"
      (let [guuid (str (UUID/randomUUID))
            post-resp (post {"groups" [{"uuid" guuid
                                        "host_placement" {"type" "attribute-equals"}}]
                             "jobs" [(make-job :group guuid) (make-job :group guuid)]})]
        (is (<= 400 (:status post-resp) 499))))

    (testing "Straggler handling quantile deviation"
      (let [guuid1 (str (UUID/randomUUID))
            juuid1 (str (UUID/randomUUID))
            juuid2 (str (UUID/randomUUID))
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
      (let [guuid (str (UUID/randomUUID))
            post-resp1 (post {"jobs" [(make-job :group guuid) (make-job :group guuid)]})
            post-resp2 (post {"jobs" [(make-job :group guuid)]})]
        (is (<= 201 (:status post-resp1) 299))
        (is (<= 400 (:status post-resp2) 499))))
    ))


(deftest reasons-api
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        db (d/db conn)
        default-limit 10]
    ;; set default failure limit.
    @(d/transact conn [{:db/id :scheduler/config
                        :scheduler.config/mea-culpa-failure-limit default-limit}])

    (testing "reason-entity->consumable-map"
      (let [not-mea-culpa {:db/id 17592186045473
                           :reason/code 4003
                           :reason/string "Container launch failed"
                           :reason/mesos-reason :reason-container-launch-failed
                           :reason/name :mesos-container-launch-failed
                           :reason/mea-culpa? false}
            mea-culpa-with-limit {:db/id 17592186045473
                                  :reason/code 4003
                                  :reason/string "Container launch failed"
                                  :reason/mesos-reason :reason-container-launch-failed
                                  :reason/name :mesos-container-launch-failed
                                  :reason/mea-culpa? true
                                  :reason/failure-limit 8}
            mea-culpa-with-default {:db/id 17592186045473
                                    :reason/code 4003
                                    :reason/string "Container launch failed"
                                    :reason/mesos-reason :reason-container-launch-failed
                                    :reason/name :mesos-container-launch-failed
                                    :reason/mea-culpa? true}]
        (is (= (api/reason-entity->consumable-map default-limit not-mea-culpa)
               {:code 4003
                :name "mesos-container-launch-failed"
                :description "Container launch failed"
                :mea_culpa false}))
        (is (= (api/reason-entity->consumable-map default-limit mea-culpa-with-limit)
               {:code 4003
                :name "mesos-container-launch-failed"
                :description "Container launch failed"
                :mea_culpa true
                :failure_limit 8}))
        (is (= (api/reason-entity->consumable-map default-limit mea-culpa-with-default)
               {:code 4003
                :name "mesos-container-launch-failed"
                :description "Container launch failed"
                :mea_culpa true
                :failure_limit default-limit}))))

    (testing "reasons-handler"
      (let [handler (basic-handler conn :retry-limit 200)
            reasons-response (handler {:request-method :get
                                       :scheme :http
                                       :uri "/failure_reasons"
                                       :headers {"Content-Type" "application/json"}
                                       :authorization/user "user"})
            reasons-body (response->body-data reasons-response)]
        (is (= (:status reasons-response 200)))
        (is (= (count reasons-body)
               (count (reason/all-known-reasons db))))))))

(deftest retry-validator
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        uuid (UUID/randomUUID)
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

(deftest test-max-128-characters-and-alphanum?
  (is (api/max-128-characters-and-alphanum? ""))
  (is (api/max-128-characters-and-alphanum? "a"))
  (is (api/max-128-characters-and-alphanum? (apply str (repeat 128 "a"))))
  (is (not (api/max-128-characters-and-alphanum? (apply str (repeat 129 "a")))))
  (is (api/max-128-characters-and-alphanum? "allows.dots"))
  (is (api/max-128-characters-and-alphanum? "allows-dashes"))
  (is (api/max-128-characters-and-alphanum? "allows_underscores")))

(deftest test-non-empty-max-128-characters-and-alphanum?
  (is (not (api/non-empty-max-128-characters-and-alphanum? "")))
  (is (api/non-empty-max-128-characters-and-alphanum? "a"))
  (is (api/non-empty-max-128-characters-and-alphanum? (apply str (repeat 128 "a"))))
  (is (not (api/non-empty-max-128-characters-and-alphanum? (apply str (repeat 129 "a")))))
  (is (api/non-empty-max-128-characters-and-alphanum? "allows.dots"))
  (is (api/non-empty-max-128-characters-and-alphanum? "allows-dashes"))
  (is (api/non-empty-max-128-characters-and-alphanum? "allows_underscores")))

(defn- minimal-job
  "Returns a dummy job with the minimal set of fields"
  []
  {:uuid (UUID/randomUUID)
   :command "ls"
   :name ""
   :priority 0
   :max-retries 1
   :max-runtime 1
   :cpus 0.1
   :mem 128.
   :user "user"})

(deftest test-job-schema
  (testing "Job schema validation"
    (let [min-job (minimal-job)]

      (testing "should require the minimal set of fields"
        (is (s/validate api/Job min-job))
        (is (thrown? Exception (s/validate api/Job (dissoc min-job :uuid))))
        (is (thrown? Exception (s/validate api/Job (dissoc min-job :command))))
        (is (thrown? Exception (s/validate api/Job (dissoc min-job :name))))
        (is (thrown? Exception (s/validate api/Job (dissoc min-job :priority))))
        (is (thrown? Exception (s/validate api/Job (dissoc min-job :max-retries))))
        (is (thrown? Exception (s/validate api/Job (dissoc min-job :max-runtime))))
        (is (thrown? Exception (s/validate api/Job (dissoc min-job :cpus))))
        (is (thrown? Exception (s/validate api/Job (dissoc min-job :mem))))
        (is (thrown? Exception (s/validate api/Job (dissoc min-job :user)))))

      (testing "should allow an optional application field"
        (is (s/validate api/Job (assoc min-job :application {:name "foo-app", :version "0.1.0"})))
        (is (thrown? Exception (s/validate api/Job (assoc min-job :application {:name "", :version "0.2.0"}))))
        (is (thrown? Exception (s/validate api/Job (assoc min-job :application {:name "bar-app", :version ""})))))

      (testing "should allow an optional expected-runtime field"
        (is (s/validate api/Job (assoc min-job :expected-runtime 2 :max-runtime 3)))
        (is (s/validate api/Job (assoc min-job :expected-runtime 2 :max-runtime 2)))
        (is (thrown? Exception (s/validate api/Job (assoc min-job :expected-runtime 3 :max-runtime 2))))))))

(deftest test-create-jobs!
  (let [expected-job-map
        (fn
          ; Converts the provided job and framework-id (fid) to the job-map we expect to get back from
          ; api/fetch-job-map. Note that we don't include the submit_time field here, so assertions below
          ; will have to dissoc it.
          [{:keys [mem max-retries max-runtime expected-runtime name gpus
                   command ports priority uuid user cpus application]}
           fid]
          (cond-> {;; Fields we will fill in from the provided args:
                   :command command
                   :cpus cpus
                   :framework_id fid
                   :gpus (or gpus 0)
                   :max_retries max-retries
                   :max_runtime max-runtime
                   :mem mem
                   :name name
                   :ports (or ports 0)
                   :priority priority
                   :user user
                   :uuid (str uuid)
                   ;; Fields we will simply hardcode for this test:
                   :env {}
                   :instances ()
                   :labels {}
                   :retries_remaining 1
                   :state "waiting"
                   :status "waiting"
                   :uris nil}
                  ;; Only assoc these fields if the job specifies one
                  application (assoc :application application)
                  expected-runtime (assoc :expected-runtime expected-runtime)))]

    (testing "Job creation"

      (testing "should work with a minimal job"
        (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
              {:keys [uuid] :as job} (minimal-job)
              fid #mesomatic.types.FrameworkID{:value "fid"}]
          (is (= {::api/results (str "submitted jobs " uuid)}
                 (api/create-jobs! conn {::api/jobs [job]})))
          (is (= (expected-job-map job fid)
                 (dissoc (api/fetch-job-map (db conn) fid uuid) :submit_time)))))

      (testing "should work when the job specifies an application"
        (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
              application {:name "foo-app", :version "0.1.0"}
              {:keys [uuid] :as job} (assoc (minimal-job) :application application)
              fid #mesomatic.types.FrameworkID{:value "fid"}]
          (is (= {::api/results (str "submitted jobs " uuid)}
                 (api/create-jobs! conn {::api/jobs [job]})))
          (is (= (expected-job-map job fid)
                 (dissoc (api/fetch-job-map (db conn) fid uuid) :submit_time)))))

      (testing "should work when the job specifies the expected runtime"
        (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
              {:keys [uuid] :as job} (assoc (minimal-job) :expected-runtime 1)
              fid #mesomatic.types.FrameworkID{:value "fid"}]
          (is (= {::api/results (str "submitted jobs " uuid)}
                 (api/create-jobs! conn {::api/jobs [job]})))
          (is (= (expected-job-map job fid)
                 (dissoc (api/fetch-job-map (db conn) fid uuid) :submit_time))))))))

(defn- minimal-config
  "Returns a minimal configuration map"
  []
  {:config {:database {:datomic-uri "datomic:mem://cook-jobs"}
            :mesos {:master "MESOS_MASTER"
                    :leader-path "/cook-scheduler"}
            :authorization {:one-user "root"}
            :scheduler {}
            :zookeeper {:local? true}
            :port 10000
            :metrics {}
            :nrepl {}}})

(deftest test-stringify
  (is (= {:foo (str +)} (api/stringify {:foo +})))
  (is (= {:foo {:bar (str +)}} (api/stringify {:foo {:bar +}})))
  (is (= {:foo "bar"} (api/stringify {:foo (atom "bar")})))
  (is (= {:foo (str +)} (api/stringify {:foo (atom +)})))
  (let [server (TestingServer.)]
    (is (= {:foo (str server)} (api/stringify {:foo server})))
    (.close server))
  (let [minutes (t/minutes 1)]
    (is (= {:foo (str minutes)} (api/stringify {:foo minutes}))))
  (let [socket (ServerSocket.)]
    (is (= {:foo (str socket)} (api/stringify {:foo socket})))
    (.close socket))
  (let [settings (components/config-settings (minimal-config))]
    (is (thrown? JsonGenerationException (cheshire/generate-string settings)))
    (is (cheshire/generate-string (api/stringify settings)))))
