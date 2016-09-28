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
           [cook.test.mesos.schema :as schema :refer (restore-fresh-database! create-dummy-job create-dummy-instance)]
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
   "max_runtime" 1000000
   "cpus" 2.0
   "mem" 2048.0})

(defn basic-handler
  [conn & {:keys [cpus mem gpus-enabled retry-limit] :or {cpus 12 mem 100 gpus-enabled false retry-limit 200}}]
  (main-handler conn "my-framework-id" {:cpus cpus :memory-gb mem :retry-limit retry-limit} gpus-enabled
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
        h (main-handler conn "my-framework-id" {:cpus 3.0 :memory-gb 2 :retry-limit 200}
                        false (fn [] []) authorized-fn)]
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
        uuid (str (java.util.UUID/randomUUID))
        create-response (h {:request-method :post
                            :scheme :http
                            :uri "/rawscheduler"
                            :headers {"Content-Type" "application/json"}
                            :authorization/user "mforsyth"
                            :body-params {"jobs" [(merge (basic-job)
                                                         {"uuid" uuid
                                                          "max_retries" 42})]}})
        retry-req-attrs {:scheme :http
                         :uri "/retry"
                         :authorization/user "mforsyth"}]
    (testing "read retry count"
      (let [resp (h (merge retry-req-attrs  {:request-method :get
                                             :query-params {"job" uuid}}))
            _ (is (<= 200 (:status resp) 299))
            body (-> resp :body slurp json/read-str)]
        (is (= body 42))))

    (testing "update retry count"
      (let [update-resp (h (merge retry-req-attrs
                                  {:request-method :put
                                   :body-params {"job" uuid "retries" 70}}))
            _ (is (<= 200 (:status update-resp) 299))
            read-resp (h (merge retry-req-attrs  {:request-method :get
                                                  :query-params {"job" uuid}}))
            read-body (-> read-resp :body slurp json/read-str)]
        (is (= read-body 70))))))

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

(deftest retry-validator
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        uuid (str (java.util.UUID/randomUUID))
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
                               :body-params {"job" uuid
                                        "retries" 199}}))
                  299)))
        (testing "At limits"
          (is (<= 200
                  (:status (h {:request-method :post
                               :scheme :http
                               :uri "/retry"
                               :headers {"Content-Type" "application/json"}
                               :authorization/user "dgrnbrg"
                               :body-params {"job" uuid
                                        "retries" 200}}))
                  299)))
        (testing "Over limit"
          (is (<= 400
                  (:status (h {:request-method :post
                               :scheme :http
                               :uri "/retry"
                               :headers {"Content-Type" "application/json"}
                               :authorization/user "dgrnbrg"
                               :body-params {"job" uuid
                                        "retries" 201}}))
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
