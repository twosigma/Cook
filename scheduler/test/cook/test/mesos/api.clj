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
 (:require [cook.mesos.api :as api :refer (handler)]
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
        h (handler conn "my-framework-id" {:cpus 12 :memory-gb 100} false (fn [] []) authorized-fn)]
    (println )
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
        job (fn [cpus mem] {"uuid" (str (java.util.UUID/randomUUID))
                            "command" "hello world"
                            "name" "my-cool-job"
                            "priority" 66
                            "max_retries" 100
                            "max_runtime" 1000000
                            "cpus" cpus
                            "mem" mem})
        h (handler conn "my-framework-id" {:cpus 3.0 :memory-gb 2} false (fn [] []) authorized-fn)]
    (testing "Within limits"
      (is (<= 200
              (:status (h {:request-method :post
                           :scheme :http
                           :uri "/rawscheduler"
                           :headers {"Content-Type" "application/json"}
                           :authorization/user "dgrnbrg"
                           :body-params {:jobs [(job 1 500)]}}))
              299)))
    (testing "At limits"
      (is (<= 200
              (:status (h {:request-method :post
                           :scheme :http
                           :uri "/rawscheduler"
                           :headers {"Content-Type" "application/json"}
                           :authorization/user "dgrnbrg"
                           :body-params {:jobs [(job 3 2048)]}}))
              299)))
    (testing "Beyond limits cpus"
      (is (<= 400
              (:status (h {:request-method :post
                           :scheme :http
                           :uri "/rawscheduler"
                           :headers {"Content-Type" "application/json"}
                           :authorization/user "dgrnbrg"
                           :body-params {"jobs" [(job 3.1 100)]}}))
              499)))
    (testing "Beyond limits mem"
      (is (<= 400
              (:status (h {:request-method :post
                           :scheme :http
                           :uri "/rawscheduler"
                           :headers {"Content-Type" "application/json"}
                           :authorization/user "dgrnbrg"
                           :body-params {"jobs" [(job 1 2050)]}}))
              499)))))

(deftest gpus-api
  (let [conn (restore-fresh-database! "datomic:mem://mesos-api-test")
        job (fn [gpus ] {"uuid" (str (java.util.UUID/randomUUID))
                         "command" "hello world"
                         "name" "my-cool-job"
                         "priority" 66
                         "max_retries" 100
                         "max_runtime" 1000000
                         "gpus" gpus
                         "cpus" 1.0
                         "mem" 1024.0})
        h (handler conn "my-framework-id" {:cpus 12 :memory-gb 100} true (fn [] []) authorized-fn)]
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
