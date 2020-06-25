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
(ns metatransaction.core-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [metatransaction.core :refer :all]))

(def test-schema
  [{:db/id #db/id[:db.part/db]
    :db/ident :job/id
    :db/valueType :db.type/string
    :db/index true
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   {:db/id #db/id [:db.part/db]
    :db/ident :job/name
    :db/index true
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(defmacro with-conn [[conn-sym] & body]
  `(let [uri# "datomic:mem://test"
         _# (d/create-database uri#)
         ~conn-sym (d/connect uri#)]
     (try
       ~@body
       (finally
         (d/delete-database uri#)))))

(defmacro with-test-conn [[conn-sym] & body]
  `(with-conn [~conn-sym]
     @(d/transact ~conn-sym test-schema)
     (~install-metatransaction-support ~conn-sym)
     ~@body))

(deftest setup-test
  (testing "setup"
    (with-conn [conn]
      (is (install-metatransaction-support conn)))))

(deftest include-in-test
  (testing "include-in-basic"
    (with-test-conn [conn]
      @(d/transact
         conn
         [[:metatransaction/include-in (d/squuid)]
          {:db/id #db/id[:db.part/user]
           :job/id "1"
           :job/name "uno"}])
      (is (= #{["1" "uno" :metatransaction.status/uncommitted]}
             (d/q '[:find ?id ?name ?mt-status
                    :where
                    [?mt :metatransaction/status ?s]
                    [?s :db/ident ?mt-status]
                    [?j :job/id ?id ?tx]
                    [?j :job/name ?name ?tx]
                    [?mt :metatransaction/tx ?tx]]
                  (d/db conn)))))))

(deftest commit-test
  (testing "commit-basic"
    (with-test-conn [conn]
      (let [uuid (d/squuid)]
        @(d/transact conn
                     [[:metatransaction/include-in uuid]
                      {:db/id #db/id[:db.part/user]
                       :job/id "1"
                       :job/name "uno"}])
        @(d/transact conn [[:metatransaction/commit uuid]])
        (is (= #{["1" "uno" :metatransaction.status/committed]}
               (d/q '[:find ?id ?name ?mt-status
                      :where
                      [?mt :metatransaction/status ?s]
                      [?s :db/ident ?mt-status]
                      [?j :job/id ?id ?tx]
                      [?j :job/name ?name ?tx]
                      [?mt :metatransaction/tx ?tx]]
                    (d/db conn))))))))

(deftest multiple-commit-test
  (testing "multiple-commit"
    (with-test-conn [conn]
     (let [uuid (d/squuid)]
      @(d/transact
         conn
         [[:metatransaction/include-in uuid]
          {:db/id #db/id[:db.part/user]
           :job/id "1"
           :job/name "uno"}])
      @(d/transact conn [[:metatransaction/commit uuid]])
      @(d/transact conn [[:metatransaction/commit uuid]])
      @(d/transact conn [[:metatransaction/commit uuid]])
      (is (= #{["1" "uno" :metatransaction.status/committed]}
             (d/q '[:find ?id ?name ?mt-status
                    :where
                    [?mt :metatransaction/status ?s]
                    [?s :db/ident ?mt-status]
                    [?j :job/id ?id ?tx]
                    [?j :job/name ?name ?tx]
                    [?mt :metatransaction/tx ?tx]]
                  (d/db conn))))))))

(deftest include-in-commit-test
  (testing "include-in-commit-error"
    (with-test-conn [conn]
      (let [uuid (d/squuid)]
        @(d/transact
           conn
           [[:metatransaction/include-in uuid]
            {:db/id #db/id[:db.part/user]
             :job/id "1"
             :job/name "uno"}])
        @(d/transact conn [[:metatransaction/commit uuid]])
        (is (thrown-with-msg?
              Exception
              #"metatransaction .* has already been committed"
              @(d/transact
                 conn
                 [[:metatransaction/include-in uuid]
                  {:db/id #db/id[:db.part/user]
                   :job/id "2"
                   :job/name "dos"}])))))))

(deftest db-filter-test
  (testing "db-filter"
    (with-test-conn [conn]
      (let [uuid1 (d/squuid)
            uuid2 (d/squuid)]
        @(d/transact
           conn
           [[:metatransaction/include-in uuid1]
            {:db/id #db/id[:db.part/user]
             :job/id "1"
             :job/name "uno"}])
        @(d/transact
           conn
           [[:metatransaction/include-in uuid1]
            {:db/id #db/id[:db.part/user]
             :job/id "2"
             :job/name "dos"}])

        @(d/transact
           conn
           [[:metatransaction/include-in uuid2]
            {:db/id #db/id[:db.part/user]
             :job/id "3"
             :job/name "tres"}])
        @(d/transact
           conn
           [[:metatransaction/include-in uuid2]
            {:db/id #db/id[:db.part/user]
             :job/id "4"
             :job/name "cuatro"}])

        @(d/transact conn [[:metatransaction/commit uuid1]])

        (is (= #{["1" "uno" :metatransaction.status/committed]
                 ["2" "dos" :metatransaction.status/committed]}
               (d/q '[:find ?id ?name ?mt-status
                      :where
                      [?mt :metatransaction/status ?s]
                      [?s :db/ident ?mt-status]
                      [?j :job/id ?id ?tx]
                      [?j :job/name ?name ?tx]
                      [?mt :metatransaction/tx ?tx]]
                    (filter-committed (d/db conn)))))))))


(deftest use-case-test
  (testing "use-case"
    (with-test-conn [conn]
      (let [uuid1 (d/squuid)
            uuid2 (d/squuid)]
        @(d/transact
           conn
           [[:metatransaction/include-in uuid1]
            {:db/id #db/id[:db.part/user]
             :job/id "1"
             :job/name "uno"}])
        @(d/transact
           conn
           [[:metatransaction/include-in uuid1]
            {:db/id #db/id[:db.part/user]
             :job/id "2"
             :job/name "dos"}])

        @(d/transact
           conn
           [[:metatransaction/include-in uuid2]
            {:db/id #db/id[:db.part/user]
             :job/id "3"
             :job/name "tres"}])
        @(d/transact
           conn
           [[:metatransaction/include-in uuid2]
            {:db/id #db/id[:db.part/user]
             :job/id "4"
             :job/name "cuatro"}])
        @(d/transact
           conn
           [{:db/id #db/id[:db.part/user]
             :job/id "5"
             :job/name "cinco"}])

        @(d/transact conn [[:metatransaction/commit uuid1]])

        (is (= #{["1" "uno"]
                 ["2" "dos"]
                 ["5" "cinco"]}
               (d/q '[:find ?id ?name
                      :where
                      [?j :job/id ?id]
                      [?j :job/name ?name]]
                     (filter-committed (d/db conn)))))))))

(deftest history-use-case-test
  (testing "history-use-case"
    (with-test-conn [conn]
      (let [uuid1 (d/squuid)
            uuid2 (d/squuid)]
        @(d/transact
           conn
           [[:metatransaction/include-in uuid1]
            {:db/id #db/id[:db.part/user]
             :job/id "1"
             :job/name "uno"}])
        @(d/transact
           conn
           [[:metatransaction/include-in uuid1]
            {:db/id #db/id[:db.part/user]
             :job/id "2"
             :job/name "dos"}])

        @(d/transact
           conn
           [[:metatransaction/include-in uuid2]
            {:db/id #db/id[:db.part/user]
             :job/id "3"
             :job/name "tres"}])
        @(d/transact
           conn
           [[:metatransaction/include-in uuid2]
            {:db/id #db/id[:db.part/user]
             :job/id "4"
             :job/name "cuatro"}])
        @(d/transact
           conn
           [{:db/id #db/id[:db.part/user]
             :job/id "5"
             :job/name "cinco"}])

        @(d/transact conn [[:metatransaction/commit uuid1]])

        (is (= #{["1" "uno"]
                 ["2" "dos"]
                 ["5" "cinco"]}
               (d/q '[:find ?id ?name
                      :where
                      [?j :job/id ?id]
                      [?j :job/name ?name]]
                    (d/history (filter-committed (d/db conn))))))))))

(defn random-job-txn []
  [{:db/id (d/tempid :db.part/user)
    :job/id (str (rand-int 100000))
    :job/name (str (rand-int 100000))}
   [:metatransaction/include-in (java.util.UUID/randomUUID)]])

#_(deftest nested-query-bug-reproducer
  (testing "nested-query-bug-reproducer"
    (with-test-conn [conn]
      (let [threads 80
            c (async/chan)
            txns 100000]
        (doseq [txn (repeatedly txns random-job-txn)]
          @(d/transact conn txn))
        (dotimes [_ threads]
          (.start (Thread. (fn []
                             (is (d/q '[:find ?e
                                        :in $ ?attr ?v
                                        :where [?e ?attr ?v]]
                                      (db conn) :job/id (str (rand-int 100000))))
                             (async/>!! c :test)))))
        (loop [n 0]
          (when (< n threads)
            (async/<!! c)
            (recur (inc n))))))))

#_(defn uuid [] (java.util.UUID/randomUUID))

#_(def uri "datomic:mem://test1")
#_(d/create-database uri)
#_(def conn (d/connect uri))

#_ (def mt (d/entity (d/db conn) [:metatransaction/uuid tmp-uuid]))

#_(def tmp-schema
    [{:db/id #db/id[:db.part/db]
      :db/ident :job/id
      :db/valueType :db.type/string
      :db/unique :db.unique/identity
      :db/cardinality :db.cardinality/one
      :db.install/_attribute :db.part/db}
     {:db/id #db/id [:db.part/db]
      :db/ident :job/name
      :db/valueType :db.type/string
      :db/cardinality :db.cardinality/many
      :db.install/_attribute :db.part/db}])
#_(d/transact conn tmp-schema)

#_ (d/transact conn [{:db/id (d/tempid :db.part/user) :job/id "1" :job/name "uno"}
                     {:db/id (d/tempid :db.part/user) :job/id "1" :job/name "dos"} ])

#_ (d/touch (d/entity (d/db conn) [:job/id "1"]))

#_(d/transact conn [{:db/id #db/id [:db.part/tx] :job/id "1"} {:db/id #db/id [:db.part/tx] :job/name "2"}])

#_(commit (d/db conn) tmp-uuid)

#_ (setup-metatransaction conn)

#_(d/transact conn metatransaction-schema)
#_(d/transact conn db-fns)
#_(def tmp-uuid (uuid))
#_(d/transact conn   [[:metatransaction/include-in tmp-uuid] {:db/id #db/id[:db.part/user] :job/id "tres"}])
#_(d/transact conn [[:metatransaction/commit tmp-uuid]])
#_ (def mts (d/q '[:find ?mt ?mt-uuid :where [?mt :metatransaction/uuid ?mt-uuid]] (d/db conn)))
#_ (def all (seq (d/datoms (filter-committed (d/db conn)) :eavt )))
#_(def jobs (d/q '[:find ?j ?id ?status :in $ :where [?mt :metatransaction/status ?s] [?s :db/ident ?status]  [?j :job/id ?id ?tx] [?mt :metatransaction/tx ?tx]] (filter-committed (d/db conn))))
#_(def jobs (d/q '[:find ?j ?id ?status :in $ ?uuid :where [?mt :metatransaction/uuid ?uuid] [?mt :metatransaction/status ?s] [?s :db/ident ?status]  [?j :job/id ?id ?tx] [?mt :metatransaction/tx ?tx]] (d/db conn) tmp-uuid))
#_(d/transact conn [{:db/id [:metatransaction/uuid tmp-uuid] }])
#_(def datoms (seq (d/datoms (db-filter (d/db conn)) :aevt :metatransaction/status)))

#_(d/transact
    conn
    [[:metatransaction/include-in uuid]
     {:db/id #db/id[:db.part/user]
      :job/id "2"
      :job/name "dos"}])
#_(d/transact
    conn
    [[:metatransaction/include-in uuid]
     {:db/id #db/id[:db.part/user]
      :job/id "3"
      :job/name "tres"}])

