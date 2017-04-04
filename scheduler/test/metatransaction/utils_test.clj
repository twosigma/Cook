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
(ns metatransaction.utils-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [datomic.api :as d]
            [metatransaction.core :as mt]
            [metatransaction.utils :refer :all]))

(def wait
  (datomic.function/construct
    {:lang :clojure
     :params '[db sleep]
     :code '(Thread/sleep sleep)}))

(def throw-ex
  (datomic.function/construct
    {:lang :clojure
     :params '[db msg]
     :code '(throw (ex-info msg {}))}))

(def test-schema
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
      :db.install/_attribute :db.part/db}
     {:db/id #db/id [:db.part/user]
      :db/ident :utils/wait
      :db/fn wait}
     {:db/id #db/id [:db.part/user]
      :db/ident :utils/throw-ex
      :db/fn throw-ex}])

(defn get-conn []
  (let [uri (str "datomic:mem://test")
        _ (d/delete-database uri)
        _ (d/create-database uri)
        conn (d/connect uri)]
    conn))

(defn get-test-conn []
  (let [conn (get-conn)
        _ (install-utils-support conn)
        _ (mt/install-metatransaction-support conn)
        _ @(d/transact conn test-schema)]
    conn))

(def uno-txn
  [{:db/id (d/tempid :db.part/user)
    :job/id "1"
    :job/name "uno"}])

(def get-ent-id-from-id-query
  '[:find ?e
    :in $ ?id
    :where [?e :job/id ?id]])

(def opts
  {:retry-schedule [10 100 1000]
   :transaction-timeout 1000})

(deftest pass-test
  (testing "simple-pass-test"
    (with-out-str
      (let [conn (get-test-conn)
            update-out (update!! conn opts (fn [_] uno-txn))]
        (is (:transaction update-out))))))

(deftest assert-test
  (testing "assert-db"
    (with-out-str
      (let [conn (get-test-conn)]
        (d/transact conn uno-txn)
        (is (thrown-with-msg? Exception
                              #"Failed assertion"
                              @(d/transact conn [[:utils/assert-db :exists get-ent-id-from-id-query "5"]])))
        (is @(d/transact conn [[:utils/assert-db :exists get-ent-id-from-id-query "1"]]))))))

(deftest no-retry-from-failing-update-fn-test
  (testing "retry"
    (with-out-str
      (let [conn (get-test-conn)
            a (atom true)
            exception-when-a-true (fn [db] (if @a
                                             (throw (ex-info "a was true" {}))
                                             [{:db/id (d/tempid :db.part/user)
                                               :job/id "1"
                                               :job/name "uno"}]))
            resp-chan (update-async conn opts exception-when-a-true)]
        (async/<!! (async/timeout 50)) ; Let run update fail at least once
        (reset! a false)
        (let [resp (async/<!! resp-chan)]
          (is (instance? clojure.lang.ExceptionInfo resp))
          (is (= "a was true" (.. resp getCause getMessage))))))))

(deftest use-case-test
  (testing "use-case with transact-with-retries!!"
    (with-out-str
      (let [conn (get-test-conn)
            query-id (fn [db id]
                       (d/q get-ent-id-from-id-query db id))
            gen-tx (fn [db id]
                     (let [e (ffirst (query-id db id))]
                       (if e
                         [{:db/id e
                           :job/name (str "name: " id)}]
                         [{:db/id (d/tempid :db.part/user)
                           :job/id (str (inc (read-string id)))
                           :job/name (str "name: " id)}
                          [:utils/assert-db :none get-ent-id-from-id-query id]])))
            id "5"
            fut (async/go (d/transact-async conn [[:utils/wait (* 5 100)]
                                                  {:db/id (d/tempid :db.part/user)
                                                   :job/id id}]))
            resp (update!! conn opts gen-tx id)]
        (is (:transaction resp))))))



(deftest idempotent-test
  (testing "basic idempotency"
    (with-out-str
      (let [conn (get-test-conn)
            uuid (d/squuid)
            id "5"
            tx [[:utils/idempotent-transaction uuid]
                {:db/id (d/tempid :db.part/user)
                 :job/id (str (inc (read-string id)))
                 :job/name (str "name: " id)}]]
        @(d/transact conn tx)
        (is (thrown-with-msg? Exception
                              #"idempotency check failed."
                              @(d/transact conn tx))))))
  (testing "transact with retries idempotency"
    (with-out-str
      (let [conn (get-test-conn)
            uuid (d/squuid)
            id "6"
            tx [[:utils/idempotent-transaction uuid]
                {:db/id (d/tempid :db.part/user)
                 :job/id (str (inc (read-string id)))
                 :job/name (str "name: " id)}]]
        @(d/transact conn tx)
        (try
          @(transact-with-retries!! conn {} tx)
          (is false)
          (catch Exception e
            (is (thrown-with-msg? Exception
                                  #"idempotency check failed."
                                  (throw (last (:errors (ex-data e)))))))))))
  (testing "make-idempotent"
    (with-out-str
      (let [conn (get-test-conn)
            id "7"
            tx-fn (fn [db]
                    [{:db/id (d/tempid :db.part/user)
                      :job/id (str (inc (read-string id)))
                      :job/name (str "name: " id)}])
            idempotent-tx-fn (make-idempotent tx-fn)]
        @(d/transact conn (idempotent-tx-fn (d/db conn)))
        (try
          @(update!! conn {} idempotent-tx-fn)
          (is false)
          (catch Exception e
            (is (:idempotent-ex? (ex-data e))))))))
  (testing "with-idempotency-working"
    (with-out-str
      (let [conn (get-test-conn)
            id "8"
            tx-fn (fn [db]
                    [{:db/id (d/tempid :db.part/user)
                      :job/id (str (inc (read-string id)))
                      :job/name (str "name: " id)}])
            idempotent-tx-fn (make-idempotent tx-fn)]
        (is
          ((complement #(isa? % Exception))
            (suppress-idempotent-exceptions
              (update!! conn {} idempotent-tx-fn)))))))
  (testing "with-idempotency"
    (with-out-str
      (let [conn (get-test-conn)
            id "8"
            tx-fn (fn [db]
                    [{:db/id (d/tempid :db.part/user)
                      :job/id (str (inc (read-string id)))
                      :job/name (str "name: " id)}])
            idempotent-tx-fn (make-idempotent tx-fn)]
        @(d/transact conn (idempotent-tx-fn (d/db conn)))
        (is
          (suppress-idempotent-exceptions
            (update!! conn {} idempotent-tx-fn)))))))

(deftest retry-test
  (testing "retry with update!"
    (with-out-str
      (let [conn (get-test-conn)
            a (atom true)
            exception-when-a-true (fn [db] (if @a
                                             [{:db/id (d/tempid :db.part/user)
                                               :job/id "1"
                                               :job/name "uno"}
                                              [:utils/throw-ex "test!"]]
                                             [{:db/id (d/tempid :db.part/user)
                                               :job/id "1"
                                               :job/name "uno"}]))
            resp-chan (async/chan)]
        (async/go
          (try
            (async/>! resp-chan (update! conn opts exception-when-a-true))
            (catch Exception e
              (async/>! resp-chan e))))
        (async/<!! (async/timeout 10)) ; Let run update fail at least once
        (reset! a false)
        (let [resp (async/<!! resp-chan)]
          (is (:transaction resp)))))))

(deftest use-case-test
  (testing "use-case with update!"
    (with-out-str
      (let [conn (get-test-conn)
            query-id (fn [db id]
                       (d/q get-ent-id-from-id-query db id))
            gen-tx (fn [db id]
                     (let [e (ffirst (query-id db id))]
                       (if e
                         [{:db/id e
                           :job/name (str "name: " id)}]
                         [{:db/id (d/tempid :db.part/user)
                           :job/id (str (inc (read-string id)))
                           :job/name (str "name: " id)}
                          [:utils/assert-db :none get-ent-id-from-id-query id]])))
            id "5"
            fut (async/go (d/transact-async conn [[:utils/wait (* 5 100)]
                                                  {:db/id (d/tempid :db.part/user)
                                                   :job/id id}]))
            resp-chan (async/chan)]
        (async/go
          (try
            (async/>! resp-chan (update! conn opts gen-tx id))
            (catch Exception e
              (async/>! resp-chan e))))
        (let [resp (async/<!! resp-chan)]
          (is (and (not (instance? Throwable resp)) (:transaction resp))))))))



#_(d/transact conn tmp-schema)
#_ (def id "12")
#_ (def fut (async/go (d/transact-async conn [[:utils/wait (* 30 1000)] {:db/id #db/id [:db.part/user] :job/id id}])))
#_ (transactStuff (d/db conn) id)
#_ (update conn {:retry-schedule [10 100 1000 10000 60000]} transactStuff id)
#_ (update conn {:retry-schedule [10 10 10]} transactStuff "5")
#_ (query-id (d/db conn) "1")
#_ (def a (atom false))
#_ (def conn (get-test-conn))
#_ (defn query-id [db id]
     (println db id)
     (d/q '[:find ?e
            :in $ ?id
            :where
            [?e :job/id ?id]]
          db id))
#_ (defn transactStuff [db id]
     (println query-id)
     (let [e (ffirst (query-id db id))]
       (println e)
       (if e
         [{:db/id e
           :job/name (str "name: " id)}]
         [{:db/id (d/tempid :db.part/user)
           :job/id (str (inc (read-string id)))
           :job/name (str "name: " id)
           }
          [:utils/assert-db #(not (ffirst (query-id % id)))]

          ])))

#_ (defn exceptionIf [db _]
     (when @a
       (throw (ex-info "Simple test" {})))
     [{:db/id (d/tempid :db.part/user)
       :job/id "2"
       :job/name "uno"}])
#_ (update conn {:transaction-timeout1 1000} exceptionIf "1")

(comment
  (def conn (get-test-conn))
  (def uuid (d/squuid))
  (def uuid2 (d/squuid))
  (def id "5")
  (d/transact conn [{:db/id (d/tempid :db.part/tx)
                     :tx/idempotent-uuid uuid}
                    {:db/id (d/tempid :db.part/user)
                     :job/id (str (inc (read-string id)))
                     :job/name (str "name: " id)}])
  (d/transact conn [{:db/id (d/tempid :db.part/tx)
                     :tx/idempotent-uuid uuid2}
                    {:db/id (d/tempid :db.part/user)
                     :job/id (str (inc (read-string id)))
                     :job/name (str "name: " id)}])


  (d/entity (d/db conn) [:tx/idempotent-uuid uuid])
  (d/entity (d/db conn) [:tx/idempotent-uuid uuid2])
  (d/entity (d/db conn) [:tx/idempotent-uuid (d/squuid)]))
