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
(ns dev
  "Tools for interactive development with the REPL. This file should
  not be included in a production build of the application."
  (:require
   [clojure.java.io :as io]
   [clojure.java.javadoc :refer [javadoc]]
   [clojure.pprint :refer [pprint]]
   [clojure.reflect :refer [reflect]]
   [clojure.repl :refer [apropos dir doc find-doc pst source]]
   [clojure.edn :as edn]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.test :as test]
   [clojure.tools.namespace.repl :refer [refresh refresh-all]]
   [datomic.api :as d]
   [twosigma.datomic.copy-log :refer :all]
   [twosigma.datomic.filter-log :refer :all]
   [clj-leveldb :as leveldb])
  (:import
    (java.text SimpleDateFormat)))

(def test-n 5)

(def from-url "datomic:dev://localhost:4334/mbrainz-1968-1973")
(def to-url (str "datomic:dev://localhost:4334/test-" test-n))

(def from-url "datomic:mem://from")
(def to-url "datomic:mem://to")

(d/create-database from-url)

(def from-conn (d/connect from-url))
(def to-conn (d/connect to-url))

@(d/transact from-conn [])

(defn from-db [] (d/db from-conn))
(defn to-db [] (d/db to-conn))

(defonce dbidmap (atom-db-id-map))

(defn run1 []
  (filter-log {:pred (constantly true)
               :dbidmap dbidmap
               :from-conn from-conn
               :to-conn to-conn}))

(defn run2 []
  (filter-log {:pred (remove-attr (d/db from-conn) :instance/progress-text)
               :dbidmap dbidmap
               :from-conn from-conn
               :to-conn to-conn}))

(def instance-pull
  [:instance/id
   :instance/log-url
   :instance/pid
   :instance/progress-text
   :instance/status])

(def job-pull
  [:job/command-text
   :job/cpus
   :job/disk
   :job/hostgroup
   :job/id
   :job/memory
   :job/priority
   :job/retry-count
   :job/uuid
   {:job/user [:user/name]
    :job/instance instance-pull}])

(def sim-set-pull
  [:sim-set/name
   :sim-set/time
   :sim-set/uuid
   {:sim-set/user [:user/name]
    :sim-set/job job-pull}])

;; (defn instances [db]
;;   (set (map (fn [eid] (d/pull db instance-pull eid))
;;             (d/q '[:find [?instance ...]
;;                    :where [?instance :instance/id]]
;;                  db))))

;; (defn sim-sets [db]
;;   (set (map (fn [eid] (d/pull db sim-set-pull eid))
;;             (d/q '[:find [?sim-set ...]
;;                    :where [?sim-set :sim-set/uuid]]
;;                  db))))

;; (defn check1 []
;;   (let [from-db (d/db from-conn)
;;         to-db (d/db to-conn)]
;;     (= (sim-sets from-db)
;;        (sim-sets to-db))))

(defn read-log [string]
  (edn/read-string {:eof nil
                    :readers {'inst identity}
                    :default (fn [tag value] value)}
                   string))

(defn rewrite-log-file [file]
  (spit file (with-out-str (pprint (read-log (slurp file))))))

(def country-attributes
  [:country/name :release/country :artist/country ])

(defn remove-countries [db]
  (let [attrs (->> country-attributes
                   (map #(d/entid db %))
                   set)]
    (fn [datom]
      (not (contains? attrs (:a datom))))))

(defn sample-artists [db]
  (map #(d/entity db %)
       (ffirst
        (d/q '[:find (sample 1000 ?e)
               :in $
               :where [?e :artist/name]]
             db))))

(defn find-artists [db entities]
  (map #(d/entity db (first %))
       (d/q '[:find ?e :in $ [?gid ...] :where [?e :artist/gid ?gid]]
            db (map :artist/gid entities))))

(def some-ts
  #{1000 1001 1002 1028 1054 1080 1106 1132 1158 1184 1210 1236 1262 1288
    3229 3515})

(comment

(with-open [dbidmap (leveldb-id-map (str "/tmp/leveldb-" test-n))]
  (filter-log (init {:pred (remove-countries (d/db from-conn))
                     :from-conn from-conn
                     :to-conn to-conn
                     :dbidmap dbidmap})))

(with-open [dbidmap (leveldb-id-map (str "/tmp/leveldb-" test-n))]
  (let [ctx (init {:pred (remove-countries (d/db from-conn))
                   :from-conn from-conn
                   :to-conn to-conn
                   :dbidmap dbidmap})]
    (filter-ts some-ts ctx)))

(d/delete-database to-url)

(d/basis-t (from-db))
(d/basis-t (to-db))

(:t (first (d/tx-range (d/log to-conn) 29948 nil)))

(first (d/tx-range (d/log to-conn) 9861 nil))

)


(defn t->date [db t]
  (.format
    (SimpleDateFormat. "yyyy-MM-dd HH:mm")
    (:db/txInstant (d/entity db (d/t->tx t)))))

;; ufs
(comment
  (do
    (def from-uri "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/ufs?interface=http")
    (def to-uri "datomic:riak://ramkv.pit.twosigma.com:8098/datomic5/ufs?interface=http")
    (d/create-database to-uri)
    (def from-conn (d/connect from-uri))
    (def to-conn (d/connect to-uri)))


  (t->date (d/db to-conn) (d/basis-t (d/db to-conn)))

  (with-open [dbidmap (leveldb-id-map (str "/data/scratch/local/wyegelwe/dbidmaps/ufs.leveldb"))]
    (let [ctx (init {:pred (remove-autoexcision from-conn)
                     :dbidmap dbidmap
                     :from-conn from-conn
                     :to-conn to-conn
                     :riemann-host "rammon1.pit.twosigma.com"
                     })]
      (filter-log ctx)))
  )


(defn get-all-attr-txns
  [db attr]
  (map (comp d/tx->t first)
       (d/q '[:find ?tx
              :in $ ?attr
              :where
              [_ ?attr _ ?tx]]
            db attr)))

(defn get-schema-txns
  [db]
  (map (comp d/tx->t first)
       (d/q '[:find ?tx
              :where
              [_ :db/ident _ ?tx]
              ]
            db)))

(defn get-schema-ents
  [db]
  (d/q '[:find ?a ?tx
         :where
         [?e :db/ident ?a ?tx]]
       db))

(defn get-static-datoms-before
  [from-conn time-before]
  (let [schema-txns (filter #(>= % 1000) ; Only take user schema elements
                            (get-schema-txns (d/history (d/db from-conn))))
        schema-ents (get-schema-ents (d/db from-conn))
        our-schema-ents (->> schema-ents
                             (filter #(>= (-> % second d/tx->t) 100))
                             (map (comp #(d/entity (d/db from-conn) [:db/ident %]) first)))
        non-expiring-schema-ents (->> our-schema-ents
                                      (remove :autoexcision/duration)
                                      (map d/touch))
        static-txns (concat schema-txns
                            (mapcat (comp
                                      (partial get-all-attr-txns (d/db from-conn))
                                      :db/ident)
                                    non-expiring-schema-ents))
        t-before (:t (first (d/tx-range (d/log from-conn) time-before nil)))]
    (->> static-txns
         (filter #(< % t-before))
         distinct
         sort)))

;; mesos
(comment
  (do
    (def from-uri "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http")
    (def to-uri "datomic:riak://ramkv.pit.twosigma.com:8098/datomic5/mesos-jobs3?interface=http")
    (d/create-database to-uri)
    (def from-conn (d/connect from-uri))
    (def to-conn (d/connect to-uri)))

  (def schema-txns (filter #(>= % 1000) (get-schema-txns (d/history (d/db from-conn)))))
  (def schema-ents (get-schema-ents (d/db from-conn)))
  (def our-schema-ents
    (->> schema-ents
         (filter #(>= (-> % second d/tx->t) 1000))
         (map (comp #(d/entity (d/db from-conn) [:db/ident %]) first))))
  (def non-expiring-schema-ents
    (->> our-schema-ents
         (remove :autoexcision/duration)
         (map d/touch)))

  (def static-stuff
    (concat schema-txns
            (mapcat (comp
                      (partial get-all-attr-txns (d/db from-conn))
                      :db/ident)
                    non-expiring-schema-ents)))

  (def start #inst "2015-01-20T00:00:00-05:00")
  (def start-t (:t (first (d/tx-range (d/log from-conn) start nil))))

  (def last-to-txn (first (d/tx-range (d/log to-conn) (d/basis-t (d/db to-conn)) nil)))
  (def time-of-last-t (:db/txInstant (d/entity (d/db to-conn) (d/t->tx (:t last-to-txn)))))
  (def last-from-txn (first (rest (rest (rest (rest (rest (rest (rest (d/tx-range (d/log from-conn) time-of-last-t nil))))))))))

  (map (fn [[e a v t op]] [e (d/ident (d/db from-conn) a) v t op]) (:data last-from-txn))
  ([13194761036636 :db/txInstant #inst "2015-03-02T18:37:18.619-00:00" 13194761036636 true] [97 :db/ident :share/resource 13194761036636 true] [97 :db/valueType 20 13194761036636 true] [97 :db/isComponent true 13194761036636 true] [97 :db/cardinality 36 13194761036636 true] [0 :db.install/attribute 97 13194761036636 true] [98 :db/ident :share/user 13194761036636 true] [98 :db/valueType 23 13194761036636 true] [98 :db/unique 38 13194761036636 true] [98 :db/cardinality 35 13194761036636 true] [0 :db.install/attribute 98 13194761036636 true])
  (map (fn [[e a v t op]] [e (d/ident (d/db from-conn) a) v t op]) (:data last-to-txn))

  (def static-stuff-before-start-t (filter #(< % start-t) static-stuff))

  {:id #uuid "5518713e-afb4-4755-8aac-9d07e23c6eb0", :data [#datom[13194191773906 50 #inst "2015-03-02T18:37:17.290-00:00" 13194191773906 true] #datom[17592238203744 75 90 13194191773906 true] #datom[17592238203744 75 80 13194191773906 false] #datom[17592238203744 75 80 13194191773906 false]], :t 52240594}


  (d/attribute (d/db from-conn) :db.install/attribute)

  (map (comp (partial d/ident (d/db from-conn)) first)
       (clojure.set/difference
         (set (d/q '[:find ?attr
                     :where
                     [?attr :db/ident]
                     [?attr :db/valueType :db.type/ref]
                     ]
                   (d/db from-conn)
                   ))
          #{}
         #_(set (d/q '[:find ?attr
                     :where
                     [?attr :db/valueType :db.type/ref]]
                   (d/db from-conn)
                   ))))

  (d/touch (d/entity (d/db from-conn) [:db/ident :db.install/attribute]))

  (find-ref-attrs (d/db from-conn))

  (map (partial d/ident (d/db from-conn)) (find-ref-attrs (d/db from-conn)))

  (with-open [dbidmap (leveldb-id-map (str "/data/scratch/local/datomic-migration/mesos-jobs3/dbidmaps/mesos-jobs3.leveldb"))]
    #_(get-ids dbidmap [97 98])
    (let [ctx (init {:pred (remove-autoexcision from-conn)
                     :dbidmap dbidmap
                     :from-conn from-conn
                     :to-conn to-conn})]
      (filter-ts [621503324] ctx)))

  (with-open [dbidmap (leveldb-id-map (str "/data/scratch/local/datomic-migration/mesos-jobs3/dbidmaps/mesos-jobs3.leveldb"))]
    (let [ctx (init {:pred (remove-autoexcision from-conn)
                     :dbidmap dbidmap
                     :from-conn from-conn
                     :to-conn to-conn
                     :riemann-host "rammon1.pit.twosigma.com"
                     })]
      (filter-log ctx)))
  )





(comment
  (do
    (def datomic-db "cook-cloud2")
    (def leveldb "core")

    (def cook-from-uri "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/cook-cloud2?interface=http")
;    (def to-uri (str "datomic:riak://ramkv.pit.twosigma.com:8098/datomic5/" datomic-db "?interface=http"))
;    (d/create-database to-uri)
    (def from-conn (d/connect cook-from-uri))
 ;   (def to-conn (d/connect to-uri))

    (comment
      (map (partial d/ident (d/db cook-from-conn)) (find-ref-attrs (d/db cook-from-conn)))

    (do
      (println (d/basis-t (d/db from-conn)))
      (println (d/basis-t (d/db to-conn)))
      )

      (def last-tx (first (d/tx-range (d/log to-conn) (d/basis-t (d/db to-conn)) nil)))

      (def last-t (first (d/tx-range (d/log from-conn) #inst "2015-03-27T14:59:06.408-00:00" nil))))
;    (t->date (d/db from-conn) (d/basis-t (d/db from-conn)))
;    (t->date (d/db to-conn) (d/basis-t (d/db to-conn)))

    (defn get-all-attr-txns
      [db attr]
      (map (comp d/tx->t first)
           (d/q '[:find ?tx
                  :in $ ?attr
                  :where
                  [_ ?attr _ ?tx]]
                db attr)))

    (defn get-schema-txns
      [db]
      (map (comp d/tx->t first)
           (d/q '[:find ?tx
                  :where
                  [_ :db/ident _ ?tx]
                  ]
                db)))




    (def schema-txns (filter #(>= % 1000) (get-schema-txns (d/history (d/db from-conn)))))
    (def users (get-all-attr-txns (d/history (d/db from-conn)) :user/name))
    (def prefs (get-all-attr-txns (d/history (d/db from-conn)) :core/prefs))
    (def hosts (get-all-attr-txns (d/history (d/db from-conn)) :host/name))
    (def auth-user (get-all-attr-txns (d/history (d/db from-conn)) :authorization/user))

    (def static-stuff (concat schema-txns users prefs hosts auth-user))

    (def start #inst "2015-01-01T00:00:00-05:00")
    (def start-t (:t (first (d/tx-range (d/log from-conn) start nil))))

    (def static-stuff-before-start-t (filter #(< % start-t) static-stuff))

    (def static-txns-before-t (get-static-datoms-before from-conn start))

    #_(defn transfer-ts
      [from to dbidmap pred ts]
      (doseq [t ts]
        (filter-log {:start t
                     :end (inc t)
                     :pred pred
                     :dbidmap dbidmap
                     :from-conn from-conn
                     :to-conn to-conn}))))

  (d/basis-t (d/db from-conn))
  (clojure.pprint/pprint (take 3 (d/tx-range (d/log from-conn) 1453798225 nil)))
  (d/touch (d/entity (d/db from-conn) 100))

  (d/basis-t (d/db to-conn))


  (with-open [dbidmap (leveldb-id-map (str "/data/scratch/local/wyegelwe/dbidmaps/" leveldb ".leveldb"))]
    (let [ctx (init {:pred (remove-autoexcision from-conn)
                     :dbidmap dbidmap
                     :from-conn from-conn
                     :to-conn to-conn
                     :start start-t
                     })]
      (filter-ts (distinct (sort static-stuff-before-start-t)) ctx)
      (filter-log ctx)))

  (d/q '[:find ?a ?d ?attr
         :where
         [?a :autoexcision/duration ?d]
         [?a :db/ident ?attr]
         ]
       (d/db from-conn)
       )

  (with-open [dbidmap (leveldb-id-map (str "/data/scratch/local/wyegelwe/dbidmaps/" leveldb ".leveldb"))]
    (let [ctx (init {:pred (remove-autoexcision from-conn)
                     :dbidmap dbidmap
                     :from-conn from-conn
                     :to-conn to-conn
                     :riemann-host "rammon1.pit.twosigma.com"
                     })]
      (filter-log ctx)))



  (with-open [dbidmap (leveldb-id-map "/data/scratch/local/wyegelwe/dbidmaps/cook-cloud2.leveldb-3-27-2015_2-37")]
    (let [ctx (init {:pred (remove-autoexcision from-conn)
                     :dbidmap dbidmap
                     :from-conn from-conn
                     :to-conn to-conn
                     :start (inc 2566317248)
                     :riemann-host "rammon1.pit.twosigma.com"
                     })]
      (filter-log ctx)))

  (def ctx (copy-log/init {:end 4000  ; for demonstration
                           :pred (constantly true)
                           :dbidmap dbidmap
                           :from-conn from-conn
                           :to-conn to-conn}))

  (filter-log ctx)
  ;;=> {:last-t 3994, :transacted-count 450}

  ;; Must keep same dbidmap for next run

  ;; Next run starts at last-t + 1
  (filter-log (assoc ctx :start (inc (:last-t *1))))

  ;; With LevelDB storing entity ID mappings:
  (with-open [dbidmap (copy-log/leveldb-id-map "/tmp/dbidmap.leveldb")]
    (filter-log (copy-log/init {:pred (constantly true)
                                :dbidmap dbidmap
                                :from-conn from-conn
                                :to-conn to-conn}))))




