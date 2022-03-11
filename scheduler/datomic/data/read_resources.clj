

; run this with lein


(ns data.read-resources
  (:require [cook.datomic :as datomic]
            [cook.postgres :as pg]
            [cook.quota :as quota]
            [cook.queries :as queries]
            [datomic.api :as d]))

(def uri (second *command-line-args*))
(println "Datomic URI is" uri)

(defn pools
  [db]
  (->> (d/q '[:find [?p ...]
              :in $ [?state ...]
              :where
              [?p :pool/state ?state]]
            db [:pool.state/active :pool.state/inactive])
       (map (partial d/entity db))
       (map d/touch)))

(defn quotas
      [db]
      (->> (d/q '[:find [?q ...]
                  :in $
                  :where
                  [?q :quota/user]]
                db)
           (map (partial d/entity db))
           (map d/touch)))

(try
  (let [conn (datomic/create-connection {:settings {:mesos-datomic-uri uri}})]
    (println "Connected to Datomic:" conn)
    ;(quota/set-quota! conn "default" "k8s-alpha" "For quota-related testing." :cpus 8 :mem 1024)
    ;(quota/set-quota! conn "default" "k8s-gamma" "For quota-related testing." :cpus 9 :mem 2048)
    (println "Pools & Quotas:")
    (run! (fn [{:keys [pool/name] :as p}]
            (clojure.pprint/pprint p)
            (clojure.pprint/pprint (quota/get-quota (d/db conn) "default" name)))
          (pools (d/db conn)))
       (let [db (d/db conn)]
            (clojure.pprint/pprint (queries/get-all-resource-types db))
            (clojure.pprint/pprint (quotas db))
            (spit "datomic_quota_export.sql"
                  (str "truncate resource_limits;\n"
                       (clojure.string/join
                         "\n"
                         (map
                           (fn [quota]
                               (clojure.string/join
                                 "\n"
                                 (map
                                   (fn [resource]
                                       (str "INSERT INTO resource_limits "
                                            "(resource_limit_type, pool_name, user_name, resource_name, amount, reason) VALUES "
                                            "('quota','" (->> resource :resource/pool :db/id (d/entity db) :pool/name) "','" (-> quota :quota/user) "','" (name (-> resource :resource/type)) "', " (-> resource :resource/amount) ", '" (-> quota :quota/reason) "');"
                                            ))
                                   (-> quota :quota/resource)))
                                  )
                              (quotas db)))
                       "\n"))

            )
       (System/exit 0))
  (catch Throwable t
    (println "Failed to seed pools:" t)
    (System/exit 1)))
