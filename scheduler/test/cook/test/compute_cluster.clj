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
(ns cook.test.compute-cluster
  (:require [clojure.test :refer :all]
            [cook.compute-cluster :refer :all]
            [cook.test.testutil :refer [restore-fresh-database!]]
            [datomic.api :as d]))

(deftest test-add-config
  (let [uri "datomic:mem://test-compute-cluster-config"
        conn (restore-fresh-database! uri)
        db-id (d/tempid :db.part/user)
        new-config [[:compute-cluster-config/name "cluster-1"]
                    [:compute-cluster-config/template "cluster-1-template"]
                    [:compute-cluster-config/base-path "https://35.224.254.75"]
                    [:compute-cluster-config/ca-cert "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURERENDQWZTZ0F3SUJBZ0lSQUtYaGhOVjlKdHRoYm9QYm1IVlFDMmd3RFFZSktvWklodmNOQVFFTEJRQXcKTHpFdE1Dc0dBMVVFQXhNa01UVTVPR1V5T0RBdE1EUTVPUzAwWWpSa0xUaGhNekV0TmpjM1pHRmlNRFJpTm1NMQpNQjRYRFRJd01EZ3hNREU1TVRJd01Wb1hEVEkxTURnd09USXdNVEl3TVZvd0x6RXRNQ3NHQTFVRUF4TWtNVFU1Ck9HVXlPREF0TURRNU9TMDBZalJrTFRoaE16RXROamMzWkdGaU1EUmlObU0xTUlJQklqQU5CZ2txaGtpRzl3MEIKQVFFRkFBT0NBUThBTUlJQkNnS0NBUUVBNHFLUXplRVNmN2kwMjBKcGhVeXdCN1pUTHVXRkFIMEk4VCtMWnRGMQp1TEwzMDdqMVJQN1lYeWxjcURBSm5TM0FiaEpTLytsMFRySUZGRkpnbEN5bGdIQjJSczV0Y2FjdlYrWmpQNkZXCmFSNExOTkNEZWJvM1JBRE5RM2ZPWGpIV3JZdnVFbTBKS1JsSk9QU1Q1Zmw1UnBXNERkK1djQ1FaclFoOWpMaG0KUExyUDBaMjlyTmI0RDBpeEk3enkwb09aUFVJd3k0N1A3eVVoYVE1TkszUWNtL0ZueWJlc1FxWXFpN0w5SGtCTgpsd0xxSjlVL3dIZDlGSG4zM0ZFUy8rZGhuV3pSa3pzejJrQlNYbHdybWxqMkFKbGNSRjlDdEFlNjU1RHJ1YlMrCk5ibENlNzUrdmxWcVl2RkNOcTNFUUc5Qlk1b1lwVDc4b3BIVVRlcER2V2NldVFJREFRQUJveU13SVRBT0JnTlYKSFE4QkFmOEVCQU1DQWdRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBTkJna3Foa2lHOXcwQkFRc0ZBQU9DQVFFQQpMNmNKUXRTUngwTDUxeVVra0FFQTQzK05QbWxWMFRvTUJCa3BOVHBveVRDU3l4N2pmLzMrVGs3cWUyYlk2ck1vCkJMVFBpeFYxaVZsRXIwL05yY25wQjhrU090YjJYY2p4UjBUdWpWd1JGUmN1STc0eFFRcndMVkUvZjdmNURlOUsKcWNtOHBYcWxFblYwM1JrQUdDbGZLUzBmSDJSTjRxbHRaLzJjL21MRkJTbmFZWWEreVUwMUFJSjNOYm5CQld2WQpqTFlMd28yQ3Bvdm5zQnV6cmkxZi9VZWRpUTBIUTFuL2ttUlVZbjIxU0JNU1NqcCsxTExKdDVBd0NEb01oYWw0CjIvOGRqOVR0NytrVE1wK0VjSUhTVWNzT2JobVA5cmRXS3ZobGtXaEZQOTFzb1V4MVJrWDMxdWFpZDFhcUN1cHMKREdoeUl3elJKWVJZdjJBQ2c0d2tIQT09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"]
                    ]
        _ (println (into [] (map #(into [] (concat [:db/add db-id] %)) new-config)))
        xx @(d/transact conn (into [] (map #(into [] (concat [:db/add db-id] %)) new-config)))
        db-id (d/tempid :db.part/user)]
    (println xx)


    (println @(d/transact conn [{:db/id (d/tempid :db.part/user) :compute-cluster-config/name "cluster-2" :compute-cluster-config/template "cluster-2-template"}]))
    (let [db (d/db conn)
          current-configs (map #(let [e (d/entity db %)]
                                  {:name (:compute-cluster-config/name e)
                                   :template (:compute-cluster-config/template e)})
                               (d/q '[:find [?compute-cluster-config ...]
                                      :where
                                      [?compute-cluster-config :compute-cluster-config/name ?name]]
                                    db ))
          ]
      (println current-configs)
      )

    (let [db (d/db conn)
          zzz (d/q '[:find [?compute-cluster-config ...]
                     :in $ ?cluster-name
                     :where
                     [?compute-cluster-config :compute-cluster-config/name ?cluster-name]]
                   db "cluster-2")]

      (println zzz)

      (println @(d/transact conn [{:db/id (first zzz) :compute-cluster-config/name "cluster-2" :compute-cluster-config/template "cxxxluster-2-template"}]))
      (let [db (d/db conn)
            current-configs (map #(let [e (d/entity db %)]
                                    {:name (:compute-cluster-config/name e)
                                     :template (:compute-cluster-config/template e)
                                     :xxx (:db/id e)})
                                 (d/q '[:find [?compute-cluster-config ...]
                                        :where
                                        [?compute-cluster-config :compute-cluster-config/name ?name]]
                                      db))
            ]
        (println current-configs)
        ))

    ))