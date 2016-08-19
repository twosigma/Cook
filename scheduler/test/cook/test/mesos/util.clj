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
(ns cook.test.mesos.util
 (:use clojure.test)
 (:require [cook.mesos.util :as util]
           [cook.test.mesos.schema :refer (restore-fresh-database! create-dummy-job create-dummy-instance)]
           [datomic.api :as d :refer (q db)]))

(deftest test-get-pending-job-ents
  (let [uri "datomic:mem://test-get-pending-job-ents"
        conn (restore-fresh-database! uri)]
    (create-dummy-job conn :user "u1" :job-state :job.state/waiting)
    (create-dummy-job conn :user "u1" :job-state :job.state/running)
    (create-dummy-job conn :user "u2" :job-state :job.state/waiting)
    (create-dummy-job conn :user "u1" :job-state :job.state/waiting)
    (is (= 3 (count (util/get-pending-job-ents (db conn)))))))

(deftest test-filter-sequential
  ; Same as filter
  (is (util/filter-sequential (fn [state x]
                            [state (even? x)])
                          {}
                          (range 10))
      (filter even? (range 10)))
  ;; Check lazy
  (is (take 100 (util/filter-sequential (fn [state x]
                            [state (even? x)])
                          {}
                          (range)))
      (take 100 (filter even? (range)))
      )
  ;; Check with state
  ;; Take first 100 odd numbers. Take even numbers after first 100
  (is (take 200
            (util/filter-sequential
              (fn [state x]
                (let [state' (merge-with + state {:even (mod (inc x) 2) ;if x even this is 1
                                                  :odd (mod x 2) ;if x odd, this is 1
                                                  })]
                  [state' (or (and (even? x) (> (:even state) 100))
                              (and (not (even? x)) (< (:odd state) 100)))]))
                                    {:even 0
                                     :odd 0}
                                    (range)))
      (take 200
            (concat (take 100 (filter odd? (range)))
                    (drop 100 (filter even? (range)))))))

(comment (run-tests))
