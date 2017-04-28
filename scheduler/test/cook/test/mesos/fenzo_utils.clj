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
(ns cook.test.mesos.fenzo-utils
  (:use clojure.test
        lucid.core.java)
  (:require [cook.mesos.fenzo-utils :as fenzo])
  (import com.netflix.fenzo.SimpleAssignmentResult
          com.netflix.fenzo.AssignmentFailure
          com.netflix.fenzo.ConstraintFailure
          com.netflix.fenzo.VMResource))

;; Fenzo is aware of other resources as well; just limiting it to ones
;; Cook will usually encounter
(def keyword->VMResource
  {:cpus VMResource/CPU
   :mem VMResource/Memory
   :gpus VMResource/Other
   :ports VMResource/Ports
   :disk VMResource/Disk})

(defn assignment-failure
  [resource-type]
  (let [asking (+ 1.0 (rand 100))
        used (rand 1000)
        available (- asking 1.0)
        message (name resource-type)]
    (AssignmentFailure. (keyword->VMResource resource-type)
                        asking used available message)))

(defn assignment-result
  [constraint-name resources-lacking]
  (SimpleAssignmentResult.
   (map assignment-failure resources-lacking)
   (when constraint-name
     (ConstraintFailure. constraint-name (str constraint-name " was not satisfied")))
   ))

(deftest test-summarize-placement-failures
  (testing "starting from empty accumulator"
    (is (= (fenzo/summarize-placement-failure {} (assignment-result nil []))
           {}))

    (is (= (fenzo/summarize-placement-failure
            {}
            (assignment-result nil [:ports]))
           {:resources {"ports" 1}}))

    (is (= (fenzo/summarize-placement-failure
            {}
            (assignment-result "novel_host_constraint" []))
           {:constraints {"novel_host_constraint" 1}}))

    (is (= (fenzo/summarize-placement-failure
            {}
            (assignment-result "novel_host_constraint" [:cpus :mem]))
           {:constraints {"novel_host_constraint" 1}
            :resources {"cpus" 1
                        "mem" 1}})))

  (testing "reducing multiple failure results"
    (is (= (reduce fenzo/summarize-placement-failure
                   {}
                   [(assignment-result "novel_host_constraint" [])
                    (assignment-result "other_constraint" [:cpus :mem])
                    (assignment-result nil [:cpus])
                    (assignment-result nil [:gpus])
                    (assignment-result nil [:ports :disk])
                    (assignment-result "novel_host_constraint" [:mem :cpus :ports])])
           {:constraints {"novel_host_constraint" 2
                          "other_constraint" 1}
            :resources {"cpus" 3
                        "gpus" 1
                        "mem" 2
                        "ports" 2
                        "disk" 1}}))))
