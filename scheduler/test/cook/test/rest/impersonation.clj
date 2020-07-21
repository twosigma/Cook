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
(ns cook.test.rest.impersonation
  (:require [clojure.test :refer :all]
            [cook.rest.authorization :as auth]
            [cook.rest.impersonation :as imp]))

(deftest impersonated-is-authorized
  (let [test-job-owner "the-job-owner"
        test-job {:owner test-job-owner :item :job}
        impersonator-user "the-impersonator"
        admin-user "admin"
        auth-settings {:authorization-fn 'cook.rest.authorization/configfile-admins-auth-open-gets
                       :admins #{admin-user "other-admin"}}
        is-authorized? (imp/impersonation-authorized-wrapper auth/is-authorized? auth-settings)]

    (testing "is-authorized? function performs correctly with impersonated users"

      ; random users can't touch other's job
      (is (not (is-authorized? "foo" :create nil test-job)))
      (is (not (is-authorized? "bar" :read nil test-job)))
      (is (not (is-authorized? "baz" :update nil test-job)))
      (is (not (is-authorized? "frob" :destroy nil test-job)))

      ; but random gets are OK
      (is (true? (is-authorized? "froo" :get impersonator-user test-job)))

      ; job owner can modify their own job
      (is (true? (is-authorized? test-job-owner :create nil test-job)))
      (is (true? (is-authorized? test-job-owner :read nil test-job)))
      (is (true? (is-authorized? test-job-owner :update nil test-job)))
      (is (true? (is-authorized? test-job-owner :destroy nil test-job)))

      ; admin can modify other's job
      (is (true? (is-authorized? admin-user :create nil test-job)))
      (is (true? (is-authorized? admin-user :read nil test-job)))
      (is (true? (is-authorized? admin-user :update nil test-job)))
      (is (true? (is-authorized? admin-user :destroy nil test-job)))

      ; admin can read and write user quotas
      (is (true? (is-authorized? admin-user :get nil {:item :quota})))
      (is (true? (is-authorized? admin-user :create nil {:item :quota})))
      (is (true? (is-authorized? admin-user :update nil {:item :quota})))
      (is (true? (is-authorized? admin-user :destroy nil {:item :quota})))

      ; admin can read and write user shares
      (is (true? (is-authorized? admin-user :get nil {:item :share})))
      (is (true? (is-authorized? admin-user :create nil {:item :share})))
      (is (true? (is-authorized? admin-user :update nil {:item :share})))
      (is (true? (is-authorized? admin-user :destroy nil {:item :share})))

      ; admin can read user usage
      (is (true? (is-authorized? admin-user :get nil {:item :usage})))

      ; admin can read queue endpoint
      (is (true? (is-authorized? admin-user :read nil {:item :queue})))

      ; impersonated random users can't touch other's job
      (is (not (is-authorized? "foo" :create impersonator-user test-job)))
      (is (not (is-authorized? "bar" :read impersonator-user test-job)))
      (is (not (is-authorized? "baz" :update impersonator-user test-job)))
      (is (not (is-authorized? "frob" :destroy impersonator-user test-job)))

      ; but random impersonated gets are OK
      (is (true? (is-authorized? "froo" :get impersonator-user test-job)))

      ; impersonated job owner can modify their own job
      (is (true? (is-authorized? test-job-owner :create impersonator-user test-job)))
      (is (true? (is-authorized? test-job-owner :read impersonator-user test-job)))
      (is (true? (is-authorized? test-job-owner :update impersonator-user test-job)))
      (is (true? (is-authorized? test-job-owner :destroy impersonator-user test-job)))

      ; impersonated admin can modify other's job
      (is (true? (is-authorized? admin-user :create impersonator-user test-job)))
      (is (true? (is-authorized? admin-user :read impersonator-user test-job)))
      (is (true? (is-authorized? admin-user :update impersonator-user test-job)))
      (is (true? (is-authorized? admin-user :destroy impersonator-user test-job)))

      ; impersonated admin can read--but not write--user quotas
      (is (true? (is-authorized? admin-user :get impersonator-user {:item :quota})))
      (is (not (is-authorized? admin-user :create impersonator-user {:item :quota})))
      (is (not (is-authorized? admin-user :update impersonator-user {:item :quota})))
      (is (not (is-authorized? admin-user :destroy impersonator-user {:item :quota})))

      ; impersonated admin can read--but not write--user shares
      (is (true? (is-authorized? admin-user :get impersonator-user {:item :share})))
      (is (not (is-authorized? admin-user :create impersonator-user {:item :share})))
      (is (not (is-authorized? admin-user :update impersonator-user {:item :share})))
      (is (not (is-authorized? admin-user :destroy impersonator-user {:item :share})))

      ; impersonated admin can read user usage
      (is (true? (is-authorized? admin-user :get impersonator-user {:item :usage})))

      ; impersonated admin can't read queue endpoint
      (is (not (is-authorized? admin-user :read impersonator-user {:item :queue}))))))
