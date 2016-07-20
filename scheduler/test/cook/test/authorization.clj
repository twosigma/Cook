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
(ns cook.test.authorization
 (:use clojure.test)
 (:require [cook.authorization :as auth]
           [cook.mesos.api :as api]))

(def test-job-owner "the-job-owner")
(def admin-user "admin")
(def test-job (api/map->Job {:name "foo"
                             :uuid #uuid  "26719da8-394f-44f9-9e6d-8a17500f5111"
                             :command "echo hello"
                             :priority 10
                             :max-retries 2
                             :max-runtime 100
                             :cpus 1.0
                             :mem 512.0
                             :user test-job-owner
                             }))

(def configfile-admins-settings {:authorization-fn 'cook.authorization/configfile-admins-auth
                                 :admins #{ admin-user "other-admin" }})

(def open-auth-settings {:authorization-fn 'cook.authorization/open-auth})


(deftest open-auth-test
  (testing "open auth allows any user to do anything to any object"
    (is (true? (auth/open-auth {} "foo" :create test-job)))
    (is (true? (auth/open-auth {} "bar" :read test-job)))
    (is (true? (auth/open-auth {} "baz" :update test-job)))
    (is (true? (auth/open-auth {} "frob" :destroy test-job)))

    (is (true? (auth/open-auth {} test-job-owner :create test-job)))
    (is (true? (auth/open-auth {} admin-user :read test-job)))))


(deftest configfile-admins-auth
  (testing "ordinary users can manipulate their own objects"
    (is (true? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :create test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :read test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :update test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :destroy test-job))))
  (testing "admins can manipulate other users' objects"
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :create test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :read test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :update test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :destroy test-job))))
  (testing "ordinary users cannot manipulate other users' objects"
    (is (false? (auth/configfile-admins-auth configfile-admins-settings "unauthorized-user" :create test-job)))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings "unauthorized-user" :read test-job)))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings "unauthorized-user" :update test-job)))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings "unauthorized-user" :destroy test-job)))))


(deftest is-authorized
  (testing "is-authorized? selects the auth function defined in the settings correctly"
    (is (true? (auth/is-authorized? open-auth-settings "foo" :create test-job)))
    (is (true? (auth/is-authorized? open-auth-settings "bar" :read test-job)))
    (is (true? (auth/is-authorized? open-auth-settings "baz" :update test-job)))
    (is (true? (auth/is-authorized? open-auth-settings "frob" :destroy test-job)))


    (is (false? (auth/is-authorized? configfile-admins-settings "foo" :create test-job)))
    (is (false? (auth/is-authorized? configfile-admins-settings "bar" :read test-job)))
    (is (false? (auth/is-authorized? configfile-admins-settings "baz" :update test-job)))
    (is (false? (auth/is-authorized? configfile-admins-settings "frob" :destroy test-job)))

    (is (true? (auth/is-authorized? configfile-admins-settings test-job-owner :create test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings test-job-owner :read test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings test-job-owner :update test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings test-job-owner :destroy test-job)))

    (is (true? (auth/is-authorized? configfile-admins-settings admin-user :create test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings admin-user :read test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings admin-user :update test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings admin-user :destroy test-job)))))
