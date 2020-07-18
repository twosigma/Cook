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
(ns cook.test.rest.authorization
  (:require [clojure.test :refer :all]
            [cook.rest.authorization :as auth]))

(def test-job-owner "the-job-owner")
(def admin-user "admin")
(def test-job {:owner test-job-owner :item :job})

(def configfile-admins-settings {:authorization-fn 'cook.rest.authorization/configfile-admins-auth
                                 :admins #{admin-user "other-admin"}})

(def open-auth-settings {:authorization-fn 'cook.rest.authorization/open-auth})

(deftest open-auth-test
  (testing "open auth allows any user to do anything to any object"
    (is (true? (auth/open-auth {} "foo" :create test-job)))
    (is (true? (auth/open-auth {} "bar" :read test-job)))
    (is (true? (auth/open-auth {} "baz" :update test-job)))
    (is (true? (auth/open-auth {} "bazz" :get test-job)))
    (is (true? (auth/open-auth {} "bazzz" :retry test-job)))
    (is (true? (auth/open-auth {} "frob" :destroy test-job)))

    (is (true? (auth/open-auth {} test-job-owner :create test-job)))
    (is (true? (auth/open-auth {} admin-user :read test-job)))))

(deftest open-auth-no-job-killing-test
  (testing "allows any user to do anything to any object, except killing a job"
    (is (true? (auth/open-auth-no-job-killing {} "foo" :create test-job)))
    (is (true? (auth/open-auth-no-job-killing {} "bar" :read test-job)))
    (is (true? (auth/open-auth-no-job-killing {} "baz" :update test-job)))
    (is (true? (auth/open-auth-no-job-killing {} "bazz" :get test-job)))
    (is (true? (auth/open-auth-no-job-killing {} "bazzz" :retry test-job)))
    (is (true? (auth/open-auth-no-job-killing {} test-job-owner :create test-job)))
    (is (true? (auth/open-auth-no-job-killing {} admin-user :read test-job)))
    (is (true? (auth/open-auth-no-job-killing {} "foo" :delete {:item :not-a-job})))

    (is (false? (auth/open-auth-no-job-killing {} "frob" :delete test-job)))
    (is (false? (auth/open-auth-no-job-killing {} (:owner test-job) :delete test-job)))
    (is (false? (auth/open-auth-no-job-killing {} admin-user :delete test-job)))))

(deftest configfile-admins-auth
  (testing "ordinary users can manipulate their own objects"
    (is (true? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :create test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :read test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :update test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :get test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :retry test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :destroy test-job))))
  (testing "admins can manipulate other users' objects"
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :create test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :read test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :update test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :get test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :retry test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :destroy test-job))))
  (testing "admins can manipulate the Cook system itself"
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :create {:owner ::system :item ::system})))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :read {:owner ::system :item ::system})))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :update {:owner ::system :item ::system})))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :destroy test-job)))
    (is (true? (auth/configfile-admins-auth configfile-admins-settings admin-user :access test-job))))
  (testing "ordinary users cannot manipulate the Cook system itself"
    (is (false? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :create {:owner ::system :item ::system})))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :read {:owner ::system :item ::system})))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :update {:owner ::system :item ::system})))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :destroy {:owner ::system :item ::system})))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings test-job-owner :access {:owner ::system :item ::system}))))
  (testing "ordinary users cannot manipulate other users' objects"
    (is (false? (auth/configfile-admins-auth configfile-admins-settings "unauthorized-user" :create test-job)))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings "unauthorized-user" :read test-job)))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings "unauthorized-user" :update test-job)))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings "unauthorized-user" :get test-job)))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings "unauthorized-user" :retry test-job)))
    (is (false? (auth/configfile-admins-auth configfile-admins-settings "unauthorized-user" :destroy test-job)))))

(deftest test-configfile-admins-open-gets-allowed-users-auth
  (let [settings {:authorization-fn 'cook.rest.authorization/configfile-admins-open-gets-allowed-users-auth
                  :admins #{"sally" "joe"}
                  :allow #{"alice" "bob"}}
        authorized? #(auth/configfile-admins-open-gets-allowed-users-auth settings %1 %2 %3)]

    (testing "allowed users can manipulate their own objects"
      (is (true? (authorized? "alice" :create {:owner "alice" :item :job})))
      (is (true? (authorized? "alice" :read {:owner "alice" :item :job})))
      (is (true? (authorized? "alice" :update {:owner "alice" :item :job})))
      (is (true? (authorized? "alice" :get {:owner "alice" :item :job})))
      (is (true? (authorized? "alice" :retry {:owner "alice" :item :job})))
      (is (true? (authorized? "alice" :destroy {:owner "alice" :item :job}))))

    (testing "non-allowed users cannot manipulate their own objects"
      (is (false? (authorized? "bill" :create {:owner "bill" :item :job})))
      (is (false? (authorized? "bill" :read {:owner "bill" :item :job})))
      (is (false? (authorized? "bill" :update {:owner "bill" :item :job})))
      (is (false? (authorized? "bill" :get {:owner "bill" :item :job})))
      (is (false? (authorized? "bill" :retry {:owner "bill" :item :job})))
      (is (false? (authorized? "bill" :destroy {:owner "bill" :item :job}))))

    (testing "admins can manipulate other users' objects"
      (is (true? (authorized? "sally" :create {:owner "bill" :item :job})))
      (is (true? (authorized? "sally" :read {:owner "bill" :item :job})))
      (is (true? (authorized? "sally" :update {:owner "bill" :item :job})))
      (is (true? (authorized? "sally" :get {:owner "bill" :item :job})))
      (is (true? (authorized? "sally" :retry {:owner "bill" :item :job})))
      (is (true? (authorized? "sally" :destroy {:owner "bill" :item :job}))))

    (testing "non-admins cannot manipulate other users' objects"
      (is (false? (authorized? "alice" :create {:owner "bill" :item :job})))
      (is (false? (authorized? "alice" :read {:owner "bill" :item :job})))
      (is (false? (authorized? "alice" :update {:owner "bill" :item :job})))
      (is (false? (authorized? "alice" :retry {:owner "bill" :item :job})))
      (is (false? (authorized? "alice" :destroy {:owner "bill" :item :job}))))

    (testing "admins can manipulate the Cook system itself"
      (is (true? (authorized? "joe" :create {:owner ::system :item ::system})))
      (is (true? (authorized? "joe" :read {:owner ::system :item ::system})))
      (is (true? (authorized? "joe" :update {:owner ::system :item ::system})))
      (is (true? (authorized? "joe" :destroy {:owner ::system :item ::system})))
      (is (true? (authorized? "joe" :access {:owner ::system :item ::system}))))

    (testing "non-admins cannot manipulate the Cook system itself"
      (is (false? (authorized? "bob" :create {:owner ::system :item ::system})))
      (is (false? (authorized? "bob" :read {:owner ::system :item ::system})))
      (is (false? (authorized? "bob" :update {:owner ::system :item ::system})))
      (is (false? (authorized? "bob" :destroy {:owner ::system :item ::system})))
      (is (false? (authorized? "bob" :access {:owner ::system :item ::system}))))

    (testing "allowed users can :get anything"
      (is (true? (authorized? "bob" :get {})))
      (is (true? (authorized? "bob" :get {:owner "bill" :item :job})))
      (is (true? (authorized? "bob" :get {:owner "anyone" :item :anything}))))

    (testing "non-allowed users cannot :get anything"
      (is (false? (authorized? "bill" :get {})))
      (is (false? (authorized? "bill" :get {:owner "bill" :item :job})))
      (is (false? (authorized? "bill" :get {:owner "anyone" :item :anything}))))))

(deftest is-authorized
  (testing "is-authorized? selects the auth function defined in the settings correctly"
    (is (true? (auth/is-authorized? open-auth-settings "foo" :create nil test-job)))
    (is (true? (auth/is-authorized? open-auth-settings "bar" :read nil test-job)))
    (is (true? (auth/is-authorized? open-auth-settings "baz" :update nil test-job)))
    (is (true? (auth/is-authorized? open-auth-settings "frob" :destroy nil test-job)))

    (is (false? (auth/is-authorized? configfile-admins-settings "foo" :create nil test-job)))
    (is (false? (auth/is-authorized? configfile-admins-settings "bar" :read nil test-job)))
    (is (false? (auth/is-authorized? configfile-admins-settings "baz" :update nil test-job)))
    (is (false? (auth/is-authorized? configfile-admins-settings "frob" :destroy nil test-job)))

    (is (true? (auth/is-authorized? configfile-admins-settings test-job-owner :create nil test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings test-job-owner :read nil test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings test-job-owner :update nil test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings test-job-owner :destroy nil test-job)))

    (is (true? (auth/is-authorized? configfile-admins-settings admin-user :create nil test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings admin-user :read nil test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings admin-user :update nil test-job)))
    (is (true? (auth/is-authorized? configfile-admins-settings admin-user :destroy nil test-job)))))
