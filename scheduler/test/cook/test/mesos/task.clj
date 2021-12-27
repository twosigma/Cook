(ns cook.test.mesos.task
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [cook.compute-cluster :as cc]
            [cook.mesos.task :as task]
            [cook.test.postgres]
            [cook.scheduler.offer :as offer]
            [cook.test.testutil :as tu]
            [datomic.api :as d]
            [mesomatic.types :as mtypes])
  (:import (java.util UUID)))

(use-fixtures :once cook.test.postgres/with-pg-db)

(deftest test-resources-by-role
  (let [
        offers [{:resources [{:name "mem", :type :value-scalar, :scalar 100.0, :role "cook"}
                             {:name "mem", :type :value-scalar, :scalar 50.0, :role "*"}
                             {:name "cpus", :type :value-scalar, :scalar 6.0, :role "cook"}
                             {:name "cpus", :type :value-scalar, :scalar 4.0, :role "*"}
                             {:name "ports",
                              :type :value-ranges,
                              :ranges [{:begin 1000 :end 2000} {:begin 3000 :end 4000}],
                              :role "cook"}
                             {:name "ports",
                              :type :value-ranges,
                              :ranges [{:begin 5000 :end 6000} {:begin 7000 :end 8000}],
                              :role "*"}]}
                {:resources [{:name "mem", :type :value-scalar, :scalar 30.0, :role "cook"}
                             {:name "mem", :type :value-scalar, :scalar 20.0, :role "*"}
                             {:name "cpus", :type :value-scalar, :scalar 3.0, :role "cook"}
                             {:name "cpus", :type :value-scalar, :scalar 2.0, :role "*"}
                             {:name "ports",
                              :type :value-ranges,
                              :ranges [{:begin 11000 :end 12000} {:begin 13000 :end 14000}],
                              :role "cook"}
                             {:name "ports",
                              :type :value-ranges,
                              :ranges [{:begin 15000 :end 16000} {:begin 17000 :end 18000}],
                              :role "*"}]
                 }]
        results (task/resources-by-role offers)]

    (is (= (results "mem")
           {"cook" 130.0, "*" 70.0}))

    (is (= (results "cpus")
           {"cook" 9.0, "*" 6.0}))

    (is (= (results "ports")
           {"cook" [{:begin 1000, :end 2000}
                    {:begin 3000, :end 4000}
                    {:begin 11000, :end 12000}
                    {:begin 13000, :end 14000}],
            "*" [{:begin 5000, :end 6000}
                 {:begin 7000, :end 8000}
                 {:begin 15000, :end 16000}
                 {:begin 17000, :end 18000}]}))))

(deftest test-take-resources
  (let [resources {"mem" {"cook" 50.0 "*" 125.0}}
        result (task/take-resources resources "mem" 75.0)]
    (is (= (:amount-still-needed result) 0.0))
    (is (= (:mesos-messages result) [
                                     {:name "mem"
                                      :type :value-scalar
                                      :role "cook"
                                      :scalar 50.0}
                                     {:name "mem"
                                      :type :value-scalar
                                      :role "*"
                                      :scalar 25.0}
                                     ])
        (is (= (:remaining-resources result) {"cook" 0.0 "*" 100.0})))))

(deftest test-range-contains?
  (let [r {:begin 100 :end 200}]
    (is (true? (task/range-contains? 100 r)))
    (is (true? (task/range-contains? 200 r)))
    (is (false? (task/range-contains? 99 r)))
    (is (false? (task/range-contains? 250 r)))))

(deftest test-role-containing-port
  (let [avail {"*" [{:begin 201 :end 202}] "cook" [{:begin 203 :end 204}]}]
    (is (= (task/role-containing-port avail 201) "*"))
    (is (= (task/role-containing-port avail 204) "cook"))
    (is (nil? (task/role-containing-port avail 999)))))

(deftest test-take-ports
  (let [resources {"*" [{:begin 201 :end 202}] "cook" [{:begin 203 :end 204}]}
        result (task/take-ports resources [201 203])]
    (is (= result [{:name "ports" :type :value-ranges :role "*" :ranges [{:begin 201 :end 201}]}
                   {:name "ports" :type :value-ranges :role "cook" :ranges [{:begin 203 :end 203}]}]))))

(deftest test-take-scalar-resources-for-task
  (let [available {"cpus" {"cook" 8.0 "*" 6.0}
                   "mem" {"cook" 800.0 "*" 700.0}}
        task {:task-request (reify com.netflix.fenzo.TaskRequest
                              (getScalarRequests [_] {:cpus 12.0 :mem 900.0}))}
        result (task/take-all-scalar-resources-for-task available task)]
    (is (= (:remaining-resources result)
           {"cpus" {"cook" 0.0 "*" 2.0}
            "mem" {"cook" 0.0 "*" 600.0}}))
    (is (= (:mesos-messages result)
           [{:name "cpus", :type :value-scalar, :role "cook", :scalar 8.0}
            {:name "cpus", :type :value-scalar, :role "*", :scalar 4.0}
            {:name "mem", :type :value-scalar, :role "cook", :scalar 800.0}
            {:name "mem", :type :value-scalar, :role "*", :scalar 100.0}]))))

(deftest test-add-scalar-resources-to-task-infos
  (let [available {"cpus" {"cook" 8.0 "*" 6.0}
                   "mem" {"cook" 800.0 "*" 700.0}}
        tasks [{:task-request (reify com.netflix.fenzo.TaskRequest
                                (getScalarRequests [_] {:cpus 12.0 :mem 900.0}))}]
        results (task/add-scalar-resources-to-task-infos available tasks)]
    (is (= (-> results first :scalar-resource-messages)
           [{:name "cpus", :type :value-scalar, :role "cook", :scalar 8.0}
            {:name "cpus", :type :value-scalar, :role "*", :scalar 4.0}
            {:name "mem", :type :value-scalar, :role "cook", :scalar 800.0}
            {:name "mem", :type :value-scalar, :role "*", :scalar 100.0}]))
    (is (= (-> results first :resources)
           (-> tasks first :resources)))))

(deftest test-task-info->mesos-message
  (let [framework-id "4425e656-2278-4f91-b1e4-9a2e942e6e81"
        task-info->mesos-message-wrap (partial task/task-info->mesos-message framework-id)]
    (testing "task-info->mesos-message"
      (let [command "sleep 26; exit 0"
            task {:name "yaiqlzwhfm_andalucien_4425e656-2278-4f91-b1e4-9a2e942e6e82",
                  :slave-id {:value "foobar"},
                  :task-id "4425e656-2278-4f91-b1e4-9a2e942e6e82",
                  :scalar-resource-messages [{:name "mem", :type :value-scalar, :scalar 623.0, :role "cook"}
                                             {:name "cpus", :type :value-scalar, :scalar 1.0, :role "cook"}]
                  :ports-resource-messages [{:name "ports" :type :value-ranges :role "cook" :ranges [{:begin 31000 :end 31002}]}]
                  :command {:value command,
                            :environment {"MYENV" "VAR"},
                            :user "andalucien",
                            :uris [{:value "http://www.yahoo.com"
                                    :executable true
                                    :cache true
                                    :extract true}]}
                  :labels {"foo" "bar", "doo" "dar"},
                  :data (.getBytes "string-data" "UTF-8")
                  :role "4425e656-2278-4f91-b1e4-9a2e942e6e82",
                  :num-ports 0,
                  :ports-assigned [31000 31001]
                  :resources {:mem 623.0
                              :cpus 1.0
                              :ports [{:begin 31000, :end 31002}]}}
            ;; roundrip to and from Mesos protobuf to validate clojure data format
            msg (->> task
                     task-info->mesos-message-wrap
                     (mtypes/->pb :TaskInfo)
                     mtypes/pb->data)]

        (testing "name-and-ids"
          (is (= (:name msg) (:name task)))
          (is (= (-> msg :slave-id :value) (-> task :slave-id :value)))
          (is (= (-> msg :task-id :value) (:task-id task))))

        (testing "resources"
          ;; offers have the same resources structure as tasks so we can reuse (offer-resource-values)
          (is (= (offer/offer-resource-scalar msg "mem") (-> task :resources :mem)))
          (is (= (offer/offer-resource-scalar msg "cpus") (-> task :resources :cpus)))
          (is (= (->> (offer/offer-resource-ranges msg "ports") first :begin)
                 (-> task :resources :ports first :begin)))
          (is (= (->> (offer/offer-resource-ranges msg "ports") first :end)
                 (-> task :resources :ports first :end)))
          (is (= (->> msg :resources (map :role))
                 (map :role (into (:scalar-resource-messages task)
                                  (:ports-resource-messages task))))))

        (testing "labels"
          (is (= (->> msg :labels :labels (filter #(= (:key %) "foo")) first :value) "bar")))


        (testing "command-executor"
          (let [command-executor-task (assoc task :data (.getBytes (pr-str {:instance "5"}) "UTF-8")
                                                  :executor-key :command-executor)
                command-executor-msg (->> command-executor-task
                                          task-info->mesos-message-wrap
                                          (mtypes/->pb :TaskInfo)
                                          mtypes/pb->data)]
            ;; Check custom executor built correctly
            (is (= (:instance (edn/read-string (String. (.toByteArray (:data command-executor-msg))))) "5"))
            (let [command-executor-msg-cmd (-> command-executor-msg :command)
                  command-executor-task-cmd (-> command-executor-task :command)
                  command-executor-msg-uri (-> command-executor-msg-cmd :uris first)
                  command-executor-task-uri (-> command-executor-task-cmd :uris first)]
              (is (= (:user command-executor-msg-cmd) (:user command-executor-task-cmd)))
              (is (= (:value command-executor-msg-cmd) (:value command-executor-task-cmd)))
              (is (= (:cache command-executor-msg-uri) (:cache command-executor-task-uri)))
              (is (= (:executable command-executor-msg-uri) (:executable command-executor-task-uri)))
              (is (= (:extract command-executor-msg-uri) (:extract command-executor-task-uri)))
              (is (= (:value command-executor-msg-uri) (:value command-executor-task-uri))))))

        (doseq [executor-key [:cook-executor :custom-executor]]
          (testing (str "common-fields-" executor-key)
            (let [task (assoc task :executor-key executor-key)
                  ;; roundrip to and from Mesos protobuf to validate clojure data format
                  msg (->> task
                           task-info->mesos-message-wrap
                           (mtypes/->pb :TaskInfo)
                           mtypes/pb->data)]
              (let [msg-cmd (-> msg :executor :command)
                    task-cmd (:command task)
                    msg-uri (-> msg-cmd :uris first)
                    task-uri (-> task-cmd :uris first)]
                (is (str/blank? (-> msg :command :uris first)))
                (is (str/blank? (-> msg :command :user)))
                (is (str/blank? (-> msg :command :value)))
                (is (= (:user msg-cmd) (:user task-cmd)))
                (is (= (:value msg-cmd) (:value task-cmd)))
                (is (= (:cache msg-uri) (:cache task-uri)))
                (is (= (:executable msg-uri) (:executable task-uri)))
                (is (= (:extract msg-uri) (:extract task-uri)))
                (is (= (:value msg-uri) (:value task-uri))))
              ;; the following assertions don't use the roundtrip because Mesomatic currently
              ;; has a bug and doesn't convert env var info back into clojure data.
              ;; It's not a problem for Cook, except for the purposes of this unit test.
              (let [msg-env (-> task task-info->mesos-message-wrap :executor :command :environment)]
                (is (= (-> msg-env :variables first :name) "MYENV"))
                (is (= (-> msg-env :variables first :value) "VAR"))))))

        (testing "cook-executor"
          (let [cook-executor-task (assoc task :data (.getBytes command "UTF-8")
                                               :executor-key :cook-executor)
                cook-executor-msg (->> cook-executor-task
                                       task-info->mesos-message-wrap
                                       (mtypes/->pb :TaskInfo)
                                       mtypes/pb->data)]
            ;; Check custom executor built correctly
            (is (= command (String. (.toByteArray (:data cook-executor-msg)))))
            (is (= (-> cook-executor-msg :executor :command :value) (-> task :command :value)))
            (is (= (-> cook-executor-msg :executor :executor-id :value) (:task-id task)))
            (is (= (-> cook-executor-msg :executor :framework-id :value) framework-id))
            (is (= (-> cook-executor-msg :executor :name) task/cook-executor-name))
            (is (= (-> cook-executor-msg :executor :source) task/cook-executor-source))))

        (testing "custom-executor"
          (let [custom-executor-task (assoc task :data (.getBytes (pr-str {:instance "5"}) "UTF-8")
                                                 :executor-key :custom-executor)
                custom-executor-msg (->> custom-executor-task
                                         task-info->mesos-message-wrap
                                         (mtypes/->pb :TaskInfo)
                                         mtypes/pb->data)]
            ;; Check custom executor built correctly
            (is (= (:instance (edn/read-string (String. (.toByteArray (:data custom-executor-msg))))) "5"))
            (is (= (-> custom-executor-msg :executor :command :value) (-> task :command :value)))
            (is (= (-> custom-executor-msg :executor :executor-id :value) (:task-id task)))
            (is (= (-> custom-executor-msg :executor :framework-id :value) framework-id))
            (is (= (-> custom-executor-msg :executor :name) task/custom-executor-name))
            (is (= (-> custom-executor-msg :executor :source) task/custom-executor-source))))

        (let [container {:docker {:image "a-docker-image"
                                  :force-pull-image false
                                  :network "HOST"
                                  :parameters [{:key "user" :value "100:5"}]
                                  :port-mapping [{:host-port 0
                                                  :container-port 1
                                                  :protocol "tcp"}
                                                 {:host-port 1
                                                  :container-port 2
                                                  :protocol "udp"}]}
                         :hostname "test.docker.hostname"
                         :type "DOCKER"
                         :volumes [{:container-path "/var/lib/sss"
                                    :host-path "/var/lib/sss"
                                    :mode "RW"}]}
              expected-container (-> (update container :docker assoc :network :docker-network-host)
                                     (update :docker #(clojure.set/rename-keys % {:port-mapping :port-mappings}))
                                     (assoc-in [:docker :port-mappings 0 :host-port]
                                               (first (:ports-assigned task)))
                                     (assoc-in [:docker :port-mappings 1 :host-port]
                                               (second (:ports-assigned task)))
                                     (assoc :type :container-type-docker)
                                     (update-in [:volumes 0] assoc :mode :volume-rw)
                                     (assoc :mesos nil))]

          (testing "container-command"
            (let [container-executor-task (assoc task :container container
                                                      :data (.getBytes (pr-str {:instance "5"}) "UTF-8")
                                                      :executor-key :container-command-executor)
                  container-executor-msg (->> container-executor-task
                                              task-info->mesos-message-wrap
                                              (mtypes/->pb :TaskInfo)
                                              mtypes/pb->data)]
              ;; Check container executor built correctly
              (is (= (:instance (edn/read-string (String. (.toByteArray (:data container-executor-msg))))) "5"))
              (is (= (-> container-executor-msg :command :value) (-> task :command :value)))
              (is (str/blank? (-> container-executor-msg :executor :command :value)))
              (is (str/blank? (-> container-executor-msg :executor :executor-id :value)))
              (is (str/blank? (-> container-executor-msg :executor :framework-id :value)))
              (is (= (-> container-executor-msg :executor :name) ""))
              (is (str/blank? (-> container-executor-msg :executor :source)))
              (is (= expected-container (->> container-executor-task task-info->mesos-message-wrap :container)))
              (is (nil? (->> container-executor-task task-info->mesos-message-wrap :executor :container)))))

          (testing "container-executor"
            (let [container-executor-task (assoc task :container container
                                                      :data (.getBytes (pr-str {:instance "5"}) "UTF-8")
                                                      :executor-key :container-executor)
                  container-executor-msg (->> container-executor-task
                                              task-info->mesos-message-wrap
                                              (mtypes/->pb :TaskInfo)
                                              mtypes/pb->data)]
              ;; Check container executor built correctly
              (is (= (:instance (edn/read-string (String. (.toByteArray (:data container-executor-msg))))) "5"))
              (is (str/blank? (-> container-executor-msg :command :value)))
              (is (= (-> container-executor-msg :executor :command :value) (-> task :command :value)))
              (is (= (-> container-executor-msg :executor :executor-id :value) (:task-id task)))
              (is (= (-> container-executor-msg :executor :framework-id :value) framework-id))
              (is (= (-> container-executor-msg :executor :name) task/custom-executor-name))
              (is (= (-> container-executor-msg :executor :source) task/custom-executor-source))
              (is (nil? (->> container-executor-task task-info->mesos-message-wrap :container)))
              (is (= expected-container (->> container-executor-task task-info->mesos-message-wrap :executor :container)))))

          (testing "container-cook-executor"
            (let [container-executor-task (assoc task :container container
                                                      :data (.getBytes (pr-str {:instance "5"}) "UTF-8")
                                                      :executor-key :container-cook-executor)
                  container-executor-msg (->> container-executor-task
                                              task-info->mesos-message-wrap
                                              (mtypes/->pb :TaskInfo)
                                              mtypes/pb->data)]
              ;; Check container executor built correctly
              (is (= (:instance (edn/read-string (String. (.toByteArray (:data container-executor-msg))))) "5"))
              (is (str/blank? (-> container-executor-msg :command :value)))
              (is (= (-> container-executor-msg :executor :command :value) (-> task :command :value)))
              (is (= (-> container-executor-msg :executor :executor-id :value) (:task-id task)))
              (is (= (-> container-executor-msg :executor :framework-id :value) framework-id))
              (is (= (-> container-executor-msg :executor :name) task/cook-executor-name))
              (is (= (-> container-executor-msg :executor :source) task/cook-executor-source))
              (is (nil? (->> container-executor-task task-info->mesos-message-wrap :container)))
              (is (= expected-container (->> container-executor-task task-info->mesos-message-wrap :executor :container))))))))))

(deftest test-job->task-metadata
  (tu/setup)
  (let [uri "datomic:mem://test-job-task-metadata"
        conn (tu/restore-fresh-database! uri)
        executor {:command "./cook-executor"
                  :log-level "INFO"
                  :max-message-length 512
                  :default-progress-regex-string "regex-string"
                  :progress-sample-interval-ms 1000
                  :uri {:cache true
                        :executable true
                        :extract false
                        :value "file:///path/to/cook-executor"}}
        mesos-run-as-user nil]
    (with-redefs [cook.config/executor-config (constantly executor)
                  cc/use-cook-executor? (constantly true)
                  cc/container-defaults (constantly {})]
      (testing "custom-executor with simple job"
        (let [task-id (str (UUID/randomUUID))
              job (tu/create-dummy-job conn :user "test-user" :job-state :job.state/running :command "run-my-command")
              db (d/db conn)
              job-ent (d/entity db job)
              framework-id {:value "framework-id"}
              environment {"COOK_INSTANCE_NUM" "0"
                           "COOK_INSTANCE_UUID" task-id
                           "COOK_JOB_CPUS" "1.0"
                           "COOK_JOB_MEM_MB" "10.0"
                           "COOK_JOB_UUID" (-> job-ent :job/uuid str)}]

          (testing "mesos-run-as-user absent"
            (let [task-metadata (task/job->task-metadata nil mesos-run-as-user job-ent task-id)]
              (is (= {:command {:value "run-my-command", :environment environment, :user "test-user", :uris []}
                      :container nil
                      :environment environment
                      :executor :executor/custom
                      :executor-key :custom-executor
                      :labels {}
                      :name (format "dummy_job_%s_%s" (:job/user job-ent) task-id)
                      :num-ports 0
                      :resources {:cpus 1.0, :mem 10.0}
                      :task-id task-id}
                     (dissoc task-metadata :data)))
              (is (= (pr-str {:instance "0"}) (-> task-metadata :data (String. "UTF-8"))))))

          (testing "mesos-run-as-user present"
            (let [mesos-run-as-user "mesos-run-as-user"
                  task-metadata (task/job->task-metadata nil mesos-run-as-user job-ent task-id)]
              (is (= {:command {:value "run-my-command", :environment environment, :user mesos-run-as-user, :uris []}
                      :container nil
                      :environment environment
                      :executor :executor/custom
                      :executor-key :custom-executor
                      :labels {}
                      :name (format "dummy_job_%s_%s" (:job/user job-ent) task-id)
                      :num-ports 0
                      :resources {:cpus 1.0, :mem 10.0}
                      :task-id task-id}
                     (dissoc task-metadata :data)))
              (is (= (pr-str {:instance "0"}) (-> task-metadata :data (String. "UTF-8"))))))))

      (testing "custom-executor with simple job in a group"
        (let [task-id (str (UUID/randomUUID))
              group-ent-id (tu/create-dummy-group conn)
              job (tu/create-dummy-job conn :command "run-my-command" :group group-ent-id
                                       :job-state :job.state/running :user "test-user")
              db (d/db conn)
              job-ent (d/entity db job)
              group-ent (d/entity db group-ent-id)
              framework-id {:value "framework-id"}
              environment {"COOK_INSTANCE_NUM" "0"
                           "COOK_INSTANCE_UUID" task-id
                           "COOK_JOB_CPUS" "1.0"
                           "COOK_JOB_GROUP_UUID" (-> group-ent :group/uuid str)
                           "COOK_JOB_MEM_MB" "10.0"
                           "COOK_JOB_UUID" (-> job-ent :job/uuid str)}
              task-metadata (task/job->task-metadata nil mesos-run-as-user job-ent task-id)]
          (is (= {:command {:value "run-my-command", :environment environment, :user "test-user", :uris []}
                  :container nil
                  :environment environment
                  :executor :executor/custom
                  :executor-key :custom-executor
                  :labels {}
                  :name (format "dummy_job_%s_%s" (:job/user job-ent) task-id)
                  :num-ports 0
                  :resources {:cpus 1.0, :mem 10.0}
                  :task-id task-id}
                 (dissoc task-metadata :data)))
          (is (= (pr-str {:instance "0"}) (-> task-metadata :data (String. "UTF-8"))))))

      (testing "explicit custom-executor with simple job"
        (let [task-id (str (UUID/randomUUID))
              job (tu/create-dummy-job conn :user "test-user" :job-state :job.state/running :command "run-my-command"
                                       :custom-executor? true)
              db (d/db conn)
              job-ent (d/entity db job)
              framework-id {:value "framework-id"}
              environment {"COOK_INSTANCE_NUM" "0"
                           "COOK_INSTANCE_UUID" task-id
                           "COOK_JOB_CPUS" "1.0"
                           "COOK_JOB_MEM_MB" "10.0"
                           "COOK_JOB_UUID" (-> job-ent :job/uuid str)}
              task-metadata (task/job->task-metadata nil mesos-run-as-user job-ent task-id)]
          (is (= {:command {:value "run-my-command", :environment environment, :user "test-user", :uris []}
                  :container nil
                  :environment environment
                  :executor :executor/custom
                  :executor-key :custom-executor
                  :labels {}
                  :name (format "dummy_job_%s_%s" (:job/user job-ent) task-id)
                  :num-ports 0
                  :resources {:cpus 1.0, :mem 10.0}
                  :task-id task-id}
                 (dissoc task-metadata :data)))
          (is (= (pr-str {:instance "0"}) (-> task-metadata :data (String. "UTF-8"))))))

      (testing "cook-executor with simple job"
        (let [task-id (str (UUID/randomUUID))
              job (tu/create-dummy-job conn :user "test-user" :job-state :job.state/running :command "run-my-command"
                                       :custom-executor? false :executor :executor/cook)
              db (d/db conn)
              job-ent (d/entity db job)
              framework-id {:value "framework-id"}
              environment {"COOK_INSTANCE_NUM" "0"
                           "COOK_INSTANCE_UUID" task-id
                           "COOK_JOB_CPUS" "1.0"
                           "COOK_JOB_MEM_MB" "10.0"
                           "COOK_JOB_UUID" (-> job-ent :job/uuid str)
                           "EXECUTOR_LOG_LEVEL" (:log-level executor)
                           "EXECUTOR_MAX_MESSAGE_LENGTH" (:max-message-length executor)
                           "PROGRESS_REGEX_STRING" (:default-progress-regex-string executor)
                           "PROGRESS_SAMPLE_INTERVAL_MS" (:progress-sample-interval-ms executor)}
              task-metadata (task/job->task-metadata nil mesos-run-as-user job-ent task-id)]
          (is (= {:command {:environment environment
                            :uris [(:uri executor)]
                            :user "test-user"
                            :value (:command executor)}
                  :container nil
                  :environment environment
                  :executor :executor/cook
                  :executor-key :cook-executor
                  :labels {}
                  :name (format "dummy_job_%s_%s" (:job/user job-ent) task-id)
                  :num-ports 0
                  :resources {:cpus 1.0, :mem 10.0}
                  :task-id task-id}
                 (dissoc task-metadata :data)))
          (is (= (json/write-str {"command" "run-my-command"})
                 (-> task-metadata :data (String. "UTF-8"))))))

      (testing "cook-executor with simple job in a group"
        (let [task-id (str (UUID/randomUUID))
              group-ent-id (tu/create-dummy-group conn)
              job (tu/create-dummy-job conn :command "run-my-command" :custom-executor? false :executor :executor/cook
                                       :group group-ent-id :gpus 1000 :job-state :job.state/running :user "test-user")
              db (d/db conn)
              group-ent (d/entity db group-ent-id)
              job-ent (d/entity db job)
              framework-id {:value "framework-id"}
              environment {"COOK_INSTANCE_NUM" "0"
                           "COOK_INSTANCE_UUID" task-id
                           "COOK_JOB_CPUS" "1.0"
                           "COOK_JOB_GROUP_UUID" (-> group-ent :group/uuid str)
                           "COOK_JOB_GPUS" "1000.0"
                           "COOK_JOB_MEM_MB" "10.0"
                           "COOK_JOB_UUID" (-> job-ent :job/uuid str)
                           "EXECUTOR_LOG_LEVEL" (:log-level executor)
                           "EXECUTOR_MAX_MESSAGE_LENGTH" (:max-message-length executor)
                           "PROGRESS_REGEX_STRING" (:default-progress-regex-string executor)
                           "PROGRESS_SAMPLE_INTERVAL_MS" (:progress-sample-interval-ms executor)}
              task-metadata (task/job->task-metadata nil mesos-run-as-user job-ent task-id)]
          (is (= {:command {:environment environment
                            :uris [(:uri executor)]
                            :user "test-user"
                            :value (:command executor)}
                  :container nil
                  :environment environment
                  :executor :executor/cook
                  :executor-key :cook-executor
                  :labels {}
                  :name (format "dummy_job_%s_%s" (:job/user job-ent) task-id)
                  :num-ports 0
                  :resources {:cpus 1.0, :mem 10.0}
                  :task-id task-id}
                 (dissoc task-metadata :data)))
          (is (= (json/write-str {"command" "run-my-command"})
                 (-> task-metadata :data (String. "UTF-8"))))))

      (testing "command-executor with simple job"
        (let [task-id (str (UUID/randomUUID))
              job (tu/create-dummy-job conn :user "test-user" :job-state :job.state/running :command "run-my-command"
                                       :custom-executor? false)
              db (d/db conn)
              job-ent (d/entity db job)
              framework-id {:value "framework-id"}
              environment {"COOK_INSTANCE_NUM" "0"
                           "COOK_INSTANCE_UUID" task-id
                           "COOK_JOB_CPUS" "1.0"
                           "COOK_JOB_MEM_MB" "10.0"
                           "COOK_JOB_UUID" (-> job-ent :job/uuid str)}
              task-metadata (task/job->task-metadata nil mesos-run-as-user job-ent task-id)]
          (is (= {:command {:environment environment
                            :uris []
                            :user "test-user"
                            :value "run-my-command"}
                  :container nil
                  :environment environment
                  :executor :executor/mesos
                  :executor-key :command-executor
                  :labels {}
                  :name (format "dummy_job_%s_%s" (:job/user job-ent) task-id)
                  :num-ports 0
                  :resources {:cpus 1.0, :mem 10.0}
                  :task-id task-id}
                 (dissoc task-metadata :data)))
          (is (= (pr-str {:instance "0"}) (-> task-metadata :data (String. "UTF-8"))))))

      (testing "container-executor with simple job"
        (let [task-id (str (UUID/randomUUID))
              job (tu/create-dummy-job conn
                                       :command "run-my-command"
                                       :container {:container/docker {:docker/image "a-docker-image"
                                                                      :docker/network "HOST"
                                                                      :docker/parameters []}
                                                   :container/type "DOCKER"
                                                   :container/volumes []}
                                       :custom-executor? true
                                       :job-state :job.state/running
                                       :user "test-user")
              db (d/db conn)
              job-ent (d/entity db job)
              framework-id {:value "framework-id"}
              environment {"COOK_INSTANCE_NUM" "0"
                           "COOK_INSTANCE_UUID" task-id
                           "COOK_JOB_CPUS" "1.0"
                           "COOK_JOB_MEM_MB" "10.0"
                           "COOK_JOB_UUID" (-> job-ent :job/uuid str)}
              task-metadata (task/job->task-metadata nil mesos-run-as-user job-ent task-id)]
          (is (= {:command {:environment environment
                            :uris []
                            :user "test-user"
                            :value "run-my-command"}
                  :container {:docker {:image "a-docker-image"
                                       :network "HOST"}
                              :type "DOCKER"}
                  :environment environment
                  :executor :executor/custom
                  :executor-key :container-executor
                  :labels {}
                  :name (format "dummy_job_%s_%s" (:job/user job-ent) task-id)
                  :num-ports 0
                  :resources {:cpus 1.0, :mem 10.0}
                  :task-id task-id}
                 (dissoc task-metadata :data)))
          (is (= (pr-str {:instance "0"}) (-> task-metadata :data (String. "UTF-8"))))))

      (testing "container-command with simple job"
        (let [task-id (str (UUID/randomUUID))
              job (tu/create-dummy-job conn
                                       :command "run-my-command"
                                       :container {:container/docker {:docker/image "a-docker-image"
                                                                      :docker/network "HOST"
                                                                      :docker/parameters []}
                                                   :container/type "DOCKER"
                                                   :container/volumes []}
                                       :custom-executor? false
                                       :job-state :job.state/running
                                       :memory 200
                                       :user "test-user")
              db (d/db conn)
              job-ent (d/entity db job)
              framework-id {:value "framework-id"}
              environment {"COOK_INSTANCE_NUM" "0"
                           "COOK_INSTANCE_UUID" task-id
                           "COOK_JOB_CPUS" "1.0"
                           "COOK_JOB_MEM_MB" "200.0"
                           "COOK_JOB_UUID" (-> job-ent :job/uuid str)}
              task-metadata (task/job->task-metadata nil mesos-run-as-user job-ent task-id)]
          (is (= {:command {:environment environment
                            :uris []
                            :user "test-user"
                            :value "run-my-command"}
                  :container {:docker {:image "a-docker-image"
                                       :network "HOST"}
                              :type "DOCKER"}
                  :environment environment
                  :executor :executor/mesos
                  :executor-key :container-command-executor
                  :labels {}
                  :name (format "dummy_job_%s_%s" (:job/user job-ent) task-id)
                  :num-ports 0
                  :resources {:cpus 1.0, :mem 200.0}
                  :task-id task-id}
                 (dissoc task-metadata :data)))
          (is (= (pr-str {:instance "0"}) (-> task-metadata :data (String. "UTF-8"))))))

      (testing "container-command with simple job and cook executor"
        (let [task-id (str (UUID/randomUUID))
              job (tu/create-dummy-job conn
                                       :command "run-my-command"
                                       :container {:container/docker {:docker/image "a-docker-image"
                                                                      :docker/network "HOST"
                                                                      :docker/parameters []}
                                                   :container/type "DOCKER"
                                                   :container/volumes []}
                                       :custom-executor? false
                                       :executor :executor/cook
                                       :job-state :job.state/running
                                       :memory 200
                                       :user "test-user")
              db (d/db conn)
              job-ent (d/entity db job)
              framework-id {:value "framework-id"}
              environment {"COOK_INSTANCE_NUM" "0"
                           "COOK_INSTANCE_UUID" task-id
                           "COOK_JOB_CPUS" "1.0"
                           "COOK_JOB_MEM_MB" "200.0"
                           "COOK_JOB_UUID" (-> job-ent :job/uuid str)
                           "EXECUTOR_LOG_LEVEL" (:log-level executor)
                           "EXECUTOR_MAX_MESSAGE_LENGTH" (:max-message-length executor)
                           "PROGRESS_REGEX_STRING" (:default-progress-regex-string executor)
                           "PROGRESS_SAMPLE_INTERVAL_MS" (:progress-sample-interval-ms executor)}
              task-metadata (task/job->task-metadata nil mesos-run-as-user job-ent task-id)]
          (is (= {:command {:environment environment
                            :uris [(:uri executor)]
                            :user "test-user"
                            :value (:command executor)}
                  :container {:docker {:image "a-docker-image"
                                       :network "HOST"}
                              :type "DOCKER"}
                  :environment environment
                  :executor :executor/cook
                  :executor-key :container-cook-executor
                  :labels {}
                  :name (format "dummy_job_%s_%s" (:job/user job-ent) task-id)
                  :num-ports 0
                  :resources {:cpus 1.0, :mem 200.0}
                  :task-id task-id}
                 (dissoc task-metadata :data)))
          (is (= (json/write-str {"command" "run-my-command"})
                 (-> task-metadata :data (String. "UTF-8")))))))))


(deftest test-use-cook-executor?
  (testing "empty entity and config"
    (let [job-ent {}
          executor-config {}]
      (with-redefs [cook.config/executor-config (constantly executor-config)]
        (is (not (task/use-cook-executor? job-ent))))))

  (testing "custom-executor not configured"
    (let [job-ent {}
          executor-config {:command "cook-executor"
                           :portion 0.25
                           :retry-limit 1}]
      (with-redefs [cook.config/executor-config (constantly executor-config)]
        (is (not (task/use-cook-executor? job-ent))))))

  (testing "custom-executor retry-limit not configured"
    (let [job-ent {:job/custom-executor true}
          executor-config {:command "cook-executor"
                           :portion 0.25}]
      (with-redefs [cook.config/executor-config (constantly executor-config)]
        (is (not (task/use-cook-executor? job-ent))))))

  (testing "custom-executor enabled"
    (let [job-ent {:job/custom-executor true}
          executor-config {:command "cook-executor"
                           :portion 0.25
                           :retry-limit 1}]
      (with-redefs [cook.config/executor-config (constantly executor-config)]
        (is (not (task/use-cook-executor? job-ent))))))

  (testing "custom-executor enabled and cook-executor enabled [faulty state]"
    (let [job-ent {:job/custom-executor true
                   :job/executor :executor/cook}
          executor-config {:command "cook-executor"
                           :portion 0.25
                           :retry-limit 1}]
      (with-redefs [cook.config/executor-config (constantly executor-config)]
        (is (not (task/use-cook-executor? job-ent))))))

  (testing "custom-executor disabled and cook-executor enabled"
    (let [job-ent {:job/custom-executor false
                   :job/executor :executor/cook}
          executor-config {:command "cook-executor"
                           :portion 0.25
                           :retry-limit 1}]
      (with-redefs [cook.config/executor-config (constantly executor-config)]
        (is (task/use-cook-executor? job-ent)))))

  (testing "custom-executor disabled and cook-executor disabled"
    (let [job-ent {:job/custom-executor false
                   :job/executor :executor/mesos}
          executor-config {:command "cook-executor"
                           :portion 0.25
                           :retry-limit 1}]
      (with-redefs [cook.config/executor-config (constantly executor-config)]
        (is (not (task/use-cook-executor? job-ent))))))

  (testing "custom-executor disabled and coin toss favorable"
    (let [job-uuid (str (UUID/randomUUID))
          job-ent {:job/custom-executor false
                   :job/uuid job-uuid}
          executor-config {:command "cook-executor"
                           :portion 0.25
                           :retry-limit 1}]
      (with-redefs [hash (fn [obj] (is (= job-uuid obj)) 10)
                    cook.config/executor-config (constantly executor-config)]
        (is (task/use-cook-executor? job-ent)))))

  (testing "custom-executor disabled and coin toss unfavorable"
    (let [job-uuid (str (UUID/randomUUID))
          job-ent {:job/custom-executor false
                   :job/uuid job-uuid}
          executor-config {:command "cook-executor"
                           :portion 0.25
                           :retry-limit 1}]
      (with-redefs [hash (fn [obj] (is (= job-uuid obj)) 90)
                    cook.config/executor-config (constantly executor-config)]
        (is (not (task/use-cook-executor? job-ent))))))

  (testing "custom-executor disabled, coin toss favorable with single instance"
    (let [instance-1 {:instance/executor-id "foo"}
          job-uuid (str (UUID/randomUUID))
          job-ent {:job/custom-executor false
                   :job/instance [instance-1]
                   :job/uuid job-uuid}]
      (with-redefs [hash (fn [obj] (is (= job-uuid obj)) 10)]
        (with-redefs [cook.config/executor-config (constantly {:command "cook-executor"
                                                               :portion 0.25
                                                               :retry-limit "not-a-number"})]
          (is (not (task/use-cook-executor? job-ent))))
        (with-redefs [cook.config/executor-config (constantly {:command "cook-executor"
                                                               :portion 0.25
                                                               :retry-limit 0})]
          (is (task/use-cook-executor? job-ent)))
        (with-redefs [cook.config/executor-config (constantly {:command "cook-executor"
                                                               :portion 0.25
                                                               :retry-limit 1})]
          (is (not (task/use-cook-executor? job-ent))))
        (with-redefs [cook.config/executor-config (constantly {:command "cook-executor"
                                                               :portion 0.25
                                                               :retry-limit 2})]
          (is (task/use-cook-executor? job-ent))))))

  (testing "custom-executor disabled, coin toss favorable with multiple instances"
    (let [instance-1 {:instance/executor-id "foo"}
          instance-2 {:instance/executor-id "bar"}
          instance-3 {:instance/executor-id "baz"}
          job-uuid (str (UUID/randomUUID))
          job-ent {:job/custom-executor false
                   :job/instance [instance-1 instance-2 instance-3]
                   :job/uuid job-uuid}]
      (with-redefs [hash (fn [obj] (is (= job-uuid obj)) 10)]
        (with-redefs [cook.config/executor-config (constantly {:command "cook-executor"
                                                               :portion 0.25
                                                               :retry-limit "not-a-number"})]
          (is (not (task/use-cook-executor? job-ent))))
        (with-redefs [cook.config/executor-config (constantly {:command "cook-executor"
                                                               :portion 0.25
                                                               :retry-limit 2})]
          (is (not (task/use-cook-executor? job-ent))))
        (with-redefs [cook.config/executor-config (constantly {:command "cook-executor"
                                                               :portion 0.25
                                                               :retry-limit 3})]
          (is (not (task/use-cook-executor? job-ent))))
        (with-redefs [cook.config/executor-config (constantly {:command "cook-executor"
                                                               :portion 0.25
                                                               :retry-limit 4})]
          (is (task/use-cook-executor? job-ent)))))))

(deftest test-merge-container-defaults
  (testing "does not add docker info if missing"
    (with-redefs [cc/container-defaults (constantly {:docker {:parameters {"foo" "bar"}}})]
      (is (= {:mesos {:image "baz"}} (task/merge-container-defaults nil {:mesos {:image "baz"}})))))
  (testing "adds volumes even when missing"
    (let [volumes [{:container-path "/foo"
                    :host-path "/foo"}]]
      (with-redefs [cc/container-defaults (constantly {:volumes volumes})]
        (is (= {:mesos {:image "baz"}
                :volumes volumes}
               (task/merge-container-defaults nil {:mesos {:image "baz"}}))))))
  (testing "merges volumes"
    (with-redefs [cc/container-defaults (constantly {:volumes [{:host-path "/h/a"
                                                                :container-path "/c/a"
                                                                :mode "rw"}
                                                               {:host-path "/mnt/app/data"
                                                                :mode "r"}]})]
      (is (= nil (task/merge-container-defaults nil nil)))
      (is (= {:docker {:image "foo"}
              :volumes [{:host-path "/h/a"
                         :container-path "/c/a"
                         :mode "rw"}
                        {:host-path "/mnt/app/data"
                         :mode "r"}]}
             (task/merge-container-defaults nil {:docker {:image "foo"}})))
      (is (= {:docker {:image "foo"}
              :volumes [{:host-path "/mnt/app"
                         :mode "rw"}
                        {:host-path "/h/a"
                         :container-path "/c/a"
                         :mode "rw"}]}
             (task/merge-container-defaults nil
                                            {:docker {:image "foo"}
                                             :volumes [{:host-path "/mnt/app"
                                                        :mode "rw"}]})))
      (is (= {:docker {:image "foo"}
              :volumes [{:host-path "/diff/a"
                         :container-path "/c/a"}
                        {:host-path "/mnt/app/data"
                         :mode "r"}]}
             (task/merge-container-defaults nil
                                            {:docker {:image "foo"}
                                             :volumes [{:host-path "/diff/a"
                                                        :container-path "/c/a"}]})))
      (is (= {:docker {:image "foo"}
              :volumes [{:host-path "/diff/a"
                         :container-path "/c/a/b"}
                        {:host-path "/h/a"
                         :container-path "/c/a"
                         :mode "rw"}
                        {:host-path "/mnt/app/data"
                         :mode "r"}]}
             (task/merge-container-defaults nil
                                            {:docker {:image "foo"}
                                             :volumes [{:host-path "/diff/a"
                                                        :container-path "/c/a/b"}]}))))))
