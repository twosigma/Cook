(ns cook.test.mesos.task
  (:use clojure.test)
  (:require [clojure.edn :as edn]
            [mesomatic.types :as mtypes]
            [cook.mesos.task :as task]
            [cook.mesos.scheduler :as sched]))

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
    (is (= result  [{:name "ports" :type :value-ranges :role "*" :ranges [{:begin 201 :end 201}]}
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
  (let [task {:name "yaiqlzwhfm_andalucien_4425e656-2278-4f91-b1e4-9a2e942e6e82",
              :slave-id {:value "foobar"},
              :task-id "4425e656-2278-4f91-b1e4-9a2e942e6e82",
              :scalar-resource-messages [{:name "mem", :type :value-scalar, :scalar 623.0, :role "cook"}
                                        {:name "cpus", :type :value-scalar, :scalar 1.0, :role "cook"}]
              :ports-resource-messages [{:name "ports" :type :value-ranges :role "cook" :ranges [{:begin 31000 :end 31002}]}]
              :executor-key :command
              :command {:value "sleep 26; exit 0",
                        :environment {"MYENV" "VAR"},
                        :user "andalucien",
                        :uris [{:value "http://www.yahoo.com"
                                :executable true
                                :cache true
                                :extract true}]}
              :labels {"foo" "bar", "doo" "dar"},
              :data (.getBytes (pr-str {:instance "5"}) "UTF-8"),
              :framework-id {:value "4425e656-2278-4f91-b1e4-9a2e942e6e81"}
              :role "4425e656-2278-4f91-b1e4-9a2e942e6e82",
              :num-ports 0,
              :resources {:mem 623.0
                          :cpus 1.0
                          :ports [{:begin 31000, :end 31002}]}}
        container {:type "DOCKER"
                   :volumes [{:container-path "/var/lib/sss"
                              :host-path "/var/lib/sss"
                              :mode "RW"}]
                   :docker {:image "nvidia/cuda"
                            :network "HOST"
                            :force-pull-image false
                            :parameters [{:key "user" :value "100:5"}]
                            :port-mapping [{:host-port 0
                                            :container-port 1
                                            :protocol "tcp"}]}}
        custom-executor-task (assoc task :executor-key :executor)
        container-task (assoc task :container container)
        ;; roundrip to and from Mesos protobuf to validate clojure data format
        msg (->> task
                 task/task-info->mesos-message
                 (mtypes/->pb :TaskInfo)
                 mtypes/pb->data)
        custom-executor-msg (->> custom-executor-task
                                 task/task-info->mesos-message
                                 (mtypes/->pb :TaskInfo)
                                 mtypes/pb->data)
        ;; TODO: Check values of container-msg.
        ;; mesomatic doesn't do conversion to map for containerinfo so holding off for now
        container-msg (->> container-task
                           task/task-info->mesos-message
                           (mtypes/->pb :TaskInfo)
                           mtypes/pb->data)]

    (is (= (:name msg) (:name task)))
    (is (= (-> msg :slave-id :value) (-> task :slave-id :value)))
    (is (= (-> msg :task-id :value) (:task-id task)))

    ;; offers have the same resources structure as tasks so we can reuse (offer-resource-values)
    (is (= (sched/offer-resource-scalar msg "mem") (-> task :resources :mem)))
    (is (= (sched/offer-resource-scalar msg "cpus") (-> task :resources :cpus)))
    (is (= (->> (sched/offer-resource-ranges msg "ports") first :begin)
           (-> task :resources :ports first :begin)))
    (is (= (->> (sched/offer-resource-ranges msg "ports") first :end)
           (-> task :resources :ports first :end)))
    (is (= (->> msg :resources (map :role))
           (map :role (concat (:scalar-resource-messages task)
                              (:ports-resource-messages task)))))

    (is (= (:instance (edn/read-string (String. (.toByteArray (:data msg))))) "5"))

    (is (= (->> msg :labels :labels (filter #(= (:key %) "foo")) first :value) "bar"))

    (let [msg-cmd (:command msg)
          task-cmd (:command task)
          msg-uri (-> msg-cmd :uris first)
          task-uri (-> task-cmd :uris first)]
      (is (= (:value msg-cmd) (:value task-cmd)))
      (is (= (:user msg-cmd) (:user task-cmd)))
      (is (= (:value msg-uri) (:value task-uri)))
      (is (= (:executable msg-uri) (:executable task-uri)))
      (is (= (:cache msg-uri) (:cache task-uri)))
      (is (= (:extract msg-uri) (:extract task-uri))))

    ;; the following assertions don't use the roundtrip because Mesomatic currently
    ;; has a bug and doesn't convert env var info back into clojure data.
    ;; It's not a problem for Cook, except for the purposes of this unit test.
    (let [msg-env (-> task task/task-info->mesos-message :command :environment)]
      (is (= (-> msg-env :variables first :name) "MYENV"))
      (is (= (-> msg-env :variables first :value) "VAR")))

    ;; Check custom executor built correctly
    (is (= (-> custom-executor-msg :executor :executor-id :value) (:task-id task)))
    (is (= (-> custom-executor-msg :executor :framework-id :value) (-> task :framework-id :value)))
    (is (= (-> custom-executor-msg :executor :name) task/custom-executor-name))
    (is (= (-> custom-executor-msg :executor :source) task/custom-executor-source))
    (is (= (-> custom-executor-msg :executor :command :value)
           (-> task :command :value)))))
