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
;(ns cook.mesos.simulator
;  (:import [org.apache.mesos.SchedulerDriver])
;  (:use [incanter core stats charts])
;  (:require [cook.mesos.scheduler :as sched]
;            [cook.mesos.util :as util]
;            [cook.mesos.rebalancer :as rebalancer]
;            [cook.mesos.schema]
;            [clj-time.core :as time]
;            [clj-time.coerce :as coerce]
;            [clj-time.periodic :as time-period]
;            [clojure.data.priority-map :as pm]
;            [datomic.api :as d :refer (q)]
;            [metatransaction.core :as mt]
;            [clj-mesos.scheduler :as mesos]
;            [swiss.arrows :refer :all]
;            [plumbing.graph :as graph]
;            [plumbing.core :refer (fnk)])
;  (:gen-class))
;
;;;
;;; First, we'll take a collection of jobs for the scheduler
;;; the jobs have a submit time, run time, and resource reqs
;;; we also have a collection of machines on the cluster, each with cpu and memory
;;;
;;; to test the scheduler, we'll make a timeseries of the events in the system:
;;; the fixed interval scheduling times, and the times of all job submissions
;;; there will be synthesized events as time goes on
;;;
;;; event types:
;;; 1) submit a job: in this case, we just transact the job at its submit time
;;; 2) do a scheduling step: first, we run the scoring function. Then, we use
;;;    a custom/modified version of commit-preemptions that figures out
;;;    what machines we can free up, and marks those machines' resources as freed
;;;    when we start a task, we register its expected completion event. When we kill
;;;    a task, we remove its expected completion event
;;; 3) task completion event: we mark the task and job as finished, and release the resources
;;;    back into the pool
;;;
;;;
;;; Analyses:
;;; - compute the total value of running tasks over time, with & without the runtime boost
;;; - compute the percentiles of the waiting times for all tasks in the queue
;;; - plot the realized shares of all users in the cluster over time
;;; - plot the fair shares of all users in the cluster over time
;;; - plot the communist shares of all users in the cluster over time
;;; - calculate the total wasted resources spent on preemptions
;;;
;
;;; for implementation, we'll use datomic for storing all data structures
;
;(def extra-schema
;  [{:db/id (d/tempid :db.part/db)
;    :db/doc "how many ms a task takes to run"
;    :db/ident :job/task-runtime
;    :db/valueType :db.type/long
;    :db/cardinality :db.cardinality/one
;    :db.install/_attribute :db.part/db}
;   {:db/id (d/tempid :db.part/db)
;    :db/doc "simset uuid"
;    :db/ident :sim-set/uuid
;    :db/valueType :db.type/uuid
;    :db/cardinality :db.cardinality/one
;    :db.install/_attribute :db.part/db}
;   {:db/id (d/tempid :db.part/db)
;    :db/doc "simset job"
;    :db/ident :sim-set/job
;    :db/valueType :db.type/ref
;    :db/cardinality :db.cardinality/many
;    :db.install/_attribute :db.part/db}
;   ;; Representing the machines on the cluster
;   {:db/id (d/tempid :db.part/db)
;    :db/ident :machine/name
;    :db/valueType :db.type/string
;    :db/unique :db.unique/identity
;    :db/cardinality :db.cardinality/one
;    :db.install/_attribute :db.part/db}
;   ;; Total resources on a machine
;   {:db/id (d/tempid :db.part/db)
;    :db/ident :machine/total-cpus
;    :db/valueType :db.type/double
;    :db/cardinality :db.cardinality/one
;    :db.install/_attribute :db.part/db}
;   {:db/id (d/tempid :db.part/db)
;    :db/ident :machine/total-mem
;    :db/valueType :db.type/double
;    :db/cardinality :db.cardinality/one
;    :db.install/_attribute :db.part/db}
;   ;; Resources curretly available on that machine
;   {:db/id (d/tempid :db.part/db)
;    :db/ident :machine/cpus
;    :db/valueType :db.type/double
;    :db/cardinality :db.cardinality/one
;    :db.install/_attribute :db.part/db}
;   {:db/id (d/tempid :db.part/db)
;    :db/ident :machine/mem
;    :db/valueType :db.type/double
;    :db/cardinality :db.cardinality/one
;    :db.install/_attribute :db.part/db}
;   ;; Global things we'd like to track
;   {:db/id (d/tempid :db.part/db)
;    :db/ident :global/start-time
;    :db/valueType :db.type/long
;    :db/cardinality :db.cardinality/one
;    :db.install/_attribute :db.part/db}
;   {:db/id (d/tempid :db.part/db)
;    :db/ident :global/end-time
;    :db/valueType :db.type/long
;    :db/cardinality :db.cardinality/one
;    :db.install/_attribute :db.part/db}
;   {:db/id (d/tempid :db.part/db)
;    :db/ident :global/stamp-time
;    :db/valueType :db.type/long
;    :db/cardinality :db.cardinality/one
;    :db.install/_attribute :db.part/db}
;   {:db/id (d/tempid :db.part/user)
;    :db/ident :global}
;   {:db/id (d/tempid :db.part/user)
;    :db/ident :atomic-inc
;    :db/fn #db/fn {:lang "clojure"
;                   :params [db e a v]
;                   :code
;                   (let [v' (a (d/entity db e))]
;                     [[:db/add e a (+ v' v)]])}}])
;
;(defn make-host-txn
;  [name cpus mem]
;  [{:db/id (d/tempid :db.part/user)
;    :machine/name name
;    :machine/total-cpus cpus
;    :machine/total-mem mem
;    :machine/cpus cpus
;    :machine/mem mem}])
;
;(defn make-job-txn
;  [simset-id {:keys [uuid user time simset-uuid runtime cpus mem]}]
;  (if simset-id
;    [{:db/id (d/tempid :db.part/user)
;      :job/state :job.state/waiting
;      :sim-set/_job simset-id
;      :job/resource [{:db/id (d/tempid :db.part/user)
;                      :resource/type :resource.type/cpus
;                      :resource/amount cpus}
;                     {:db/id (d/tempid :db.part/user)
;                      :resource/type :resource.type/mem
;                      :resource/amount mem}]
;      :job/uuid uuid
;      :job/submit-time time
;      :job/task-runtime runtime
;      :job/user user
;      :job/command "true"}]
;    (let [simset-tempid (d/tempid :db.part/user)
;          job-tempid (d/tempid :db.part/user)]
;      [{:db/id simset-tempid
;        :sim-set/uuid simset-uuid}
;       {:db/id (d/tempid :db.part/user)
;        :job/state :job.state/waiting
;        :sim-set/_job simset-tempid
;        :job/resource [{:db/id (d/tempid :db.part/user)
;                        :resource/type :resource.type/cpus
;                        :resource/amount cpus}
;                       {:db/id (d/tempid :db.part/user)
;                        :resource/type :resource.type/mem
;                        :resource/amount mem}]
;        :job/uuid uuid
;        :job/submit-time time
;        :job/task-runtime runtime
;        :job/user user
;        :job/command "true"}])))
;
;(defn init-db
;  "Puts schema into the freshly created db, with all the schema back in 1970"
;  [uri]
;  (d/delete-database uri)
;  (when-not (d/create-database uri)
;    (throw (ex-info "must only run on a clean db" {:uri uri})))
;  (println "Waiting for database creation...")
;  (let [conn (loop [conn nil]
;               (if conn
;                 conn
;                 (recur (try
;                          (d/connect uri)
;                          (catch Exception e
;                            (println "Database not created yet...")
;                            (Thread/sleep 5000)
;                            nil)))))]
;    (doseq [[t i] (mapv vector (conj cook.mesos.schema/work-item-schema
;                                     [{:db/id (d/tempid :db.part/user)
;                                       :share/user "default"
;                                       :share/resource [{:db/id (d/tempid :db.part/user)
;                                                         :resource/type :resource.type/mem
;                                                         :resource/amount 0.0}
;                                                        {:db/id (d/tempid :db.part/user)
;                                                         :resource/type :resource.type/cpus
;                                                         :resource/amount 0.0}]}])
;                        (range))]
;      (deref (d/transact conn (conj t
;                                    [:db/add (d/tempid :db.part/tx) :db/txInstant (java.util.Date. i)]))))
;    @(d/transact conn (conj extra-schema
;                            [:db/add (d/tempid :db.part/tx) :db/txInstant
;                             (java.util.Date. (inc (count cook.mesos.schema/work-item-schema)))]))
;    conn))
;
;(defn job-wait-time
;  "Takes a job and computes how long it was until it finished waiting"
;  [db j]
;  (let [;; This is the end of the waiting time
;        end (ffirst (q '[:find ?end
;                         :in $ ?j
;                         :where
;                         [?j :job/instance ?i]
;                         [?i :instance/status :instance.status/success]
;                         [?i :instance/start-time ?end]]
;                       db j))
;        start (ffirst (q '[:find ?start
;                           :in $ ?j
;                           :where
;                           [?j :job/submit-time ?start]]
;                         db j))]
;    (when end
;      (- (.getTime end) (.getTime start)))))
;
;(defn job-complete-time
;  "Takes a job and computes how long it was until it finished"
;  [db j]
;  (let [end (ffirst (q '[:find ?end
;                         :in $ ?j
;                         :where
;                         [?j :job/instance ?i]
;                         [?i :instance/status :instance.status/success]
;                         [?i :instance/end-time ?end]]
;                       db j))
;        start (ffirst (q '[:find ?start
;                           :in $ ?j
;                           :where
;                           [?j :job/submit-time ?start]]
;                         db j))]
;    (when end
;      (- (.getTime end) (.getTime start)))))
;
;(defn to-hhmmss
;  [ms]
;  (let [sec (mod (quot ms 1000) 60)
;        min (mod (quot ms (* 1000 60)) 60)
;        hr (quot ms (* 1000 60 60))]
;    (format "%02d:%02d:%02d" hr min sec)))
;
;(defn quantiles
;  [samples formatter & quantiles]
;  (let [s (sort samples)]
;    (into (sorted-map) (map (fn [q]
;                              (let [i (int (* q (count s)))]
;                                [q (formatter (nth s (min i (dec (count s)))))]))
;                            quantiles))))
;
;(defn job-slow-down-factor
;  "Takes a job and computes how many times worse the latency was than unrealistically perfect"
;  [db j]
;  (let [j-ent (d/entity db j)
;        submit-time (.getTime (:job/submit-time j-ent))
;        run-time (:job/task-runtime j-ent)
;        first-completed-instance (->> (:job/instance j-ent)
;                                      (filter #(= (:instance/status %) :instance.status/success))
;                                      (sort-by :instance/end-time)
;                                      first
;                                      :instance/end-time
;                                      .getTime)]
;    (double
;     (/ (- first-completed-instance submit-time)
;        run-time))))
;
;(defn sim-set->wait-time
;  "Takes an simset and computes the wait time of the latest piece"
;  [db sim-set]
;  (let [job-wait-time (partial job-wait-time db)
;        wait-time (or (->> (q '[:find ?j
;                                    :in $ ?sim-set
;                                    :where
;                                    [?sim-set :sim-set/job ?j]]
;                                  db sim-set)
;                               (map first)
;                               (map job-wait-time)
;                               (remove nil?)
;                               (apply max)))]
;    wait-time))
;
;(defn sim-set->complete-time
;  "Takes an simset and computes the wait time of the latest piece"
;  [db sim-set]
;  (let [job-complete-time (partial job-complete-time db)
;        complete-time (or (->> (q '[:find ?j
;                                    :in $ ?sim-set
;                                    :where
;                                    [?sim-set :sim-set/job ?j]]
;                                  db sim-set)
;                               (map first)
;                               (map job-complete-time)
;                               (apply max)))]
;    complete-time))
;
;(defn sim-set->slow-down-factor
;  "Takes a simset and computes how many times worse the latency was than unrealistically perfect"
;  [db sim-set]
;  (let [max-piece-runtime (->> (q '[:find ?j ?runtime
;                                   :in $ ?s
;                                   :where
;                                   [?s :sim-set/job ?j]
;                                   [?j :job/task-runtime ?runtime]]
;                                 db sim-set)
;                              (map second)
;                              (apply max))
;        sim-set-complete-time (sim-set->complete-time db sim-set)]
;    (double (/ sim-set-complete-time max-piece-runtime))))
;
;(defn sim-set-completed?
;  [db sim-set]
;  (let [total-pieces (ffirst (q '[:find (count ?j)
;                                  :in $ ?s
;                                  :where
;                                  [?s :sim-set/job ?j]
;                                  ]
;                                db sim-set))
;        finished-pieces (or (ffirst (q '[:find (count ?j)
;                                         :in $ ?s
;                                         :where
;                                         [?s :sim-set/job ?j]
;                                         [?j :job/state :job.state/completed]]
;                                       db sim-set))
;                            0)]
;    (= total-pieces finished-pieces)))
;
;(defn sim-set->submit-window
;  [db window-size-ms sim-set]
;  (let [submit-time->submit-window (fn [t] (- t (rem t window-size-ms)))
;        submit-time-ms (->> (q '[:find ?t
;                                 :in $ ?s
;                                 :where
;                                 [?s :sim-set/job ?j]
;                                 [?j :job/submit-time ?t]]
;                               db sim-set)
;                            (map (fn [[t]] (.getTime t)))
;                            (apply min))]
;    (submit-time->submit-window submit-time-ms)))
;
;(defn sim-set->info
;  [db sim-set]
;  (let [[uuid user] (first (q '[:find ?uuid ?user
;                                :in $ ?s
;                                :where
;                                [?s :sim-set/uuid ?uuid]
;                                [?s :sim-set/job ?j]
;                                [?j :job/user ?user]]
;                              db sim-set))
;
;        total-jobs-count (ffirst (q '[:find (count ?j)
;                                      :in $ ?s
;                                      :where
;                                      [?s :sim-set/job ?j]]
;                                    db sim-set))
;
;        completed-jobs-count (or
;                              (ffirst (q '[:find (count ?j)
;                                           :in $ ?s
;                                           :where
;                                           [?s :sim-set/job ?j]
;                                           [?j :job/state :job.state/completed]]
;                                         db sim-set))
;                              0)
;
;        total-preempted-instance-count (or
;                                        (ffirst (q '[:find (count ?i)
;                                                     :in $ ?s
;                                                     :where
;                                                     [?s :sim-set/job ?j]
;                                                     [?j :job/instance ?i]
;                                                     [?i :instance/status :instance.status/failed]]
;                                                   db sim-set))
;                                        0)
;
;        completed? (= total-jobs-count completed-jobs-count)
;
;        job-infos (->>
;                   (q '[:find ?j ?uuid ?submit-time ?runtime ?mem ?cpus
;                        :in $ ?s
;                        :where
;                        [?s :sim-set/job ?j]
;                        [?j :job/uuid ?uuid]
;                        [?j :job/submit-time ?submit-time]
;                        [?j :job/task-runtime ?runtime]
;                        [?j :job/resource ?r1]
;                        [?r1 :resource/type :resource.type/cpus]
;                        [?r1 :resource/amount ?cpus]
;                        [?j :job/resource ?r2]
;                        [?r2 :resource/type :resource.type/mem]
;                        [?r2 :resource/amount ?mem]]
;                      db sim-set)
;                   (map (fn [[_ uuid submit-time runtime mem cpus]]
;                          {:uuid uuid
;                           :submit-time submit-time
;                           :runtime runtime
;                           :mem mem
;                           :cpus cpus})))
;
;        submit-time (apply min-key #(.getTime %) (map :submit-time job-infos))
;        runtime (apply max (map :runtime job-infos))
;        mem (->> job-infos
;                 (map :mem)
;                 (reduce +))
;        cpus (->> job-infos
;                  (map :cpus)
;                  (reduce +))
;
;        successful-instance-infos (->>
;                                   (q '[:find ?i ?start-time ?end-time
;                                        :in $ ?s
;                                        :where
;                                        [?s :sim-set/job ?j]
;                                        [?j :job/instance ?i]
;                                        [?i :instance/status :instance.status/success]
;                                        [?i :instance/start-time ?start-time]
;                                        [?i :instance/end-time ?end-time]]
;                                      db sim-set)
;                                   (map (fn [[_ start-time end-time]]
;                                          {:start-time start-time
;                                           :end-time end-time})))
;
;        start-time (if completed? (apply max-key #(.getTime %) (map :start-time successful-instance-infos)) nil)
;        end-time (if completed? (apply max-key #(.getTime %) (map :end-time successful-instance-infos)) nil)]
;    {:uuid uuid
;     :user user
;     :completed? completed?
;     :submit-time submit-time
;     :start-time start-time
;     :end-time end-time
;     :runtime runtime
;     :mem mem
;     :cpus cpus
;     :total-jobs total-jobs-count
;     :completed-jobs completed-jobs-count
;     :preemption-count total-preempted-instance-count}))
;
;(defn job->info
;  [db j]
;  (let [[uuid user state submit-time runtime mem cpus]
;        (first
;         (q '[:find ?uuid ?user ?state ?submit-time ?runtime ?mem ?cpus
;              :in $ ?j
;              :where
;              [?j :job/uuid ?uuid]
;              [?j :job/user ?user]
;              [?j :job/submit-time ?submit-time]
;              [?j :job/state ?s]
;              [?s :db/ident ?state]
;              [?j :job/task-runtime ?runtime]
;              [?j :job/resource ?r1]
;              [?r1 :resource/type :resource.type/cpus]
;              [?r1 :resource/amount ?cpus]
;              [?j :job/resource ?r2]
;              [?r2 :resource/type :resource.type/mem]
;              [?r2 :resource/amount ?mem]]
;            db j))]
;    {:uuid uuid
;     :user user
;     :completed? (= state :job.state/completed)
;     :submit-time submit-time
;     :runtime runtime
;     :mem mem
;     :cpus cpus}))
;
;(defn gen-report
;  [db]
;  (let [ms->minute (fn [ms] (double (/ ms 60000.0)))
;        stamps-txn-t (sort (->> (q '[:find ?t
;                                     :where
;                                     [_ :global/stamp-time _ ?t]]
;                                   db)
;                                (map (fn [[x]] x))))
;
;        duration-hours 24
;
;        total-jobs (ffirst (q '[:find (count ?j)
;                                :where
;                                [?j :job/state _]]
;                              db))
;
;        total-req-hours (let [[total-cpus-hours total-mem-hours]
;                              (->> db
;                                   (q '[:find ?j ?runtime ?cpus ?mem
;                                        :where
;                                        [?j :job/task-runtime ?runtime]
;                                        [?j :job/resource ?r1]
;                                        [?r1 :resource/type :resource.type/cpus]
;                                        [?r1 :resource/amount ?cpus]
;                                        [?j :job/resource ?r2]
;                                        [?r2 :resource/type :resource.type/mem]
;                                        [?r2 :resource/amount ?mem]])
;                                   (reduce (fn [[cpus-hours mem-hours] [_ runtime cpus mem]]
;                                             [(+ cpus-hours (/ (* runtime cpus) 3600000.0))
;                                              (+ mem-hours (/ (* runtime mem) 3600000.0))])
;                                           [0.0 0.0]))]
;                          {:cpus total-cpus-hours :mem total-mem-hours})
;
;        total-req-hours-by-user (->> db
;                                     (q '[:find ?j ?u ?runtime ?cpus ?mem
;                                          :where
;                                          [?j :job/user ?u]
;                                          [?j :job/task-runtime ?runtime]
;                                          [?j :job/resource ?r1]
;                                          [?r1 :resource/type :resource.type/cpus]
;                                          [?r1 :resource/amount ?cpus]
;                                          [?j :job/resource ?r2]
;                                          [?r2 :resource/type :resource.type/mem]
;                                          [?r2 :resource/amount ?mem]])
;                                     (group-by second)
;                                     (map (fn [[user reqs]]
;                                            [user (reduce (fn [[cpus-hours mem-hours] [_ _ runtime cpus mem]]
;                                                            [(+ cpus-hours (/ (* runtime cpus) 3600000.0))
;                                                             (+ mem-hours (/ (* runtime mem) 3600000.0))])
;                                                          [0.0 0.0]
;                                                          reqs)]))
;                                     (map (fn [[user [cpus-hours mem-hours]]]
;                                            [user {:cpus cpus-hours :mem mem-hours}]))
;                                     (into {}))
;
;        total-req-by-user (->> db
;                               (q '[:find ?j ?u ?cpus ?mem
;                                    :where
;                                    [?j :job/user ?u]
;                                    [?j :job/resource ?r1]
;                                    [?r1 :resource/type :resource.type/cpus]
;                                    [?r1 :resource/amount ?cpus]
;                                    [?j :job/resource ?r2]
;                                    [?r2 :resource/type :resource.type/mem]
;                                    [?r2 :resource/amount ?mem]])
;                               (group-by second)
;                               (map (fn [[user reqs]]
;                                      [user (reduce (fn [[cpus-sum mem-sum] [_ _ cpus mem]]
;                                                      [(+ cpus-sum cpus) (+ mem-sum mem)])
;                                                    [0.0 0.0]
;                                                    reqs)]))
;                               (map (fn [[user [cpus-sum mem-sum]]]
;                                      [user {:cpus cpus-sum :mem mem-sum}]))
;                               (into {}))
;
;        [total-cpus total-mem] (first (->> db
;                                           (q '[:find (sum ?cpus) (sum ?mem)
;                                                :with ?m
;                                                :where
;                                                [?m :machine/total-mem ?mem]
;                                                [?m :machine/total-cpus ?cpus]])))
;
;        total-capacity {:cpus total-cpus :mem total-mem}
;
;        total-capacity-hours {:cpus (* duration-hours total-cpus)
;                              :mem (* duration-hours total-mem)}
;
;        total-req-hours-ratio-by-user (->> total-req-hours-by-user
;                                           (map
;                                            (fn [[user req]]
;                                              [user (merge-with / req total-capacity-hours)]))
;                                           (into {}))
;
;        total-req-ratio-by-user (->> total-req-by-user
;                                     (map
;                                      (fn [[user req]]
;                                        [user (merge-with / req total-capacity)]))
;                                     (into {}))
;
;        workload-heaviness (merge-with / total-req-hours total-capacity-hours)
;
;        total-sim-sets (ffirst (q '[:find (count ?s)
;                                    :where
;                                    [?s :sim-set/uuid _]]
;                                  db))
;
;        sim-sets (->> (q '[:find ?s
;                           :where
;                           [?s :sim-set/uuid _]]
;                         db)
;                      (map first))
;
;        completed-sim-sets (->> sim-sets
;                                (filter (partial sim-set-completed? db))
;                                (into #{}))
;
;        completed-sim-sets-by-window (->> completed-sim-sets
;                                          (group-by (partial sim-set->submit-window db (* 1000 60 10))))
;
;        sim-set-slow-downs-by-window
;        (->> completed-sim-sets-by-window
;             (map (fn [[window sim-sets]]
;                    [window (map (partial sim-set->slow-down-factor db) sim-sets)])))
;
;        max-sim-set-slow-downs-by-window
;        (->> sim-set-slow-downs-by-window
;             (map (fn [[window datas]]
;                    [window (apply max datas)]))
;             (sort-by first))
;
;        min-sim-set-slow-downs-by-window
;        (->> sim-set-slow-downs-by-window
;             (map (fn [[window datas]]
;                    [window (apply min datas)]))
;             (sort-by first))
;
;        mean-sim-set-slow-downs-by-window
;        (->> sim-set-slow-downs-by-window
;             (map (fn [[window datas]]
;                    (let [sum (reduce + datas)
;                          count (count datas)]
;                      [window (double (/ sum count))])))
;             (sort-by first))
;
;        median-sim-set-slow-downs-by-window
;        (->> sim-set-slow-downs-by-window
;             (map (fn [[window datas]]
;                    (let [sorted-datas (sort datas)
;                          count (count datas)]
;                      [window (nth sorted-datas (int (/ count 2)))])))
;             (sort-by first))
;
;        sim-set-wait-times-by-window
;        (->> completed-sim-sets-by-window
;             (map (fn [[window sim-sets]]
;                    [window (->> sim-sets
;                                 (map (partial sim-set->wait-time db))
;                                 (map (fn [time]
;                                        (/ time 60000.0))))])))
;
;        max-sim-set-wait-times-by-window
;        (->> sim-set-wait-times-by-window
;             (map (fn [[window datas]]
;                    [window (apply max datas)]))
;             (sort-by first))
;
;        min-sim-set-wait-times-by-window
;        (->> sim-set-wait-times-by-window
;             (map (fn [[window datas]]
;                    [window (apply min datas)]))
;             (sort-by first))
;
;        mean-sim-set-wait-times-by-window
;        (->> sim-set-wait-times-by-window
;             (map (fn [[window datas]]
;                    (let [sum (reduce + datas)
;                          count (count datas)]
;                      [window (double (/ sum count))])))
;             (sort-by first))
;
;        median-sim-set-wait-times-by-window
;        (->> sim-set-wait-times-by-window
;             (map (fn [[window datas]]
;                    (let [sorted-datas (sort datas)
;                          count (count sorted-datas)]
;                      [window (nth sorted-datas (int (/ count 2)))])))
;             (sort-by first))
;
;        sim-set-complete-times-by-window
;        (->> completed-sim-sets-by-window
;             (map (fn [[window sim-sets]]
;                    [window (->> sim-sets
;                                 (map (partial sim-set->complete-time db))
;                                 (map (fn [time]
;                                        (/ time 60000.0))))])))
;
;        max-sim-set-complete-times-by-window
;        (->> sim-set-complete-times-by-window
;             (map (fn [[window datas]]
;                    [window (apply max datas)]))
;             (sort-by first))
;
;        min-sim-set-complete-times-by-window
;        (->> sim-set-complete-times-by-window
;             (map (fn [[window datas]]
;                    [window (apply min datas)]))
;             (sort-by first))
;
;        mean-sim-set-complete-times-by-window
;        (->> sim-set-complete-times-by-window
;             (map (fn [[window datas]]
;                    (let [sum (reduce + datas)
;                          count (count datas)]
;                      [window (double (/ sum count))])))
;             (sort-by first))
;
;        median-sim-set-complete-times-by-window
;        (->> sim-set-complete-times-by-window
;             (map (fn [[window datas]]
;                    (let [sorted-datas (sort datas)
;                          count (count sorted-datas)]
;                      [window (nth sorted-datas (int (/ count 2)))])))
;             (sort-by first))
;
;        sim-set-wait-times (->> completed-sim-sets
;                                (map (partial sim-set->wait-time db)))
;
;        sim-set-complete-times (->> completed-sim-sets
;                                    (map (partial sim-set->complete-time db)))
;
;        sim-set->complete-times (->> completed-sim-sets
;                                     (map (fn [sim-set]
;                                            [sim-set (sim-set->complete-time db sim-set)]))
;                                     (into {}))
;
;        sim-set-slow-downs (->> completed-sim-sets
;                                (map (partial sim-set->slow-down-factor db)))
;
;        completed-sim-sets-by-user (->> (q '[:find ?user ?s
;                                   :where
;                                   [?s :sim-set/uuid _]
;                                   [?s :sim-set/job ?j]
;                                   [?j :job/user ?user]]
;                                 db)
;                              (filter (fn [[user sim-set]]
;                                        (get completed-sim-sets sim-set)))
;                              (group-by first)
;                              (map (fn [[user user-simset-pairs]]
;                                     [user (map second user-simset-pairs)]))
;                              (into {}))
;
;        sim-set-wait-times-by-user  (->> completed-sim-sets-by-user
;                                         (map (fn [[user sim-sets]]
;                                                [user (map (partial sim-set->wait-time db) sim-sets)]))
;                                         (into {}))
;
;        sim-set-complete-times-by-user (->> completed-sim-sets-by-user
;                                            (map (fn [[user sim-sets]]
;                                                   [user (map (partial sim-set->complete-time db) sim-sets)]))
;                                            (into {}))
;
;
;        sim-set-slow-downs-by-user (->> completed-sim-sets-by-user
;                                        (map (fn [[user sim-sets]]
;                                               [user (map (partial sim-set->slow-down-factor db) sim-sets)]))
;                                        (into {}))
;
;        total-jobs-by-user (->> (q '[:find ?u (count ?j)
;                                     :where
;                                     [?j :job/user ?u]
;                                     [?j :job/state _]]
;                                   db)
;                                (into {}))
;        completed-jobs (->>
;                        (q '[:find ?j
;                             :where
;                             [?j :job/state :job.state/completed]]
;                           db)
;                        (map first))
;
;        completed-jobs-by-user (->> (q '[:find ?u (count ?j)
;                                         :where
;                                         [?j :job/user ?u]
;                                         [?j :job/state :job.state/completed]]
;                                       db)
;                                    (into {}))
;        instance-status (into {} (q '[:find ?status (count ?t)
;                                      :where
;                                      [?t :instance/status ?s]
;                                      [?s :db/ident ?status]]
;                                    db))
;
;        preempted-instances-by-user (->> db
;                                         (q '[:find ?u ?i
;                                              :where
;                                              [?i :instance/status :instance.status/failed]
;                                              [?j :job/instance ?i]
;                                              [?j :job/user ?u]])
;                                         (group-by first)
;                                         (map (fn [[user user-and-instances]]
;                                                [user (count user-and-instances)]))
;                                         (into {}))
;
;        wait-times (->> (q '[:find ?t ?j
;                             :where
;                             [?j :job/state :job.state/completed]
;                             [(cook.mesos.scheduler.simulator/job-wait-time $ ?j) ?t]
;                             [(identity ?t)]]
;                           db)
;                        (map first))
;
;        complete-times (->> (q '[:find ?t ?j
;                                 :where
;                                 [?j :job/state :job.state/completed]
;                                 [(cook.mesos.scheduler.simulator/job-complete-time $ ?j) ?t]
;                                 [(identity ?t)]]
;                               db)
;                            (map first))
;
;        wait-times-by-user (->> (q '[:find ?user ?t ?j
;                                     :where
;                                     [?j :job/state :job.state/completed]
;                                     [?j :job/user ?user]
;                                     [(cook.mesos.scheduler.simulator/job-wait-time $ ?j) ?t]
;                                     [(identity ?t)]]
;                                   db)
;                                (group-by first)
;                                (map (fn [[k v]]
;                                       [k (mapv second v)])))
;        total-cpus (ffirst (q '[:find (sum ?cpu)
;                                :with ?m
;                                :where
;                                [?m :machine/total-cpus ?cpu]]
;                              db))
;        total-mem (ffirst (q '[:find (sum ?mem)
;                               :with ?m
;                               :where
;                               [?m :machine/total-mem ?mem]]
;                             db))
;        free-cpus (ffirst (q '[:find (sum ?cpu)
;                               :with ?m
;                               :where
;                               [?m :machine/cpus ?cpu]]
;                             db))
;        free-mem (ffirst (q '[:find (sum ?mem)
;                              :with ?m
;                              :where
;                              [?m :machine/mem ?mem]]
;                            db))
;        used-cpu-time (ffirst (q '[:find (sum ?cpu-time)
;                                   :with ?i
;                                   :where
;                                   [?i :instance/start-time ?start]
;                                   [?i :instance/end-time ?end]
;                                   [?j :job/instance ?i]
;                                   [?j :job/resource ?r]
;                                   [?r :resource/type :resource.type/cpus]
;                                   [?r :resource/amount ?cpus]
;                                   [(.getTime ?start) ?s]
;                                   [(.getTime ?end) ?e]
;                                   [(- ?e ?s) ?d]
;                                   [(* ?cpus ?d) ?cpu-time]]
;                                 db))
;        wasted-cpu-time (ffirst (q '[:find (sum ?cpu-time)
;                                     :with ?i
;                                     :where
;                                     [?i :instance/status :instance.status/failed]
;                                     [?i :instance/start-time ?start]
;                                     [?i :instance/end-time ?end]
;                                     [?j :job/instance ?i]
;                                     [?j :job/resource ?r]
;                                     [?r :resource/type :resource.type/cpus]
;                                     [?r :resource/amount ?cpus]
;                                     [(.getTime ?start) ?s]
;                                     [(.getTime ?end) ?e]
;                                     [(- ?e ?s) ?d]
;                                     [(* ?cpus ?d) ?cpu-time]]
;                                   db))
;        used-mem-time (ffirst (q '[:find (sum ?mem-time)
;                                   :with ?i
;                                   :where
;                                   [?i :instance/start-time ?start]
;                                   [?i :instance/end-time ?end]
;                                   [?j :job/instance ?i]
;                                   [?j :job/resource ?r]
;                                   [?r :resource/type :resource.type/mem]
;                                   [?r :resource/amount ?mem]
;                                   [(.getTime ?start) ?s]
;                                   [(.getTime ?end) ?e]
;                                   [(- ?e ?s) ?d]
;                                   [(* ?mem ?d) ?mem-time]]
;                                 db))
;        wasted-mem-time (ffirst (q '[:find (sum ?mem-time)
;                                     :with ?i
;                                     :where
;                                     [?i :instance/status :instance.status/failed]
;                                     [?i :instance/start-time ?start]
;                                     [?i :instance/end-time ?end]
;                                     [?j :job/instance ?i]
;                                     [?j :job/resource ?r]
;                                     [?r :resource/type :resource.type/mem]
;                                     [?r :resource/amount ?mem]
;                                     [(.getTime ?start) ?s]
;                                     [(.getTime ?end) ?e]
;                                     [(- ?e ?s) ?d]
;                                     [(* ?mem ?d) ?mem-time]]
;                                   db))
;        job-slow-downs-by-user (->> (q '[:find ?user ?factor ?j
;                                         :where
;                                         [?j :job/state :job.state/completed]
;                                         [?j :job/user ?user]
;                                         [(cook.mesos.scheduler.simulator/job-slow-down-factor $ ?j)
;                                          ?factor]]
;                                       db)
;                                    (group-by first)
;                                    (map (fn [[k v]]
;                                           [k (mapv second v)]
;                                           )))
;
;        job-slow-downs  (mapv first (q '[:find ?factor ?j
;                                         :where
;                                         [?j :job/state :job.state/completed]
;                                         [(cook.mesos.scheduler.simulator/job-slow-down-factor $ ?j) ?factor]]
;                                       db))
;
;        interesting-users (->> wait-times-by-user
;                               (filter (fn [[user wait-times]]
;                                         (> (apply max wait-times) 600000.0)))
;                               (map first)
;                               (into #{}))
;
;        utilizations (for [t stamps-txn-t
;                           :let [db (d/as-of db t)
;                                 total-cpus (ffirst (q '[:find (sum ?cpu)
;                                                         :with ?m
;                                                         :where
;                                                         [?m :machine/total-cpus ?cpu]]
;                                                       db))
;                                 total-mem (ffirst (q '[:find (sum ?mem)
;                                                        :with ?m
;                                                        :where
;                                                        [?m :machine/total-mem ?mem]]
;                                                      db))
;                                 free-cpus (ffirst (q '[:find (sum ?cpu)
;                                                        :with ?m
;                                                        :where
;                                                        [?m :machine/cpus ?cpu]]
;                                                      db))
;                                 free-mem (ffirst (q '[:find (sum ?mem)
;                                                       :with ?m
;                                                       :where
;                                                       [?m :machine/mem ?mem]]
;                                                     db))]]
;                       {:cpus (double (/ (- total-cpus free-cpus)
;                                         total-cpus))
;                        :mem (double (/ (- total-mem free-mem)
;                                        total-mem))})
;        quantile-pcts [0.0 0.1 0.2 0.3 0.4 0.5 0.6 0.65 0.70 0.75 0.80 0.85 0.90 0.95 0.97 0.99 1.0]]
;    (println "## Basics")
;    (println "We submitted a total of" total-jobs)
;    (println "Of these," (count completed-jobs) "finished")
;    (println "This is a rate of " (double (/ (count completed-jobs) total-jobs)))
;    (println "We submitted a total of" total-sim-sets "sim-sets")
;    (println "Of these," (count completed-sim-sets) "finished")
;    (println "This is a rate of " (double (/ (count completed-sim-sets) total-sim-sets)))
;    (println "The instances currently break down as such:" instance-status)
;    (println "Preempted instances count:" preempted-instances-by-user)
;    (println "## Wait times")
;    (println "Sim-set wait quantiles:")
;    (println (apply quantiles sim-set-wait-times #(format "%.2f" (float (/ % 60000))) quantile-pcts))
;    (println (apply quantiles sim-set-complete-times #(format "%.2f" (float (/ % 60000))) quantile-pcts))
;    (println (apply quantiles sim-set-slow-downs #(format "%.2f" %) quantile-pcts))
;    (println "Sim-set wait quantiles by user:")
;    (doseq [[user wait-times]
;              (->> sim-set-complete-times-by-user
;                   #_(filter (fn [[user _]] (get interesting-users user)))
;                   (sort-by (fn [[user wait-times]]
;                              (get-in total-req-hours-by-user [user :mem]))))]
;        (println (str "  " user ":")
;                 (count (get completed-sim-sets-by-user user))
;                 (format "%.3f"(get-in total-req-hours-ratio-by-user [user :mem]))
;                 (format "%.3f"(get-in total-req-ratio-by-user [user :mem]))
;                 (apply quantiles wait-times #(format "%.2f" (float (/ % 60000))) quantile-pcts)))
;    (println "Sim-set slow downs by user:")
;    (doseq [[user slow-downs]
;              (->> sim-set-slow-downs-by-user
;                   (sort-by (fn [[user _]]
;                              (get-in total-req-by-user [user :mem]))))]
;      (println (str "  " user ":")
;               (count (get completed-sim-sets-by-user user))
;               (format "%.3f"(get-in total-req-hours-ratio-by-user [user :mem]))
;               (format "%.3f"(get-in total-req-ratio-by-user [user :mem]))
;               (apply quantiles slow-downs #(format "%.2f" %) quantile-pcts)))
;    (println "Wait quantiles:")
;    (println (apply quantiles wait-times #(format "%.2f" (float (/ % 60000))) quantile-pcts))
;    (println (apply quantiles complete-times #(format "%.2f" (float (/ % 60000))) quantile-pcts))
;    (println (apply quantiles job-slow-downs #(format "%.2f" %) quantile-pcts))
;    #_(println "Wait quantiles by user:")
;    #_(doseq [[user wait-times]
;            (->> wait-times-by-user
;                 #_(filter (fn [[user _]] (get interesting-users user)))
;                 (sort-by (fn [[user wait-times]]
;                            (get-in total-req-hours-by-user [user :mem]))))]
;      (println (str "  " user ":")
;               (get total-jobs-by-user user)
;               (get completed-jobs-by-user user)
;               (format "%.3f"(get-in total-req-hours-ratio-by-user [user :mem]))
;               (format "%.3f"(get-in total-req-ratio-by-user [user :mem]))
;               (apply quantiles wait-times #(format "%.2f" (float (/ % 60000))) quantile-pcts)))
;    #_(println "Mean wait time:" (to-hhmmss (long (/ (apply + wait-times) (inc (conpunt wait-times))))))
;    #_(println "Max wait time:" (to-hhmmss (apply max -1 wait-times)))
;    #_(println "Slow downs:" (apply quantiles job-slow-downs #(format "%.2f" %) quantile-pcts))
;    #_(doseq [[user slowdowns] job-slow-downs-by-user]
;      (println (str "  " user ":")
;               (get total-jobs-by-user user)
;               (get completed-jobs-by-user user)
;               (apply quantiles slowdowns #(format "%.2f" %) quantile-pcts)))
;    #_(println "## Usage")
;    #_(println "Cpu waste percent" (double (/ (or wasted-cpu-time 0) (inc (or used-cpu-time 0)) )))
;    #_(println "Mem waste percent" (double (/ (or wasted-mem-time 0) (inc (or used-mem-time 0)) )))
;    #_(println "cpu utilization overall (including waste)" (apply quantiles (map :cpus utilizations) #(format "%.2f" %) quantile-pcts))
;    #_(println "mem utilization overall (including waste)" (apply quantiles (map :mem utilizations) #(format "%.2f" %) quantile-pcts))
;    #_(println "Usage ratio of CPU is" (double (/ (- total-cpus free-cpus)
;                                                total-cpus)))
;    #_(println "Usage ratio of memory is" (double (/ (- total-mem free-mem)
;                                                   total-mem)))
;    {:sim-set-wait-times {:x quantile-pcts :y (map second (apply quantiles sim-set-wait-times #(float (/ % 60000)) quantile-pcts))}
;     :sim-set-wait-times-by-user (->> sim-set-wait-times-by-user
;                                      (map (fn [[user datas]]
;                                             [user
;                                              {:x quantile-pcts
;                                               :y (->> (apply quantiles datas ms->minute quantile-pcts)
;                                                       (map second))}]))
;                                      (into {}))
;     :sim-set-complete-times {:x quantile-pcts :y (map second (apply quantiles sim-set-complete-times #(float (/ % 60000)) quantile-pcts))}
;     :sim-set-complete-times-by-user (->> sim-set-complete-times-by-user
;                                          (map (fn [[user datas]]
;                                                 [user
;                                                  {:x quantile-pcts
;                                                   :y (->> (apply quantiles datas ms->minute quantile-pcts)
;                                                           (map second))}]))
;                                          (into {}))
;
;     :sim-set-slow-downs {:x quantile-pcts :y (map second (apply quantiles sim-set-slow-downs identity quantile-pcts))}
;     :sim-set-slow-downs-by-user (->> sim-set-slow-downs-by-user
;                                      (map (fn [[user datas]]
;                                             [user
;                                              {:x quantile-pcts
;                                               :y (->> (apply quantiles datas identity quantile-pcts)
;                                                       (map second))}]))
;                                      (into {}))
;     :job-wait-times {:x quantile-pcts :y (map second (apply quantiles wait-times #(float (/ % 60000)) quantile-pcts))}
;     :job-complete-times {:x quantile-pcts :y (map second (apply quantiles complete-times #(float (/ % 60000)) quantile-pcts))}
;     :job-slow-downs {:x quantile-pcts :y (map second (apply quantiles job-slow-downs identity quantile-pcts))}
;     :max-sim-set-slow-downs-by-window {:x (map first max-sim-set-slow-downs-by-window) :y (map second max-sim-set-slow-downs-by-window)}
;     :min-sim-set-slow-downs-by-window {:x (map first min-sim-set-slow-downs-by-window) :y (map second min-sim-set-slow-downs-by-window)}
;     :mean-sim-set-slow-downs-by-window {:x (map first mean-sim-set-slow-downs-by-window) :y (map second mean-sim-set-slow-downs-by-window)}
;     :median-sim-set-slow-downs-by-window {:x (map first median-sim-set-slow-downs-by-window) :y (map second median-sim-set-slow-downs-by-window)}
;     :max-sim-set-wait-times-by-window {:x (map first max-sim-set-wait-times-by-window) :y (map second max-sim-set-wait-times-by-window)}
;     :min-sim-set-wait-times-by-window {:x (map first min-sim-set-wait-times-by-window) :y (map second min-sim-set-wait-times-by-window)}
;     :mean-sim-set-wait-times-by-window {:x (map first mean-sim-set-wait-times-by-window) :y (map second mean-sim-set-wait-times-by-window)}
;     :median-sim-set-wait-times-by-window {:x (map first median-sim-set-wait-times-by-window) :y (map second median-sim-set-wait-times-by-window)}
;     :max-sim-set-complete-times-by-window {:x (map first max-sim-set-complete-times-by-window) :y (map second max-sim-set-complete-times-by-window)}
;     :min-sim-set-complete-times-by-window {:x (map first min-sim-set-complete-times-by-window) :y (map second min-sim-set-complete-times-by-window)}
;     :mean-sim-set-complete-times-by-window {:x (map first mean-sim-set-complete-times-by-window) :y (map second mean-sim-set-complete-times-by-window)}
;     :median-sim-set-complete-times-by-window {:x (map first median-sim-set-complete-times-by-window) :y (map second median-sim-set-complete-times-by-window)}
;     :sim-set->complete-times sim-set->complete-times}))
;
;(defn gen-report-v2
;  [db]
;  (let [stamps-txn-t (sort (->> (q '[:find ?t ?tid
;                                     :where
;                                     [_ :global/stamp-time ?t ?tid]]
;                                   db)))
;
;        quantile-pcts [0.0 0.1 0.2 0.3 0.4 0.5 0.6 0.65 0.70 0.75 0.80 0.85 0.90 0.95 0.97 0.99 1.0]
;
;        sim-sets (->> db
;                      (q '[:find ?s
;                           :where
;                           [?s :sim-set/uuid _]])
;                      (map first))
;
;        #_job-infos #_(->> db
;                       (q '[:find ?j
;                            :where
;                            [?j :job/uuid _]])
;                       (map first)
;                       (map (partial job->info db)))
;
;        uuid->sim-set-infos (->> sim-sets
;                           (map (fn [sim-set]
;                                  (let [info (sim-set->info db sim-set)]
;                                    [(:uuid info) info])))
;                           (into {}))
;
;        uuid->completed-sim-set-infos (->> uuid->sim-set-infos
;                                           (filter (fn [[uuid info]]
;                                                     (:completed? info)))
;                                           (into {}))
;
;        effective-instance-eids (-<>> db
;                                      (q '[:find ?i
;                                           :in $ [?s ...]
;                                           :where
;                                           [?i :instance/status ?s]]
;                                         <> [:instance.status/running :instance.status/success :instance.status/unknown])
;                                      (map first)
;                                      (into #{}))
;
;        utilizations
;        (for [[t tid] stamps-txn-t
;              :let [db (d/as-of db tid)
;                    total-cpus (ffirst (q '[:find (sum ?cpu)
;                                            :with ?m
;                                            :where
;                                            [?m :machine/total-cpus ?cpu]]
;                                          db))
;                    total-mem (ffirst (q '[:find (sum ?mem)
;                                           :with ?m
;                                           :where
;                                           [?m :machine/total-mem ?mem]]
;                                         db))
;                    [used-cpus used-mem]
;                    (first (q '[:find (sum ?cpus) (sum ?mem)
;                                :in $ [?i ...]
;                                :with ?i
;                                :where
;                                [?i :instance/status :instance.status/running]
;                                [?j :job/instance ?i]
;                                [?j :job/resource ?r1]
;                                [?r1 :resource/type :resource.type/cpus]
;                                [?r1 :resource/amount ?cpus]
;                                [?j :job/resource ?r2]
;                                [?r2 :resource/type :resource.type/mem]
;                                [?r2 :resource/amount ?mem]]
;                              db effective-instance-eids))
;                    _ (println (java.util.Date. t))
;                    ]]
;          [t
;           {:cpus (double (/ used-cpus
;                             total-cpus))
;            :mem (double (/ used-mem
;                            total-mem))}])
;        mem-utils (-<>> utilizations
;                        (map second)
;                        (map :mem)
;                        (sort)
;                        #_(apply quantiles <> identity quantile-pcts))]
;    (println "Total sim-sets:" (count uuid->sim-set-infos))
;    (println "Total completed sim-sets:" (count uuid->completed-sim-set-infos))
;    (println "Total preemptions: " (->> uuid->sim-set-infos
;                                        (vals)
;                                        (map :preemption-count)
;                                        (reduce +)))
;    #_(println "Utilizations:")
;    #_(clojure.pprint/pprint utilizations)
;    #_(println "Utilization: " (/ (reduce + mem-utils) (count mem-utils)))
;    {:uuid->sim-set-infos uuid->sim-set-infos
;     :utilizations utilizations}))
;
;(comment
;  (gen-report (db (d/connect "datomic:mem://preemptive-sim")))
;
;  (gen-report (db (d/connect "datomic:dev://example.twosigma.com:4337/sim-2.0-0.75")))
;
;  (q '[:find ?i ?mem ?e ?s
;       :where
;       [?i :instance/status :instance.status/failed]
;       [?i :instance/start-time ?start]
;       [?i :instance/end-time ?end]
;       [?j :job/instance ?i]
;       [?j :job/resource ?r]
;       [?r :resource/type :resource.type/mem]
;       [?r :resource/amount ?mem]
;       [(.getTime ?start) ?s]
;       [(.getTime ?end) ?e]
;       [(- ?e ?s) ?d]
;       [(* ?mem ?d) ?mem-time]
;       ]
;     (db (d/connect "datomic:mem://preemptive-sim")))
;  )
;
;(defn preempt-task-txns
;  [db task-ent t]
;  (let [instance-id (:db/id task-ent)
;        job-id (:db/id (:job/_instance task-ent))
;        hostname (:instance/hostname task-ent)
;        host-id (ffirst (q '[:find ?h
;                             :in $ ?host
;                             :where
;                             [?h :machine/name ?host]]
;                           db hostname))
;        {:keys [cpus mem]} (util/job-ent->resources (d/entity db job-id))
;        txns
;        [[:db/add instance-id :instance/status :instance.status/failed]
;         [:db/add instance-id :instance/end-time (java.util.Date. t)]
;         [:db/add job-id :job/state :job.state/waiting]
;         [:atomic-inc host-id :machine/cpus cpus]
;         [:atomic-inc host-id :machine/mem mem]]]
;    txns))
;
;(defn create-mesos-driver
;  [conn events-atom now-atom]
;  (reify org.apache.mesos.SchedulerDriver
;    (^org.apache.mesos.Protos$Status launchTasks [this ^java.util.Collection offer-ids ^java.util.Collection tasks]
;      (try
;        (let [now-millis (.getTime @now-atom)
;              db (mt/db conn)]
;          (doseq [task tasks]
;            #_(println "LaunchTask. Host:"
;                       (-> task (.getSlaveId) (.getValue))
;                       "Name:"
;                       (-> task (.getName))
;                       )
;            (let [task-id (-> task (.getTaskId) (.getValue))
;                  slave-id (-> task (.getSlaveId) (.getValue))
;                  host-id (ffirst (q '[:find ?h
;                                       :in $ ?host
;                                       :where
;                                       [?h :machine/name ?host]]
;                                    db slave-id))
;                  resources (->> task
;                                (.getResourcesList)
;                                (map (fn [r] [(keyword (.getName r)) (.getValue (.getScalar r))]))
;                                (into {}))
;                  [job-id instance-id runtime] (first (q '[:find ?j ?i ?runtime
;                                                           :in $ ?task-id
;                                                           :where
;                                                           [?i :instance/task-id ?task-id]
;                                                           [?j :job/instance ?i]
;                                                           [?j :job/task-runtime ?runtime]]
;                                                         db task-id))
;                  task-finish-event [{:task-finish task-id} (+ now-millis runtime)]
;                  subtract-resources-txns [[:atomic-inc host-id :machine/cpus (- (:cpus resources))]
;                                           [:atomic-inc host-id :machine/mem (- (:mem resources))]]
;                  update-instance-state-txns [[:instance/update-state instance-id :instance.status/running]]
;                  update-job-state-txns [[:db/add job-id :job/state :job.state/running]]]
;              (do
;                (swap! events-atom conj task-finish-event)
;                @(d/transact conn (concat update-instance-state-txns update-job-state-txns subtract-resources-txns))
;                ))))
;        (catch Exception e
;          (clojure.stacktrace/print-stack-trace e)))
;      org.apache.mesos.Protos$Status/DRIVER_RUNNING)
;
;    (declineOffer [this offer-id]
;      org.apache.mesos.Protos$Status/DRIVER_RUNNING)))
;
;(defn vector->offer
;  [[hostname cpus mem]]
;  {:id (str (java.util.UUID/randomUUID))
;   :resources {:cpus cpus :mem mem}
;   :hostname hostname
;   :slave-id hostname})
;
;(defn generate-offers-from-db
;  [db]
;  (let [resources (q '[:find ?host ?cpus ?mem
;                       :where
;                       [?h :machine/name ?host]
;                       [?h :machine/cpus ?cpus]
;                       [?h :machine/mem ?mem]]
;                     db)]
;    (->> resources
;         (map vector->offer))))
;
;(defn run-sim
;  "Expect an empty database
;
;   If scheduler is :old-scheduler, using the simple quota system
;
;   If scheduler is a map, uses that as the factors and fair-share-over-time config"
;  [uri scheduler start-day run-days machines job-events params]
;  (println "Run-sim:" uri)
;  (let [conn (init-db uri)
;        end-time (+ start-day run-days)
;        events (-> (pm/priority-map)
;                   (into job-events))
;        max-time (-> (rseq events)
;                     first
;                     second)
;        five-min-in-ms (* 5 60 1000)
;        interval five-min-in-ms
;        scheduler-runs (->> (range (+ start-day interval) (+ start-day run-days) interval)
;                            (map #(vector {scheduler %} %)))
;        events (into events (conj scheduler-runs [{:sim-finish end-time} end-time]))
;        total-cpus (apply + (map second machines))
;        total-mem (apply + (map #(nth % 2) machines))
;        now-atom (atom (java.util.Date. 0))
;        events-atom (atom events)
;        fid "scheduler_simulator"
;        driver (create-mesos-driver conn events-atom now-atom)
;        submission-count-atom (atom 0)
;        finish-count-atom (atom 0)]
;    @(d/transact conn (conj (mapcat #(apply make-host-txn %) machines)
;                            [:db/add (d/tempid :db.part/tx) :db/txInstant
;                             (java.util.Date. 30)]))
;    (try
;      (with-redefs [sched/now (fn [] @now-atom)]
;       (loop [events-atom events-atom]
;         (when-let [[cur-event t] (peek @events-atom)]
;           (swap! events-atom pop)
;           (reset! now-atom (java.util.Date. t)) ;; ensure `now` is correct
;           (let []
;             (cond
;              (:sim-finish cur-event)
;              (println "Sim finish: " (:sim-finish cur-event))
;
;              (:new-scheduler cur-event)
;              (let [db (mt/db conn)
;                    db1 (mt/db conn)
;                    jobs (time (sched/rank-jobs conn identity))
;                    offers (generate-offers-from-db db1)
;                    _ (time @(sched/handle-resource-offer! conn driver fid (atom jobs) offers))
;
;                    db2 (mt/db conn)
;                    jobs2 (time (sched/rank-jobs db2 identity))
;                    [job-ents-to-launch task-ents-to-preempt] (time (rebalancer/rebalance db2 jobs2 {} params))
;                    #_(println "preemption count:" (count task-ents-to-preempt))
;                    #_(println "job-ents-to-launch:")
;                    #_(clojure.pprint/pprint job-ents-to-launch)
;                    #_(println "task-ents-to-preempt:")
;                    #_(clojure.pprint/pprint task-ents-to-preempt)
;                    _ (println "preemption count:" (count task-ents-to-preempt))
;                    _ (doseq [task-ent task-ents-to-preempt]
;                        (let [task-id (:instance/task-id task-ent)
;                              txns (preempt-task-txns db task-ent t)]
;                          (when task-id
;                            @(d/transact conn txns)
;                            (swap! events-atom dissoc {:task-finish task-id}))))
;                    stamp-txn [:db/add (d/tempid :db.part/user) :global/stamp-time t]]
;                (println "Current Event:" cur-event "Time:" t)
;                @(d/transact conn [stamp-txn])
;                (recur events-atom))
;
;              (:job cur-event)
;              ;; Submit the job
;              (let [db (mt/db conn)
;                    {:keys [simset-uuid] :as job} (:job cur-event)
;                    simset-id (ffirst (q '[:find ?s
;                                           :in $ ?simset-uuid
;                                           :where
;                                           [?s :sim-set/uuid ?simset-uuid]]
;                                         db simset-uuid))]
;                @(d/transact conn (make-job-txn simset-id job))
;                (swap! submission-count-atom inc)
;                (recur events-atom))
;
;              (:task-finish cur-event)
;              (let [task-id (:task-finish cur-event)
;                    [tid hostname host-id jid]
;                    (first (q '[:find ?t ?hostname ?h ?j
;                                :in $ ?task-id
;                                :where
;                                [?t :instance/task-id ?task-id]
;                                [?t :instance/hostname ?hostname]
;                                [?h :machine/name ?hostname]
;                                [?j :job/instance ?t]]
;                              (mt/db conn) task-id))
;                    finish-task-txn [[:db/add tid :instance/status :instance.status/success]
;                                     [:db/add tid :instance/end-time (java.util.Date. t)]]
;                    finish-job-txn [[:db/add jid :job/state :job.state/completed]]
;                    job-resources (util/job-ent->resources (d/entity (mt/db conn) jid))
;                    add-machine-resources-txn [[:atomic-inc host-id :machine/cpus
;                                                (:cpus job-resources)]
;                                               [:atomic-inc host-id :machine/mem
;                                                (:mem job-resources)]]]
;                ;; mark the task & job finished, release resources
;                @(d/transact conn (concat
;                                   finish-task-txn
;                                   finish-job-txn
;                                   add-machine-resources-txn))
;                (swap! finish-count-atom inc)
;                (recur events-atom)))))))
;      (catch Exception e
;        (clojure.stacktrace/print-stack-trace e)))
;    @(d/transact conn [{:db/id :global
;                        :global/start-time start-day
;                        :global/end-time max-time}])))
;
;(defn test
;  []
;  (let [datomic-uri "datomic:mem://test"
;        days-in-ms 86400000
;        start-day (.getTime #inst "2014-12-01")]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :scheduler
;             start-day
;             (* 2 60 60 1000)
;             [["usrs001.pit.twosigma.com" 48.0 256000.0]]
;             (take 100 (repeatedly
;                        (fn []
;                          (let [t start-day]
;                            [{:job [(java.util.UUID/randomUUID)
;                                    "ljin"
;                                    (java.util.Date. t)
;                                    (* 5 60 1000)
;                                    4.0
;                                    32000.0]}
;                             t])))))))
;
;(defn test-2
;  []
;  (let [datomic-uri "datomic:mem://test-2"
;        days-in-ms 86400000
;        start-day (.getTime #inst "2014-12-01")]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-day
;             (/ days-in-ms 24)
;             (map #(vector (str "machine-" %) 48.0 256000.0) (range 16))
;             (take 3328 (repeatedly
;                         (fn []
;                          (let [t start-day]
;                            [{:job {:uuid (java.util.UUID/randomUUID)
;                                    :user (rand-nth ["ljin"])
;                                    :time (java.util.Date. t)
;                                    :simset-uuid (java.util.UUID/randomUUID)
;                                    :runtime (* 60 60 1000)
;                                    :cpus 1.0
;                                    :mem 32000.0
;                                    }}
;                             t]))))
;             {:max-preemption 128
;              :safe-dru-threshold 100.0
;              :min-dru-diff 100.0
;              :dynamic-quota {:resource.type/mem  {:min   (* 250 1024 4)
;                                                   :max   (* 250 1024 4)
;                                                   :total (* 250 1024 16)
;                                                   :load  0.9}
;                              :resource.type/cpus {:min   2000.0
;                                                   :max   3000.0
;                                                   :total 9000.0
;                                                   :load  0.9}}})))
;
;(defn test-3
;  []
;  (let [datomic-uri "datomic:mem://test-3"
;        days-in-ms 86400000
;        start-day (.getTime #inst "2014-12-01")]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-day
;             days-in-ms
;             (map #(vector (str "machine-" %) 48.0 256000.0) (range 16))
;             (take 200 (repeatedly
;                        (fn []
;                          (let [t (+ start-day (rand-int (quot days-in-ms 24)))]
;                            [{:job [(java.util.UUID/randomUUID)
;                                    (rand-nth ["ljin" "wzhao" "sunil" "palaitis" "greenberg"])
;                                    (java.util.Date. t)
;                                    (* 60 60 1000)
;                                    4.0
;                                    32000.0]}
;                             t]))))
;             {:mem-quota Double/MAX_VALUE
;              :cpus-quota Double/MAX_VALUE
;              :max-preemption 128
;              :safe-dru-threshold 1.0
;              :min-dru-diff 0.5
;              })))
;
;(comment ())
;
;(defn test-4
;  []
;  (let [datomic-uri "datomic:mem://test-4"
;        days-in-ms 86400000
;        start-day (.getTime #inst "2014-12-01")]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-day
;             (* 3 60 60 1000)
;             (map #(vector (str "machine-" %) 48.0 256000.0) (range 8))
;             (concat (take 64 (repeatedly
;                                (fn []
;                                  (let [t (+ start-day 60)]
;                                    [{:job [(java.util.UUID/randomUUID)
;                                            "ljin"
;                                            (java.util.Date. t)
;                                            (* 60 60 1000)
;                                            4.0
;                                            32000.0]}
;                                     t]))))
;                     (take 64 (repeatedly
;                                (fn []
;                                  (let [t (+ start-day (* 15 60 1000) 60)]
;                                    [{:job [(java.util.UUID/randomUUID)
;                                            "wzhao"
;                                            (java.util.Date. t)
;                                            (* 60 60 1000)
;                                            4.0
;                                            32000.0]}
;                                     t]))))
;                     (take 64 (repeatedly
;                                (fn []
;                                  (let [t (+ start-day (* 30 60 1000) 60)]
;                                    [{:job [(java.util.UUID/randomUUID)
;                                            "sunil"
;                                            (java.util.Date. t)
;                                            (* 60 60 1000)
;                                            4.0
;                                            32000.0]}
;                                     t])))))
;             {:mem-quota 2048000.0
;              :cpus-quota Double/MAX_VALUE
;              :max-preemption 128
;              :safe-dru-threshold 0.2
;              :min-dru-diff 0.0})))
;
;(defn test-5
;  []
;  (let [datomic-uri "datomic:mem://test-5"
;        days-in-ms 86400000
;        start-day (.getTime #inst "2014-12-01")]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-day
;             (* 3 60 60 1000)
;             (map #(vector (str "machine-" %) 48.0 256000.0) (range 8))
;             (let [gen-sims (fn [t user num-sims]
;                              (let [simset-uuid (java.util.UUID/randomUUID)
;                                    rand-off (rand-int 60)]
;                                (take num-sims (repeatedly
;                                                (fn []
;                                                  [{:job {:uuid (java.util.UUID/randomUUID)
;                                                          :user user
;                                                          :time (java.util.Date. (+ t rand-off))
;                                                          :simset-uuid simset-uuid
;                                                          :runtime (* 60 60 1000)
;                                                          :cpus 4.0
;                                                          :mem 32000.0}}
;                                                   t])))))]
;               (concat (let [t (+ start-day 60)
;                            user "ljin"]
;                         (apply concat (take 8 (repeatedly (partial gen-sims t user 8)))))
;                       (let [t (+ start-day (* 15 60 1000) 60)
;                             user "wzhao"]
;                         (apply concat (take 8 (repeatedly (partial gen-sims t user 8)))))
;                       (let [t (+ start-day (* 30 60 1000) 60)
;                             user "sunil"]
;                         (apply concat (take 8 (repeatedly (partial gen-sims t user 8)))))))
;             {:max-preemption 128
;              :safe-dru-threshold 0.33
;              :min-dru-diff 0.05
;              :dynamic-quota {:resource.type/mem  {:min   (* 256000.0 8)
;                                                   :max   (* 256000.0 8)
;                                                   :total (* 256000.0 8)
;                                                   :load  0.9}
;                              :resource.type/cpus {:min   (* 48.0 8)
;                                                   :max   (* 48.0 8)
;                                                   :total (* 48.0 8)
;                                                   :load  0.9}}})))
;
;(defn test-6
;  []
;  (let [datomic-uri "datomic:mem://test-6"
;        days-in-ms 86400000
;        start-day (.getTime #inst "2014-12-01")]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-day
;             (* 3 60 60 1000)
;             (map #(vector (str "machine-" %) 48.0 256000.0) (range 8))
;             (concat (take 256 (repeatedly
;                               (fn []
;                                 (let [t (+ start-day 60)]
;                                   [{:job [(java.util.UUID/randomUUID)
;                                           "ljin"
;                                           (java.util.Date. t)
;                                           (* 2 60 60 1000)
;                                           4.0
;                                           32000.0]}
;                                    t]))))
;                     (take 256 (repeatedly
;                                (fn []
;                                  (let [t (+ start-day (* 15 60 1000) 60)]
;                                    [{:job [(java.util.UUID/randomUUID)
;                                            "wzhao"
;                                            (java.util.Date. t)
;                                            (* 2 60 60 1000)
;                                           4.0
;                                           32000.0]}
;                                     t]))))
;                     (take 8 (repeatedly
;                               (fn []
;                                  (let [t (+ start-day (* 30 60 1000) 60)]
;                                    [{:job [(java.util.UUID/randomUUID)
;                                            "sunil"
;                                            (java.util.Date. t)
;                                            (* 60 60 1000)
;                                            4.0
;                                            32000.0]}
;                                     t]))))
;                     (take 8 (repeatedly
;                               (fn []
;                                 (let [t (+ start-day (* 40 60 1000) 60)]
;                                   [{:job [(java.util.UUID/randomUUID)
;                                           "dgrnbrg"
;                                           (java.util.Date. t)
;                                           (* 60 60 1000)
;                                           4.0
;                                           32000.0]}
;                                    t])))))
;             {:mem-quota 1024000.0
;              :cpus-quota Double/MAX_VALUE
;              :max-preemption 128
;              :safe-dru-threshold 1.0
;              :min-dru-diff 100.0})))
;
;(defn test-7
;  []
;  (let [datomic-uri "datomic:mem://test-7"
;        days-in-ms 86400000
;        start-day (.getTime #inst "2014-12-01")]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-day
;             (* 3 60 60 1000)
;             (map #(vector (str "machine-" %) 48.0 256000.0) (range 8))
;             (concat (take 256 (repeatedly
;                               (fn []
;                                 (let [t (+ start-day 60)]
;                                   [{:job [(java.util.UUID/randomUUID)
;                                           "ljin"
;                                           (java.util.Date. t)
;                                           (* 2 60 60 1000)
;                                           4.0
;                                           32000.0]}
;                                    t]))))
;                     (take 256 (repeatedly
;                                (fn []
;                                  (let [t (+ start-day (* 15 60 1000) 60)]
;                                    [{:job [(java.util.UUID/randomUUID)
;                                            "wzhao"
;                                            (java.util.Date. t)
;                                            (* 2 60 60 1000)
;                                            4.0
;                                            32000.0]}
;                                     t]))))
;                     (take 8 (repeatedly
;                               (fn []
;                                  (let [t (+ start-day (* 30 60 1000) 60)]
;                                    [{:job [(java.util.UUID/randomUUID)
;                                            "sunil"
;                                            (java.util.Date. t)
;                                            (* 60 60 1000)
;                                            4.0
;                                            32000.0]}
;                                     t]))))
;                     (take 8 (repeatedly
;                               (fn []
;                                 (let [t (+ start-day (* 40 60 1000) 60)]
;                                   [{:job [(java.util.UUID/randomUUID)
;                                           "dgrnbrg"
;                                           (java.util.Date. t)
;                                           (* 60 60 1000)
;                                            4.0
;                                            32000.0]}
;                                    t])))))
;             {:mem-quota 1024000.0
;              :cpus-quota Double/MAX_VALUE
;              :max-preemption 128
;              :safe-dru-threshold 0.5
;              :min-dru-diff 0.0})))
;
;(defn test-8
;  []
;  (let [datomic-uri "datomic:mem://test-8"
;        days-in-ms 86400000
;        start-day (.getTime #inst "2014-12-01")]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-day
;             (* 3 60 60 1000)
;             (map #(vector (str "machine-" %) 48.0 256000.0) (range 8))
;             (concat (take 256 (repeatedly
;                               (fn []
;                                 (let [t (+ start-day 60)]
;                                   [{:job [(java.util.UUID/randomUUID)
;                                           "ljin"
;                                           (java.util.Date. t)
;                                           (* 2 60 60 1000)
;                                           4.0
;                                           32000.0]}
;                                    t]))))
;                     (take 256 (repeatedly
;                                (fn []
;                                  (let [t (+ start-day (* 15 60 1000) 60)]
;                                    [{:job [(java.util.UUID/randomUUID)
;                                            "wzhao"
;                                            (java.util.Date. t)
;                                            (* 2 60 60 1000)
;                                            4.0
;                                            32000.0]}
;                                     t]))))
;                     (take 8 (repeatedly
;                               (fn []
;                                  (let [t (+ start-day (* 30 60 1000) 60)]
;                                    [{:job [(java.util.UUID/randomUUID)
;                                            "sunil"
;                                            (java.util.Date. t)
;                                            (* 60 60 1000)
;                                            4.0
;                                            32000.0]}
;                                     t]))))
;                     (take 8 (repeatedly
;                               (fn []
;                                 (let [t (+ start-day (* 40 60 1000) 60)]
;                                   [{:job [(java.util.UUID/randomUUID)
;                                           "dgrnbrg"
;                                           (java.util.Date. t)
;                                           (* 60 60 1000)
;                                            4.0
;                                            32000.0]}
;                                    t])))))
;             {:mem-quota 682000.0
;              :cpus-quota Double/MAX_VALUE
;              :max-preemption 128
;              :safe-dru-threshold 100.0
;              :min-dru-diff 100.0})))
;
;(defn test-9
;  "Runs a test on the recorded job data"
;  []
;  (let [datomic-uri "datomic:mem://test-9"
;        hours-duration 12
;        start-time (.getTime #inst "2014-12-18T12:00:00")
;        ms-duration (* 1000 60 60 hours-duration)]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-time
;             ms-duration
;             (map #(vector (str "machine-" %) 40.0 240000.0) (range 374)) ; similar to our cluster
;             (->> (read-string (slurp "job-data.edn"))
;                  (map (fn [{:keys [user time runtime cpus mem]}]
;                         (let [t (.getTime time)]
;                           [{:job [(java.util.UUID/randomUUID)
;                                   user
;                                   time
;                                   runtime
;                                   cpus
;                                   mem
;                                   ]}
;                            t]))))
;             {:mem-quota 8192000.0
;              :cpus-quota Double/MAX_VALUE
;              :max-preemption 128
;              :safe-dru-threshold 100.0
;              :min-dru-diff 100.0})))
;
;(defn test-10
;  "Runs a test on the recorded job data"
;  []
;  (let [datomic-uri "datomic:mem://test-10"
;        hours-duration 12
;        start-time (.getTime #inst "2014-12-18T12:00:00")
;        ms-duration (* 1000 60 60 hours-duration)]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-time
;             ms-duration
;             (map #(vector (str "machine-" %) 40.0 240000.0) (range 274)) ; similar to our cluster
;             (->> (read-string (slurp "job-data.edn"))
;                  (map (fn [{:keys [user time runtime cpus mem]}]
;                         (let [t (.getTime time)]
;                           [{:job [(java.util.UUID/randomUUID)
;                                   user
;                                   time
;                                   runtime
;                                   cpus
;                                   mem
;                                   ]}
;                            t]))))
;             {:mem-quota 12288000.0
;              :cpus-quota Double/MAX_VALUE
;              :max-preemption 128
;              :safe-dru-threshold 100.0
;              :min-dru-diff 100.0})))
;
;(defn test-11
;  "Runs a test on the recorded job data"
;  []
;  (let [datomic-uri "datomic:mem://test-11"
;        hours-duration 12
;        start-time (.getTime #inst "2014-12-18T12:00:00")
;        ms-duration (* 1000 60 60 hours-duration)]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-time
;             ms-duration
;             (map #(vector (str "machine-" %) 40.0 240000.0) (range 274)) ; similar to our cluster
;             (->> (read-string (slurp "job-data.edn"))
;                  (map (fn [{:keys [user time runtime cpus mem]}]
;                         (let [t (.getTime time)]
;                           [{:job [(java.util.UUID/randomUUID)
;                                   user
;                                   time
;                                   runtime
;                                   cpus
;                                   mem
;                                   ]}
;                            t]))))
;             {:max-preemption 128
;              :safe-dru-threshold 1.0
;              :min-dru-diff 0.7
;              :dynamic-quota {:resource.type/mem  {:min   (*  6.0 1024 1024)
;                                                   :max   (* 33.0 1024 1024)
;                                                   :total (* 66.0 1024 1024)
;                                                   :load  0.9}
;                              :resource.type/cpus {:min   2000.0
;                                                   :max   3000.0
;                                                   :total 9000.0
;                                                   :load  0.9}}})))
;
;(defn test-11-1
;  "Runs a test on the recorded job data"
;  []
;  (let [datomic-uri "datomic:mem://test-11-1"
;        hours-duration 12
;        start-time (.getTime #inst "2014-12-18T12:00:00")
;        ms-duration (* 1000 60 60 hours-duration)]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-time
;             ms-duration
;             (map #(vector (str "machine-" %) 40.0 240000.0) (range 274)) ; similar to our cluster
;             (->> (read-string (slurp "job-data-x1.2.edn"))
;                  (map (fn [{:keys [user time runtime cpus mem]}]
;                         (let [t (.getTime time)]
;                           [{:job [(java.util.UUID/randomUUID)
;                                   user
;                                   time
;                                   runtime
;                                   cpus
;                                   mem
;                                   ]}
;                            t]))))
;             {:max-preemption 128
;              :safe-dru-threshold 1.0
;              :min-dru-diff 0.7
;              :dynamic-quota {:resource.type/mem  {:min   (*  6.0 1024 1024)
;                                                   :max   (* 33.0 1024 1024)
;                                                   :total (* 66.0 1024 1024)
;                                                   :load  0.9}
;                              :resource.type/cpus {:min   2000.0
;                                                   :max   3000.0
;                                                   :total 9000.0
;                                                   :load  0.9}}})))
;
;(defn test-11-3
;  "Runs a test on the recorded job data"
;  []
;  (let [datomic-uri "datomic:mem://test-11-3"
;        hours-duration 12
;        start-time (.getTime #inst "2014-12-18T12:00:00")
;        ms-duration (* 1000 60 60 hours-duration)]
;    (d/delete-database datomic-uri)
;    (run-sim datomic-uri
;             :new-scheduler
;             start-time
;             ms-duration
;             (map #(vector (str "machine-" %) 40.0 240000.0) (range 274)) ; similar to our cluster
;             (->> (read-string (slurp "job-data-x2.edn"))
;                  (map (fn [{:keys [user time runtime cpus mem]}]
;                         (let [t (.getTime time)]
;                           [{:job [(java.util.UUID/randomUUID)
;                                   user
;                                   time
;                                   runtime
;                                   cpus
;                                   mem
;                                   ]}
;                            t]))))
;             {:max-preemption 128
;              :safe-dru-threshold 1.0
;              :min-dru-diff 0.7
;              :dynamic-quota {:resource.type/mem  {:min   (*  6.0 1024 1024)
;                                                   :max   (* 33.0 1024 1024)
;                                                   :total (* 66.0 1024 1024)
;                                                   :load  0.9}
;                              :resource.type/cpus {:min   2000.0
;                                                   :max   3000.0
;                                                   :total 9000.0
;                                                   :load  0.9}}})))
;
;(defn test-12
;  "Runs a test on the recorded job data"
;  []
;  (let [datomic-uri "datomic:mem://test-12"
;        hours-duration 12
;        start-time (.getTime #inst "2014-12-18T12:00:00")
;        ms-duration (* 1000 60 60 hours-duration)]
;    (println (d/delete-database datomic-uri))
;    (run-sim datomic-uri
;             :new-scheduler
;             start-time
;             ms-duration
;             (map #(vector (str "machine-" %) 40.0 240000.0) (range 274)) ; similar to our cluster
;             (->> (read-string (slurp "job-data.edn"))
;                  (map (fn [{:keys [user time runtime cpus mem]}]
;                         (let [t (.getTime time)]
;                           [{:job [(java.util.UUID/randomUUID)
;                                   user
;                                   time
;                                   runtime
;                                   cpus
;                                   mem
;                                   ]}
;                            t]))))
;             {:max-preemption 128
;              :safe-dru-threshold 100.0
;              :min-dru-diff 100.0
;              :dynamic-quota {:resource.type/mem  {:min   (*  6.0 1024 1024)
;                                                   :max   (* 33.0 1024 1024)
;                                                   :total (* 66.0 1024 1024)
;                                                   :load  0.9}
;                              :resource.type/cpus {:min   2000.0
;                                                   :max   3000.0
;                                                   :total 9000.0
;                                                   :load  0.9}}})))
;
;(defn test-12-2
;  "Runs a test on the recorded job data"
;  []
;  (let [datomic-uri "datomic:mem://test-12-2"
;        hours-duration 12
;        start-time (.getTime #inst "2014-12-18T12:00:00")
;        ms-duration (* 1000 60 60 hours-duration)]
;    (println (d/delete-database datomic-uri))
;    (run-sim datomic-uri
;             :new-scheduler
;             start-time
;             ms-duration
;             (map #(vector (str "machine-" %) 40.0 240000.0) (range 274)) ; similar to our cluster
;             (->> (read-string (slurp "job-data-x1.5.edn"))
;                  (map (fn [{:keys [user time runtime cpus mem]}]
;                         (let [t (.getTime time)]
;                           [{:job [(java.util.UUID/randomUUID)
;                                   user
;                                   time
;                                   runtime
;                                   cpus
;                                   mem
;                                   ]}
;                            t]))))
;             {:max-preemption 128
;              :safe-dru-threshold 100.0
;              :min-dru-diff 100.0
;              :dynamic-quota {:resource.type/mem  {:min   (*  6.0 1024 1024)
;                                                   :max   (* 33.0 1024 1024)
;                                                   :total (* 66.0 1024 1024)
;                                                   :load  0.9}
;                              :resource.type/cpus {:min   2000.0
;                                                   :max   3000.0
;                                                   :total 9000.0
;                                                   :load  0.9}}})))
;
;(defn test-12-3
;  "Runs a test on the recorded job data"
;  []
;  (let [datomic-uri "datomic:mem://test-12-3"
;        hours-duration 12
;        start-time (.getTime #inst "2014-12-18T12:00:00")
;        ms-duration (* 1000 60 60 hours-duration)]
;    (println (d/delete-database datomic-uri))
;    (run-sim datomic-uri
;             :new-scheduler
;             start-time
;             ms-duration
;             (map #(vector (str "machine-" %) 40.0 240000.0) (range 274)) ; similar to our cluster
;             (->> (read-string (slurp "job-data-x2.edn"))
;                  (map (fn [{:keys [user time runtime cpus mem]}]
;                         (let [t (.getTime time)]
;                           [{:job [(java.util.UUID/randomUUID)
;                                   user
;                                   time
;                                   runtime
;                                   cpus
;                                   mem
;                                   ]}
;                            t]))))
;             {:max-preemption 128
;              :safe-dru-threshold 100.0
;              :min-dru-diff 100.0
;              :dynamic-quota {:resource.type/mem  {:min   (*  6.0 1024 1024)
;                                                   :max   (* 33.0 1024 1024)
;                                                   :total (* 66.0 1024 1024)
;                                                   :load  0.9}
;                              :resource.type/cpus {:min   2000.0
;                                                   :max   3000.0
;                                                   :total 9000.0
;                                                   :load  0.9}}})))
;
;(comment (gen-report (mt/db (d/connect "datomic:mem://test"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-2"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-3"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-4"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-5"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-6"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-7"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-8"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-9"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-10"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-11"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-11-1"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-11-3"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-12"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-12-2"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-12-3"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-13"))))
;(comment (gen-report (mt/db (d/connect "datomic:mem://test-14"))))
;
;(comment (test))
;(comment (test-2))
;(comment (test-3))
;(comment (test-4))
;(comment (test-5))
;(comment (test-6))
;(comment (test-7))
;(comment (test-8))
;(comment (test-9))
;(comment (test-10))
;(comment (test-11))
;(comment (test-11-1))
;(comment (test-11-3))
;(comment (test-12))
;(comment (test-12-2))
;(comment (test-12-3))
;(comment (test-13))
;(comment (test-14))
;
;(defn create-j->data
;  [db start-time]
;  (fn [j]
;    (let [j-ent (d/entity db j)
;          t (max-key
;             #(.getTime %)
;             (ffirst (q '[:find ?time
;                          :in $ ?j
;                          :where
;                          [?j :job/uuid _ ?tx]
;                          [?tx :db/txInstant ?time]]
;                        db j))
;             start-time)
;          running-start-and-end
;          (q '[:find ?start-time ?e
;               :in $ ?j ?start-time
;               :where
;               [?j :job/instance ?i]
;               [?i :instance/start-time ?s]
;               [?i :instance/end-time ?e]
;               [(.before ?s ?start-time)]
;               [(.after ?e ?start-time)]]
;             db j start-time)
;          all-start-and-end
;          (-> running-start-and-end
;              (concat (q '[:find ?s ?e
;                           :in $ ?j ?start-time
;                           :where
;                           [?j :job/instance ?i]
;                           [?i :instance/start-time ?s]
;                           [?i :instance/end-time ?e]
;                           [(.after ?s ?start-time)]]
;                         db j start-time)))
;          running-time (->> all-start-and-end
;                            (map (fn [[start end]]
;                                   (- (.getTime end) (.getTime start))))
;                            (reduce +))
;          [cpu mem] (first (q '[:find ?cpu ?mem
;                                :in $ ?j
;                                :where
;                                [?j :job/resource ?r1]
;                                [?r1 :resource/type :resource.type/cpus]
;                                [?r1 :resource/amount ?cpu]
;                                [?j :job/resource ?r2]
;                                [?r2 :resource/type :resource.type/mem]
;                                [?r2 :resource/amount ?mem]]
;                              db j))]
;      {:user (:job/user j-ent)
;       :time t
;       :runtime running-time
;       :cpus cpu
;       :mem mem})))
;
;(defn create-j->data-v2
;  [db cook-db start-time]
;  (fn [j]
;    (let [j-ent (d/entity db j)
;          t (max-key
;             #(.getTime %)
;             (ffirst (q '[:find ?time
;                          :in $ ?j
;                          :where
;                          [?j :job/uuid _ ?tx]
;                          [?tx :db/txInstant ?time]]
;                        db j))
;             start-time)
;
;          job-uuid (:job/uuid j-ent)
;
;          simset-uuid (ffirst (q '[:find ?uuid
;                                   :in $ ?job-uuid
;                                   :where
;                                   [?j :job/uuid ?job-uuid]
;                                   [?s :sim-set/job ?j]
;                                   [?s :sim-set/uuid ?uuid]]
;                                 cook-db job-uuid))
;
;          instances (->> (q '[:find ?status ?start ?end
;                              :in $ ?j
;                              :where
;                              [?j :job/instance ?i]
;                              [?i :instance/status ?s]
;                              [?s :db/ident ?status]
;                              [?i :instance/start-time ?start]
;                              [?i :instance/end-time ?end]]
;                            db j)
;                         (filter (fn [[_ start end]]
;                                   (and start end))))
;
;          [cpu mem] (first (q '[:find ?cpu ?mem
;                                :in $ ?j
;                                :where
;                                [?j :job/resource ?r1]
;                                [?r1 :resource/type :resource.type/cpus]
;                                [?r1 :resource/amount ?cpu]
;                                [?j :job/resource ?r2]
;                                [?r2 :resource/type :resource.type/mem]
;                                [?r2 :resource/amount ?mem]]
;                              db j))
;          runtime (loop [[instance & rest] instances
;                         runtime 0.0]
;                    (if instance
;                      (let [[this-status start end] instance
;                            this-runtime (- (.getTime end) (.getTime start))]
;                        (if (= this-status :instance.status/success)
;                          this-runtime
;                          (recur rest (max runtime this-runtime))))
;                      runtime))]
;      {:uuid job-uuid
;       :user (:job/user j-ent)
;       :time t
;       :simset simset-uuid
;       :runtime runtime
;       :cpus cpu
;       :mem mem})))
;
;(defn j->data
;  "Used to import data from the pre-scheduler v2 deployment db"
;  [db j]
;  (let [j-ent (d/entity db j)
;        t (ffirst (q '[:find ?time
;                       :in $ ?j
;                       :where
;                       [?j :job/uuid _ ?tx]
;                       [?tx :db/txInstant ?time]]
;                     db j))
;        [s] (first (q '[:find ?start
;                        :in $ ?j
;                        :where
;                        [?j :job/instance ?i ?start-tx]
;                        [?start-tx :db/txInstant ?start]
;                        ]
;                      db j))
;        [e] (first (q '[:find ?end
;                        :in $ ?j
;                        :where
;                        [?j :job/instance ?i]
;                        [?i :instance/status :instance.status/success ?end-tx]
;                        [?end-tx :db/txInstant ?end]]
;                      db j))
;        [cpu mem] (first (q '[:find ?cpu ?mem
;                              :in $ ?j
;                              :where
;                              [?j :job/resource ?r1]
;                              [?r1 :resource/type :resource.type/cpus]
;                              [?r1 :resource/amount ?cpu]
;                              [?j :job/resource ?r2]
;                              [?r2 :resource/type :resource.type/mem]
;                              [?r2 :resource/amount ?mem]]
;                            db j))]
;    [(:job/user j-ent)
;     t
;     (- (.getTime e) (.getTime s))
;     cpu
;     mem]))
;
;(defn retrieve-jobs-from-db
;  [uri start-time end-time]
;  (let [conn (d/connect uri)
;        jobs-in-order (time (sort-by second (q '[:find ?j ?t
;                                                 :where
;                                                 [?j :job/state :job.state/completed]
;                                                 [?j :job/instance ?i]
;                                                 [?i :instance/status :instance.status/success]
;                                                 [?j :job/command _ ?tx]
;                                                 [?tx :db/txInstant ?t]
;                                                 [(.after ?t #inst "2014-12-15")]
;                                                 [(.before ?t #inst "2014-12-15T12:00:00")]]
;                                               (mt/db conn))))
;        _ (println "saw" (count jobs-in-order))
;        jobs-structured (time (doall (map (comp (partial j->data (mt/db conn))
;                                                first)
;                                          jobs-in-order)))
;        jobs-string (time (pr-str jobs-structured))]
;                                        ;(j->data (db conn) (ffirst jobs-in-order))
;    (spit "job-data.edn" jobs-string)))
;
;;; TODO:
;;; - come up with a replacement for task runtime that works on both the simulated jobs and the
;;; real jobs (i.e. computing based on instances)
;;; - come up with replacements for some of the other properties not in the DB, like submit-time etc
;;; (perhaps rules?)
;;; - run the analysis on some window into the existing jobs DB
;;; -- probably just exclude any jobs with failures to simplify
;
;;;
;;; We have job-data.edn, which represents jobs from 2014-10-08 midnight to 2014-10-10 18:30
;
;(comment
;  (map (fn [t]
;         (-> t :job/_instance :job/user)
;         ) (sched/potential-tasks (d/as-of (db (d/connect "datomic:mem://preemptive-sim-1"))
;                                           #inst "2014-10-09T08:10")))
;
;  (with-redefs [sched/now (fn [] #inst "2014-10-09T08:10")]
;    (def task-score-tmp
;      (time (sched/score-all-tasks (d/as-of (db (d/connect "datomic:mem://preemptive-sim-1"))
;                                            #inst "2014-10-09T08:10")
;                                   (* 80 40.0) (* 80 240000.0))))
;    (println "users" (distinct (map (fn [[_ {{j :job/_instance} :task}]]
;                                      (-> j :job/user))
;                                    task-score-tmp)))
;    (println "scores" (sort-by second (map (fn [[_ {{:as t j :job/_instance} :task
;                                                    s :score}]]
;                                             [(-> j :job/user) s])
;                                           task-score-tmp))))
;
;  (println task-score-tmp)
;
;  (defn ffff [] (retrieve-jobs-from-db "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http"))
;
;  (ffff))
;
;(comment
;  (let [driver (create-mesos-driver nil (atom nil) (atom (java.util.Date.)))]
;    (mesos/launch-tasks driver
;                        ["1"]
;                        [{:slave-id "usrs001.pit.twosigma.com"
;                          :name "cooksim_ljin_6b32bb63-cd9f-4e85-ab38-dc0302976476"
;                          :task-id "6b32bb63-cd9f-4e85-ab38-dc0302976476"
;                          :resources {:cpus 4.0, :mem 28.0}
;                          :executor {:executor-id "1201adff-af61-4458-89b5-7b9081536bce"
;                                     :framework-id "scheduler_simulator"
;                                     :name "sim agent executor"
;                                     :source "sim_scheduler"
;                                     :command {:value "test" :user "ljin"}}}])
;    (mesos/decline-offer driver "1234")))
;
;(comment (with-redefs-fn {#'sched/now (fn [] nil)}
;           sched/print-now)
;         (with-redefs [sched/now (fn [] nil)]
;           (sched/print-now))
;         )
;(comment
;  [(or
;    (and (.before ?submit-time ?export-start-time)
;         (.after ?end-time ?export-start-time))
;    (and (.after ?submit-time ?export-start-time)
;         (.before ?submit-time ?export-end-time)))])
;
;(defn extract-jobs-data
;  [uri ^java.util.Date start-time ^java.util.Date end-time output-file dup-factor]
;  (let [conn (d/connect uri)
;        db (mt/db conn)
;        init-db (d/as-of db start-time)]
;    (let [one-day-millis (* 24 3600 1000)
;          jobs-at-start (-<>> init-db
;                              (q
;                               '[:find ?j
;                                 :in $ [?s ...]
;                                 :where
;                                 [?j :job/state ?s]]
;                               <> [:job.state/waiting :job.state/running])
;                              (map first))
;          jobs (-<>> db
;                     (q '[:find ?j
;                          :in $ ?start-time ?end-time
;                          :where
;                          [?j :job/state :job.state/completed]
;                          [?j :job/uuid _ ?tx]
;                          [?tx :db/txInstant ?t]
;                          [(.after ?t ?start-time)]
;                          [(.before ?t ?end-time)]]
;                        <> start-time end-time)
;                     (map first))
;          j->data (create-j->data db start-time)
;          all-jobs (concat jobs-at-start jobs)
;          all-jobs-data (->> all-jobs
;                             (map j->data)
;                             (filter #(> (:runtime %) 0.0))
;                             (mapcat #(if (>= (rand) dup-factor) [%] [% %]))
;                             (sort-by :time))
;          _ (println "saw" (count all-jobs-data))
;          all-jobs-string (time (pr-str all-jobs-data))]
;      (spit output-file all-jobs-string))))
;
;(defn extract-jobs-data-v2
;  [mesos-uri cook-uri ^java.util.Date start-time ^java.util.Date end-time output-file dup-factor]
;  (let [db (mt/db (d/connect mesos-uri))
;        init-db (d/as-of db start-time)
;        cook-db (mt/db (d/connect cook-uri))]
;    (let [one-day-millis (* 24 3600 1000)
;          jobs-at-start (-<>> init-db
;                              (q
;                               '[:find ?j
;                                 :in $ [?s ...]
;                                 :where
;                                 [?j :job/state ?s]]
;                               <> [:job.state/waiting :job.state/running])
;                              (map first))
;          jobs (-<>> db
;                     (q '[:find ?j
;                          :in $ ?start-time ?end-time
;                          :where
;                          [?j :job/state :job.state/completed]
;                          [?j :job/uuid _ ?tx]
;                          [?tx :db/txInstant ?t]
;                          [(.after ?t ?start-time)]
;                          [(.before ?t ?end-time)]]
;                        <> start-time end-time)
;                     (map first))
;          j->data (create-j->data-v2 db cook-db start-time)
;          all-jobs (concat jobs-at-start jobs)
;          _ (println (count all-jobs))
;          count-atom (atom 0)
;          all-jobs-data (->> all-jobs
;                             (map j->data)
;                             (map (fn [x]
;                                    (swap! count-atom inc)
;                                    (if (= (rem @count-atom 100) 0)
;                                      (println @count-atom))
;                                    x))
;                             (filter #(> (:runtime %) 0.0))
;                             (mapcat #(if (>= (rand) dup-factor) [%] [% %]))
;                             (sort-by :time))
;          _ (println "saw" (count all-jobs-data))
;          all-jobs-string (time (pr-str all-jobs-data))]
;      (spit output-file all-jobs-string))))
;
;
;(comment
;  (time (extract-jobs-data-v2 "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http"
;                              "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/cook-cloud2?interface=http"
;                              #inst "2015-01-06T12:00:00"
;                              #inst "2015-01-07T12:00:00"
;                              "job-data-01061200-01071200-v2.edn"
;                              0.0)))
;
;(comment
;  (time (extract-jobs-data-v2 "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http"
;                              "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/cook-cloud2?interface=http"
;                              #inst "2015-01-08T12:00:00"
;                              #inst "2015-01-09T12:00:00"
;                              "job-data-01081200-01091200-v2.edn"
;                              0.0)))
;
;(comment
;  (time (extract-jobs-data-v2 "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http"
;                              "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/cook-cloud2?interface=http"
;                              #inst "2015-01-06T12:00:00"
;                              #inst "2015-01-07T12:00:00"
;                              "job-data-01061200-01071200-x1.2-v2.edn"
;                              0.2)))
;
;(comment
;  (time (extract-jobs-data-v2 "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http"
;                              "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/cook-cloud2?interface=http"
;                              #inst "2015-01-08T12:00:00"
;                              #inst "2015-01-09T12:00:00"
;                              "job-data-01081200-01091200-x1.25-v2.edn"
;                              0.25)))
;
;(comment
;  (time (extract-jobs-data-v2 "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http"
;                              "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/cook-cloud2?interface=http"
;                              #inst "2015-01-06T12:00:00"
;                              #inst "2015-01-07T12:00:00"
;                              "job-data-01061200-01071200-x1.5-v2.edn"
;                              0.5)))
;
;(comment
;  (time (extract-jobs-data-v2 "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http"
;                              "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/cook-cloud2?interface=http"
;                              #inst "2015-01-08T12:00:00"
;                              #inst "2015-01-09T12:00:00"
;                              "job-data-01081200-01091200-x1.5-v2.edn"
;                              0.5)))
;(comment
;  (time (extract-jobs-data-v2 "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http"
;                              "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/cook-cloud2?interface=http"
;                              #inst "2015-01-06T12:00:00"
;                              #inst "2015-01-07T12:00:00"
;                              "job-data-01061200-01071200-x2.0-v2.edn"
;                              1.0)))
;
;(comment
;  (time (extract-jobs-data-v2 "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http"
;                              "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/cook-cloud2?interface=http"
;                              #inst "2015-01-05T12:00:00"
;                              #inst "2015-01-12T12:00:00"
;                              "job-data-01051200-01121200-x1.75-v2.edn"
;                              0.75)))
;
;(comment
;  (time (extract-jobs-data-v2 "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http"
;                              "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/cook-cloud2?interface=http"
;                              #inst "2015-01-05T12:00:00"
;                              #inst "2015-01-12T12:00:00"
;                              "job-data-01051200-01121200-x1.5-v2.edn"
;                              0.5)))
;
;(comment
;  (time (extract-jobs-data-v2 "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/mesos-jobs2?interface=http"
;                              "datomic:riak://ramkv.pit.twosigma.com:8098/datomic3/cook-cloud2?interface=http"
;                              #inst "2015-01-05T12:00:00"
;                              #inst "2015-01-12T12:00:00"
;                              "job-data-01051200-01121200-x1.25-v2.edn"
;                              0.25)))
;
;(def settings
;  {:datomic-url (fnk [] nil)
;   :scheduler (fnk [] nil)
;   :start-time (fnk [] nil)
;   :duration-ms (fnk [] nil)
;   :hosts (fnk [] nil)
;   :jobs (fnk [] nil)
;   :params (fnk [] nil)})
;
;(defn app
;  [settings]
;  {:settings settings
;   :sim (fnk [[:settings
;               datomic-url scheduler start-time duration-ms hosts jobs params]]
;             (run-sim datomic-url
;                      scheduler
;                      start-time
;                      duration-ms
;                      hosts
;                      jobs
;                      params))})
;
;(defn -main
;  [config]
;  (if (and config
;           (.exists (java.io.File. config)))
;    (do (println "Reading config file:" config)
;        (load-file config)
;        ((graph/eager-compile (app settings)) {}))))
;
;;; Here is how to run a real sim
;(comment
;  (let [datomic-url "datomic:dev://example.twosigma.com:4334/sim-01051200-01121200-1.5-100-100-v2"
;        scheduler :new-scheduler
;        start-time (.getTime #inst "2015-01-05T12:00:00")
;        duration-ms (* 1000 60 60 24 7)
;        hosts (map #(vector (str "machine-" %) 40.0 240000.0) (range 274))
;        jobs (->> (read-string (slurp "job-data-01051200-01121200-x1.5-v2.edn"))
;                  (map (fn [{:keys [user time simset runtime cpus mem]}]
;                         (let [t (.getTime time)]
;                           [{:job {:uuid (java.util.UUID/randomUUID)
;                                   :user user
;                                   :time time
;                                   :simset-uuid simset
;                                   :runtime runtime
;                                   :cpus cpus
;                                   :mem mem}}
;                            t]))))
;        params {:max-preemption 128
;                :safe-dru-threshold 100.0
;                :min-dru-diff 100.0
;                :dynamic-quota {:resource.type/mem  {:min   (*  6.0 1024 1024)
;                                                     :max   (* 33.0 1024 1024)
;                                                     :total (* 66.0 1024 1024)
;                                                     :load  0.9}
;                                :resource.type/cpus {:min   2000.0
;                                                     :max   3000.0
;                                                     :total 9000.0
;                                                     :load  0.9}}}]
;    (run-sim datomic-url
;             scheduler
;             start-time
;             duration-ms
;             hosts
;             jobs
;             params)))
;
;(comment (gen-report (mt/db (d/connect "datomic:dev://example.twosigma.com:4334/sim-01081200-01091200-1.25-100-100-v2"))))
;
;(comment
;  (def test-report-v2
;
;    (gen-report-v2 (mt/db (d/connect "datomic:mem://test-5"))))
;
;  (def baseline-v2
;    (gen-report-v2 (mt/db (d/connect "datomic:dev://example.twosigma.com:4334/sim-01081200-01091200-1.5-100-100-v2"))))
;
;  (def preemption-1-v2
;    (gen-report-v2 (mt/db (d/connect "datomic:dev://example.twosigma.com:4334/sim-01081200-01091200-1.5-1.0-0.5-v2"))))
;
;  (def preemption-2-v2
;    (gen-report-v2 (mt/db (d/connect "datomic:dev://example.twosigma.com:4334/sim-01081200-01091200-1.5-0.5-0.25-v2"))))
;
;  (do
;    (def baseline-b-v2
;      (time (gen-report-v2 (mt/db (d/connect "datomic:dev://example.twosigma.com:4334/sim-01051200-01121200-1.5-100-100-v2")))))
;    (def preemption-b-1-v2
;      (time (gen-report-v2 (mt/db (d/connect "datomic:dev://example.twosigma.com:4334/sim-01051200-01121200-1.5-1.0-0.5-v2")))))
;    (def preemption-b-2-v2
;      (time (gen-report-v2 (mt/db (d/connect "datomic:dev://example.twosigma.com:4334/sim-01051200-01121200-1.5-0.5-0.25-v2")))))
;    (def preemption-b-3-v2
;      (time (gen-report-v2 (mt/db (d/connect "datomic:dev://example.twosigma.com:4334/sim-01051200-01121200-1.5-noquota-v2")))))
;    (def preemption-b-4-v2
;      (time (gen-report-v2 (mt/db (d/connect "datomic:dev://example.twosigma.com:4334/sim-01051200-01121200-1.5-noquota-2-v2")))))
;    )
;
;  (println (count (:uuid->sim-set-infos baseline-b-v2)))
;  (println (count (:uuid->sim-set-infos preemption-b-1-v2)))
;  (println (count (:uuid->sim-set-infos preemption-b-2-v2)))
;
;  (let [quantile-pcts (range 0 1.00 0.01)
;        info->complete-time
;        (fn [info]
;          (-
;           (->> info
;                (:end-time)
;                (.getTime))
;           (->> info
;                (:submit-time)
;                (.getTime))))
;
;        submitted-during-business-hours?
;        (fn [info]
;          (let [submit-time (:submit-time info)
;                submit-day (.getDay submit-time)
;                submit-hour (.getHours submit-time)]
;            (and (not (get #{0 6} submit-day))
;                 (>= submit-hour 10)
;                 (< submit-hour 23))))
;
;        h4 (->> baseline-b-v2
;                (:uuid->sim-set-infos)
;                (filter #(:completed? (val %)))
;                (map (fn [[uuid info]]
;                       [uuid (info->complete-time info)]))
;                (into {}))
;
;        h5 (->> preemption-b-1-v2
;                (:uuid->sim-set-infos)
;                (filter #(:completed? (val %)))
;                (map (fn [[uuid info]]
;                       [uuid (info->complete-time info)]))
;                (into {}))
;
;        h6 (->> preemption-b-2-v2
;                (:uuid->sim-set-infos)
;                (filter #(:completed? (val %)))
;                (map (fn [[uuid info]]
;                       [uuid (info->complete-time info)]))
;                (into {}))
;
;        h7 (->> preemption-b-3-v2
;                (:uuid->sim-set-infos)
;                (filter #(:completed? (val %)))
;                (map (fn [[uuid info]]
;                       [uuid (info->complete-time info)]))
;                (into {}))
;
;        h8 (->> preemption-b-4-v2
;                (:uuid->sim-set-infos)
;                (filter #(:completed? (val %)))
;                (map (fn [[uuid info]]
;                       [uuid (info->complete-time info)]))
;                (into {}))
;
;        b4 (->> baseline-b-v2
;                (:uuid->sim-set-infos)
;                (filter #(:completed? (val %)))
;                (filter #(submitted-during-business-hours? (val %)))
;                (map (fn [[uuid info]]
;                       [uuid (info->complete-time info)]))
;                (into {}))
;
;        b5 (->> preemption-b-1-v2
;                (:uuid->sim-set-infos)
;                (filter #(:completed? (val %)))
;                (filter #(submitted-during-business-hours? (val %)))
;                (map (fn [[uuid info]]
;                       [uuid (info->complete-time info)]))
;                (into {}))
;
;        b6 (->> preemption-b-2-v2
;                (:uuid->sim-set-infos)
;                (filter #(:completed? (val %)))
;                (filter #(submitted-during-business-hours? (val %)))
;                (map (fn [[uuid info]]
;                       [uuid (info->complete-time info)]))
;                (into {}))
;
;        b7 (->> preemption-b-3-v2
;                (:uuid->sim-set-infos)
;                (filter #(:completed? (val %)))
;                (filter #(submitted-during-business-hours? (val %)))
;                (map (fn [[uuid info]]
;                       [uuid (info->complete-time info)]))
;                (into {}))
;
;        b8 (->> preemption-b-4-v2
;                (:uuid->sim-set-infos)
;                (filter #(:completed? (val %)))
;                (filter #(submitted-during-business-hours? (val %)))
;                (map (fn [[uuid info]]
;                       [uuid (info->complete-time info)]))
;                (into {}))
;
;        m4 (->> baseline-b-v2
;                (:utilizations)
;                (map (fn [[time {:keys [cpus mem]}]]
;                       [time mem])))
;
;        m5 (->> preemption-b-1-v2
;                (:utilizations)
;                (map (fn [[time {:keys [cpus mem]}]]
;                       [time mem])))
;
;        m6 (->> preemption-b-2-v2
;                (:utilizations)
;                (map (fn [[time {:keys [cpus mem]}]]
;                       [time mem])))
;
;        m7 (->> preemption-b-3-v2
;                (:utilizations)
;                (map (fn [[time {:keys [cpus mem]}]]
;                       [time mem])))
;
;        m8 (->> preemption-b-4-v2
;                (:utilizations)
;                (map (fn [[time {:keys [cpus mem]}]]
;                       [time mem])))
;
;        r4 (-<>>
;            (merge-with (fn [x y] (double (/ x y))) h4 h4)
;            (filter #(< (val %) 1000.0))
;            (vals)
;            (sort)
;            (apply quantiles <> identity quantile-pcts))
;
;        r5 (-<>>
;            (merge-with (fn [x y] (double (/ x y))) h4 h5)
;            (filter #(< (val %) 1000.0))
;            (vals)
;            (sort)
;            (apply quantiles <> identity quantile-pcts))
;
;        r6 (-<>>
;            (merge-with (fn [x y] (double (/ x y))) h4 h6)
;            (filter #(< (val %) 1000.0))
;            (vals)
;            (sort)
;            (apply quantiles <> identity quantile-pcts))
;
;        r7 (-<>>
;            (merge-with (fn [x y] (double (/ x y))) h4 h7)
;            (filter #(< (val %) 1000.0))
;            (vals)
;            (sort)
;            (apply quantiles <> identity quantile-pcts))
;
;        r8 (-<>>
;            (merge-with (fn [x y] (double (/ x y))) h4 h8)
;            (filter #(< (val %) 1000.0))
;            (vals)
;            (sort)
;            (apply quantiles <> identity quantile-pcts))
;
;        rb4 (-<>>
;             (merge-with (fn [x y] (double (/ x y))) b4 b4)
;             (filter #(< (val %) 1000.0))
;             (vals)
;             (sort)
;             (apply quantiles <> identity quantile-pcts))
;
;        rb5 (-<>>
;             (merge-with (fn [x y] (double (/ x y))) b4 b5)
;             (filter #(< (val %) 1000.0))
;             (vals)
;             (sort)
;             (apply quantiles <> identity quantile-pcts))
;
;        rb6 (-<>>
;             (merge-with (fn [x y] (double (/ x y))) b4 b6)
;             (filter #(< (val %) 1000.0))
;             (vals)
;             (sort)
;             (apply quantiles <> identity quantile-pcts))
;
;        rb7 (-<>>
;             (merge-with (fn [x y] (double (/ x y))) b4 b7)
;             (filter #(< (val %) 1000.0))
;             (vals)
;             (sort)
;             (apply quantiles <> identity quantile-pcts))
;
;        rb8 (-<>>
;             (merge-with (fn [x y] (double (/ x y))) b4 b8)
;             (filter #(< (val %) 1000.0))
;             (vals)
;             (sort)
;             (apply quantiles <> identity quantile-pcts))
;        ]
;    #_(let [total-mem-hours (->> baseline-origin-v2
;                               (:job-infos)
;                               (map (fn info->mem-hour
;                                      [info]
;                                      (/ (* (:runtime info) (:mem info)) 3600000.0)))
;                               (reduce +))
;          total-completed-mem-hours (->> baseline-origin-v2
;                                         (:job-infos)
;                                         (filter :completed?)
;                                         (map (fn info->mem-hour
;                                                [info]
;                                                (/ (* (:runtime info) (:mem info)) 3600000.0)))
;                                         (reduce +))
;          ]
;      (println total-mem-hours)
;      (println total-completed-mem-hours))
;
;    (save (doto
;              (time-series-plot (map first m4)
;                                (map second m4)
;                                :series-label "Baseline"
;                                :legend true
;                                :title "Memory Utilization"
;                                :x-label "Time"
;                                :y-label "Memory Utilization")
;            (add-lines (map first m5)
;                       (map second m5)
;                       :series-label "Preemption-1")
;            (add-lines (map first m6)
;                       (map second m6)
;                       :series-label "Preemption-2")
;            (add-lines (map first m7)
;                       (map second m7)
;                       :series-label "Preemption-noquota")
;            (add-lines (map first m8)
;                       (map second m8)
;                       :series-label "Preemption-noquota-2")
;            )
;          "/u/ljin/tmp/memory-util.png"
;          )
;
;    (save (doto
;              (xy-plot (map first r4)
;                       (map second r4)
;                       :series-label "Baseline"
;                       :legend true
;                       :title "Speed up (All)"
;                       :x-label "Percentage"
;                       :y-label "Speed up")
;            (add-lines (map first r5)
;                       (map second r5)
;                       :series-label "Preemption-1")
;            (add-lines (map first r6)
;                       (map second r6)
;                       :series-label "Preemption-2")
;            (add-lines (map first r7)
;                       (map second r7)
;                       :series-label "Preemption-noquota")
;            (add-lines (map first r8)
;                       (map second r8)
;                       :series-label "Preemption-noquota-2"))
;          "/u/ljin/tmp/speed-up-all.png"
;          )
;
;    (save (doto
;              (xy-plot (map first rb4)
;                       (map second rb4)
;                       :series-label "Baseline"
;                       :legend true
;                       :title "Speed up (5am to 6pm)"
;                       :x-label "Percentage"
;                       :y-label "Speed up")
;            (add-lines (map first rb5)
;                       (map second rb5)
;                       :series-label "Preemption-1")
;            (add-lines (map first rb6)
;                       (map second rb6)
;                       :series-label "Preemption-2")
;            (add-lines (map first rb7)
;                       (map second rb7)
;                       :series-label "Preemption-noquota")
;            (add-lines (map first rb8)
;                       (map second rb8)
;                       :series-label "Preemption-noquota-2")
;            )
;          "/u/ljin/tmp/speed-up-business-hour.png"
;          )
;
;    (println (-<>> baseline-b-v2
;                  (:utilizations)
;                  (map (fn [[t {:keys [mem]}]]
;                         mem))
;                  (reduce +)
;                  (/ <>(count (:utilizations baseline-b-v2)))
;                  ))
;
;    (println (-<>> preemption-b-1-v2
;                   (:utilizations)
;                   (map (fn [[t {:keys [mem]}]]
;                          mem))
;                   (reduce +)
;                   (/ <>(count (:utilizations baseline-b-v2)))
;                   ))
;
;    (println (-<>> preemption-b-2-v2
;                   (:utilizations)
;                   (map (fn [[t {:keys [mem]}]]
;                          mem))
;                   (reduce +)
;                   (/ <>(count (:utilizations baseline-b-v2)))
;                   ))
;
;    (println (-<>> preemption-b-3-v2
;                   (:utilizations)
;                   (map (fn [[t {:keys [mem]}]]
;                          mem))
;                   (reduce +)
;                   (/ <>(count (:utilizations baseline-b-v2)))
;                   ))
;
;    (clojure.pprint/pprint (count h4))
;    (clojure.pprint/pprint (count h5))
;    (clojure.pprint/pprint (count h6))
;    (clojure.pprint/pprint (count h7))
;    (clojure.pprint/pprint (count h8))))
;
;(defn time-range
;  "Return a lazy sequence of DateTime's from start to end, incremented
;  by 'step' units of time."
;  [start end step]
;  (let [inf-range (time-period/periodic-seq start step)
;        below-end? (fn [t] (time/within? (time/interval start end)
;                                         t))]
;    (take-while below-end? inf-range)))
;
;(comment
;  (def utilizations
;    (let [conn (d/connect "datomic:riak://ramkv.pit.twosigma.com:8098/datomic7/mesos-jobs?interface=http")
;          joda-start (time/date-time 2015 5 1)
;          joda-end (time/date-time 2015 5 8)
;          joda-interval (time/minutes 15)
;          start #inst "2015-05-01"
;          end #inst "2015-05-08"
;          normalize-mem (partial / (* 78960.0 1024))
;          normalize-cpu (partial / 12576.0)
;          db (->
;              (d/db conn)
;              (d/since start)
;              (d/as-of end))
;
;          effective-instance-eids (->> db
;                                       (q '[:find ?i
;                                            :where
;                                            [?i :instance/preempted? false]])
;                                       (map first))]
;      (for [t (time-range joda-start
;                          joda-end
;                          joda-interval)
;            :let [db (d/as-of db (java.util.Date. (coerce/to-long t)))
;                  utilization
;                  (->> (first (q '[:find (sum ?cpus) (sum ?mem)
;                                   :with ?i
;                                   :where
;                                   [?i :instance/status :instance.status/running]
;                                   [?j :job/instance ?i]
;                                   [?j :job/resource ?r1]
;                                   [?r1 :resource/type :resource.type/cpus]
;                                   [?r1 :resource/amount ?cpus]
;                                   [?j :job/resource ?r2]
;                                   [?r2 :resource/type :resource.type/mem]
;                                   [?r2 :resource/amount ?mem]]
;                                 db)))
;                  effective-utilization
;                  (first (q '[:find (sum ?cpus) (sum ?mem)
;                              :in $ [?i ...]
;                              :with ?i
;                              :where
;                              [?i :instance/status :instance.status/running]
;                              [?j :job/instance ?i]
;                              [?j :job/resource ?r1]
;                              [?r1 :resource/type :resource.type/cpus]
;                              [?r1 :resource/amount ?cpus]
;                              [?j :job/resource ?r2]
;                              [?r2 :resource/type :resource.type/mem]
;                              [?r2 :resource/amount ?mem]]
;                            db effective-instance-eids))]]
;        [t utilization effective-utilization])))
;  (clojure.pprint/pprint utilizations)
;  )
