(ns cook.test.hooks
  (:require [clj-time.core :as t]
            [cook.hooks :as hooks]))


    '(with-redefs hooks/hook-object reject-all-hook)

'(def fake-scheduler-hooks
  (reify hooks/SchedulerHooks
    (check-job-submission-default [this] {:status :rejected :message "Too slow"})
    (check-job-submission [this {:keys [name] :as job-map}]
      (if (= name "reject-submission")
        {:status :rejected :message "Explicit-reject"}
        (default-accept)))
    (check-job-invocation [this {:keys [name] :as job-map}]
      (cond
        ; job-a is accepted for 5 seconds, rejected for 5 seconds, valid for 5 seconds.
        (and (= name "job-a") (t/before? (-> 5 t/seconds t/from-now)))
            {:status :accepted :cache-expires-at (-> 5 t/seconds t/from-now)}
            (and (= name "job-a") (t/before? (-> 10 t/seconds t/from-now)))
            {:status :accepted :cache-expires-at (-> 5 t/seconds t/from-now)}
                 (default-accept)))))
